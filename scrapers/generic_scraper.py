"""Generic scraper for sites with similar structure"""
from scrapers.base_scraper import BaseScraper
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import time


class GenericScraper(BaseScraper):
    """Generic scraper that can work with most automotive parts sites"""
    
    def __init__(self, site_config):
        """
        Initialize generic scraper with site configuration
        
        Args:
            site_config: Dictionary with site configuration
        """
        self.config = site_config
        site_name = site_config.get('name', 'generic')
        use_selenium = site_config.get('use_selenium', True)
        
        super().__init__(site_name, use_selenium=use_selenium)
        
        self.base_url = site_config.get('base_url', '')
        self.search_strategy = site_config.get('search_strategy', 'search')
        self.search_term = site_config.get('search_term', 'wheel')
        
    def get_product_urls(self):
        """Get all wheel product URLs"""
        product_urls = []
        
        try:
            if self.search_strategy == 'search':
                product_urls = self._search_for_products()
            elif self.search_strategy == 'category':
                category_url = self.config.get('category_url', '')
                if category_url:
                    product_urls = self._get_category_products(category_url)
            
            self.logger.info(f"Found {len(product_urls)} product URLs for {self.site_name}")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
        
        return product_urls
    
    def _search_for_products(self):
        """Search for wheel products"""
        product_urls = []
        
        try:
            # Common search URL patterns
            search_patterns = [
                f"{self.base_url}/search?search_str={self.search_term}",
                f"{self.base_url}/search?q={self.search_term}",
                f"{self.base_url}/s?k={self.search_term}",
            ]
            
            for search_url in search_patterns:
                self.logger.info(f"Trying search: {search_url}")
                
                html = self.get_page(search_url, use_selenium=True, wait_time=1)  # Optimized: reduced from 3
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Look for product links (common patterns)
                link_patterns = [
                    r'/oem-parts/',
                    r'/parts/',
                    r'/product/',
                    r'/p/',
                    r'/products/',
                ]
                
                for pattern in link_patterns:
                    links = soup.find_all('a', href=re.compile(pattern))
                    
                    for link in links:
                        href = link.get('href', '')
                        if href and 'wheel' in href.lower():
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Avoid duplicates
                            if full_url not in product_urls:
                                product_urls.append(full_url)
                
                # If we found products, break
                if product_urls:
                    self.logger.info(f"Found {len(product_urls)} products with search pattern")
                    break
            
        except Exception as e:
            self.logger.error(f"Error searching for products: {str(e)}")
        
        return product_urls
    
    def _get_category_products(self, category_path):
        """Get products from category page"""
        product_urls = []
        
        try:
            category_url = f"{self.base_url}{category_path}"
            self.logger.info(f"Fetching category: {category_url}")
            
            html = self.get_page(category_url, use_selenium=True, wait_time=1)  # Optimized: reduced from 3
            if not html:
                return product_urls
            
            soup = BeautifulSoup(html, 'lxml')
            
            # Find product links
            links = soup.find_all('a', href=re.compile(r'/(oem-parts|parts|product)/'))
            
            for link in links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    if full_url not in product_urls:
                        product_urls.append(full_url)
            
        except Exception as e:
            self.logger.error(f"Error getting category products: {str(e)}")
        
        return product_urls
    
    def scrape_product(self, url):
        """
        Scrape single product - works with common HTML structures
        
        Args:
            url: Product URL
        
        Returns:
            dict: Product data or None
        """
        html = self.get_page(url, use_selenium=True, wait_time=1)  # Optimized: reduced from 2
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Initialize product data
        product_data = {
            'url': url,
            'image_url': '',
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sku': '',
            'pn': '',
            'actual_price': '',
            'msrp': '',
            'title': '',
            'also_known_as': '',
            'positions': '',
            'description': '',
            'applications': '',
            'replaces': '',
            'fitments': []
        }
        
        try:
            # Extract title - try multiple selectors
            title_selectors = [
                ('h1', {'class_': re.compile(r'product.*title', re.I)}),
                ('h1', {'class_': 'title'}),
                ('h1', {}),
                ('div', {'class_': re.compile(r'product.*name', re.I)}),
            ]
            
            for tag, attrs in title_selectors:
                title_elem = soup.find(tag, attrs)
                if title_elem:
                    product_data['title'] = title_elem.get_text(strip=True)
                    break
            
            # Check if this is a wheel product
            if not product_data['title'] or not self.is_wheel_product(product_data['title']):
                self.logger.info(f"Skipping non-wheel product: {url}")
                return None
            
            # Extract SKU/Part Number - try multiple selectors
            sku_selectors = [
                ('span', {'class_': re.compile(r'sku', re.I)}),
                ('div', {'class_': re.compile(r'part.*number', re.I)}),
                ('span', {'itemprop': 'sku'}),
            ]
            
            for tag, attrs in sku_selectors:
                sku_elem = soup.find(tag, attrs)
                if sku_elem:
                    product_data['sku'] = sku_elem.get_text(strip=True)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                    break
            
            # Extract sale price
            price_selectors = [
                ('strong', {'class_': re.compile(r'sale.*price', re.I)}),
                ('span', {'class_': re.compile(r'price.*sale', re.I)}),
                ('div', {'class_': re.compile(r'price', re.I)}),
            ]
            
            for tag, attrs in price_selectors:
                price_elem = soup.find(tag, attrs)
                if price_elem:
                    product_data['actual_price'] = self.extract_price(price_elem.get_text(strip=True))
                    break
            
            # Extract MSRP
            msrp_selectors = [
                ('span', {'class_': re.compile(r'list.*price|msrp', re.I)}),
                ('div', {'class_': re.compile(r'retail.*price', re.I)}),
            ]
            
            for tag, attrs in msrp_selectors:
                msrp_elem = soup.find(tag, attrs)
                if msrp_elem:
                    product_data['msrp'] = self.extract_price(msrp_elem.get_text(strip=True))
                    break
            
            # Extract image
            img_selectors = [
                ('img', {'class_': re.compile(r'product.*image|main.*image', re.I)}),
                ('img', {'itemprop': 'image'}),
            ]
            
            for tag, attrs in img_selectors:
                img_elem = soup.find(tag, attrs)
                if img_elem and img_elem.get('src'):
                    img_url = img_elem['src']
                    product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
                    break
            
            # Extract description
            desc_selectors = [
                ('div', {'class_': re.compile(r'description', re.I)}),
                ('span', {'class_': re.compile(r'description', re.I)}),
                ('p', {'class_': re.compile(r'description', re.I)}),
            ]
            
            for tag, attrs in desc_selectors:
                desc_elem = soup.find(tag, attrs)
                if desc_elem:
                    product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
                    break
            
            # Try to extract fitment data
            self._extract_fitment(soup, product_data)
            
            # If no fitments found, add empty fitment
            if not product_data['fitments']:
                product_data['fitments'].append({
                    'year': '',
                    'make': '',
                    'model': '',
                    'trim': '',
                    'engine': ''
                })
            
            self.logger.info(f"Successfully scraped: {product_data['title']}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"Error scraping product {url}: {str(e)}")
            return None
    
    def _extract_fitment(self, soup, product_data):
        """Extract fitment data from various sources"""
        # Try to find JSON data with fitment
        script_tags = soup.find_all('script', type='application/json')
        
        for script in script_tags:
            try:
                if script.string:
                    data = json.loads(script.string)
                    fitments = data.get('fitment', [])
                    
                    if fitments:
                        for fitment in fitments:
                            product_data['fitments'].append({
                                'year': str(fitment.get('year', '')),
                                'make': fitment.get('make', ''),
                                'model': fitment.get('model', ''),
                                'trim': ', '.join(fitment.get('trims', [])) if isinstance(fitment.get('trims'), list) else fitment.get('trim', ''),
                                'engine': ', '.join(fitment.get('engines', [])) if isinstance(fitment.get('engines'), list) else fitment.get('engine', '')
                            })
                        break
            except:
                continue
        
        # Try to find fitment table
        if not product_data['fitments']:
            fitment_table = soup.find('table', class_=re.compile(r'fitment', re.I))
            if fitment_table:
                rows = fitment_table.find_all('tr')[1:]  # Skip header
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        product_data['fitments'].append({
                            'year': cols[0].get_text(strip=True) if len(cols) > 0 else '',
                            'make': cols[1].get_text(strip=True) if len(cols) > 1 else '',
                            'model': cols[2].get_text(strip=True) if len(cols) > 2 else '',
                            'trim': cols[3].get_text(strip=True) if len(cols) > 3 else '',
                            'engine': cols[4].get_text(strip=True) if len(cols) > 4 else ''
                        })

