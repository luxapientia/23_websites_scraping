"""Scraper for parts.nissanusa.com (Nissan parts)"""
from scrapers.base_scraper import BaseScraper
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import time
import traceback
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class NissanScraper(BaseScraper):
    """Scraper for parts.nissanusa.com"""
    
    def __init__(self):
        super().__init__('nissan', use_selenium=True)
        self.base_url = 'https://parts.nissanusa.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from parts.nissanusa.com"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            product_urls = list(set(product_urls))
            self.logger.info(f"Found {len(product_urls)} unique wheel product URLs")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """Search for wheels using site search with pagination"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Initial search URL - RevolutionParts uses search_str parameter
            search_url = f"{self.base_url}/search?search_str=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(search_url, use_selenium=True, wait_time=2)
                if not html:
                    return product_urls
            except Exception as e:
                self.logger.error(f"Error loading search page: {str(e)}")
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links to appear
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/'], div.catalog-product"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products on the first page
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract products from first page
            page_count = self._extract_products_from_page(soup, product_urls)
            self.logger.info(f"Page 1: Found {page_count} new unique URLs (Total: {len(product_urls)})")
            
            # Handle pagination - iterate through all pages
            page_num = 2
            max_pages = 2000  # Safety limit
            consecutive_empty_pages = 0
            max_consecutive_empty = 3  # Stop after 3 consecutive pages with no new products
            
            while page_num <= max_pages:
                try:
                    pag_url = f"{self.base_url}/search?search_str=wheel&page={page_num}"
                    self.logger.info(f"Loading page {page_num}: {pag_url}")
                    
                    try:
                        self.page_load_timeout = 60
                        self.driver.set_page_load_timeout(60)
                        pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                        if not pag_html or len(pag_html) < 5000:
                            self.logger.warning(f"Page {page_num} content too short, stopping pagination")
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= max_consecutive_empty:
                                break
                            page_num += 1
                            continue
                        
                        # Wait for products to load
                        try:
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/'], div.catalog-product"))
                            )
                        except:
                            pass
                        
                        # Scroll to load all products on this page
                        self._scroll_to_load_content()
                        pag_html = self.driver.page_source
                        soup = BeautifulSoup(pag_html, 'lxml')
                    except Exception as e:
                        self.logger.warning(f"Error loading page {page_num}: {str(e)}")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            break
                        page_num += 1
                        continue
                    finally:
                        try:
                            self.page_load_timeout = original_timeout
                            self.driver.set_page_load_timeout(original_timeout)
                        except:
                            pass
                    
                    # Extract products from current page
                    page_count = self._extract_products_from_page(soup, product_urls)
                    self.logger.info(f"Page {page_num}: Found {page_count} new unique URLs (Total: {len(product_urls)})")
                    
                    # Check if we found any new products
                    if page_count == 0:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"No new products found on {max_consecutive_empty} consecutive pages, stopping pagination")
                            break
                    else:
                        consecutive_empty_pages = 0  # Reset counter if we found products
                    
                    page_num += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        break
                    page_num += 1
                    continue
            
            self.logger.info(f"Finished processing all pages. Total unique product URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _extract_products_from_page(self, soup, product_urls):
        """Extract product URLs from a search results page"""
        page_count = 0
        
        # Strategy 1: RevolutionParts structure - div.catalog-product
        product_rows = soup.find_all('div', class_='catalog-product')
        for row in product_rows:
            # Try multiple selectors for product links
            link = row.find('a', class_='title-link')
            if not link:
                link = row.find('a', class_='product-image-link')
            if not link:
                link = row.find('a', href=re.compile(r'/oem-parts/|/product/|/parts/', re.I))
            
            if link:
                href = link.get('href', '')
                if href:
                    full_url = self._normalize_product_url(href)
                    if full_url and full_url not in product_urls:
                        product_urls.append(full_url)
                        page_count += 1
        
        # Strategy 2: Look for product containers/rows (common in e-commerce sites)
        if page_count == 0:
            product_containers = (
                soup.find_all('div', class_=re.compile(r'product|item|result', re.I)) +
                soup.find_all('article', class_=re.compile(r'product|item|result', re.I)) +
                soup.find_all('li', class_=re.compile(r'product|item|result', re.I))
            )
            
            for container in product_containers:
                # Try multiple selectors within container
                link = container.find('a', href=re.compile(r'/product/|/parts/|/oem-parts/|/p/', re.I))
                if not link:
                    # Try finding link by class
                    link = container.find('a', class_=re.compile(r'product|title|name|link', re.I))
                if not link:
                    # Try finding any link with product URL pattern
                    link = container.find('a', href=re.compile(r'/product/|/parts/|/oem-parts/|/p/', re.I))
                
                if link:
                    href = link.get('href', '')
                    if href:
                        full_url = self._normalize_product_url(href)
                        if full_url and full_url not in product_urls:
                            product_urls.append(full_url)
                            page_count += 1
        
        # Strategy 3: Fallback - Direct search for all product links
        if page_count == 0:
            self.logger.info("No products found in containers, trying direct link search...")
            product_links = (
                soup.find_all('a', href=re.compile(r'/product/', re.I)) +
                soup.find_all('a', href=re.compile(r'/parts/', re.I)) +
                soup.find_all('a', href=re.compile(r'/oem-parts/', re.I)) +
                soup.find_all('a', href=re.compile(r'/p/', re.I))
            )
            
            for link in product_links:
                href = link.get('href', '')
                if href:
                    full_url = self._normalize_product_url(href)
                    if full_url and full_url not in product_urls:
                        product_urls.append(full_url)
                        page_count += 1
        
        # Strategy 4: Look for data attributes or script tags with product URLs
        if page_count == 0:
            self.logger.info("Trying to extract URLs from data attributes and scripts...")
            # Check for data-product-url, data-href, etc.
            elements_with_data = soup.find_all(attrs={'data-product-url': True})
            elements_with_data.extend(soup.find_all(attrs={'data-href': True}))
            elements_with_data.extend(soup.find_all(attrs={'data-url': True}))
            
            for elem in elements_with_data:
                href = (elem.get('data-product-url') or 
                       elem.get('data-href') or 
                       elem.get('data-url'))
                if href:
                    full_url = self._normalize_product_url(href)
                    if full_url and full_url not in product_urls:
                        product_urls.append(full_url)
                        page_count += 1
        
        return page_count
    
    def _normalize_product_url(self, href):
        """Normalize and validate a product URL"""
        if not href:
            return None
        
        # Convert relative to absolute URL
        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
        
        # Remove query parameters and fragments
        if '?' in full_url:
            full_url = full_url.split('?')[0]
        if '#' in full_url:
            full_url = full_url.split('#')[0]
        
        # Normalize trailing slashes
        full_url = full_url.rstrip('/')
        
        # Validate it's a product URL (contains product path indicators)
        if any(pattern in full_url.lower() for pattern in ['/product/', '/parts/', '/oem-parts/', '/p/']):
            # Filter out non-product pages (like category pages, search pages, etc.)
            if not any(exclude in full_url.lower() for exclude in ['/search', '/category', '/catalog', '/browse']):
                return full_url
        
        return None
    
    def _scroll_to_load_content(self):
        """Scroll page to load lazy-loaded content"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scrolls = 30
            no_change_count = 0
            
            while scroll_attempts < max_scrolls:
                try:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1.5)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 3:
                            break
                    else:
                        no_change_count = 0
                    last_height = new_height
                    scroll_attempts += 1
                except:
                    break
        except:
            pass
    
    def scrape_product(self, url):
        """Scrape single product from parts.nissanusa.com"""
        max_retries = 5
        retry_count = 0
        html = None
        
        while retry_count < max_retries:
            try:
                if not self.check_health():
                    return None
                
                self.logger.info(f"Loading product page (attempt {retry_count + 1}/{max_retries}): {url}")
                
                original_timeout = self.page_load_timeout
                try:
                    self.ensure_driver()
                except Exception as driver_error:
                    recovery = self.error_handler.handle_error(driver_error, retry_count)
                    if recovery['should_retry'] and retry_count < max_retries - 1:
                        wait_time = recovery['wait_time']
                        delay = random.uniform(wait_time[0], wait_time[1])
                        time.sleep(delay)
                        retry_count += 1
                        continue
                    else:
                        return None
                
                try:
                    self.page_load_timeout = 60
                    self.driver.set_page_load_timeout(60)
                    self.driver.get(url)
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    if self.has_cloudflare_challenge():
                        cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                        if not cloudflare_bypassed:
                            retry_count += 1
                            if retry_count < max_retries:
                                time.sleep(random.uniform(10, 15))
                                continue
                            else:
                                return None
                    
                    time.sleep(random.uniform(1.5, 3.0))
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    
                    title_text = ''
                    title_elem = soup.find('h1')
                    if title_elem:
                        title_text = title_elem.get_text(strip=True)
                    
                    if not title_text or len(title_text) < 3:
                        title_tag = soup.find('title')
                        if title_tag:
                            title_text = title_tag.get_text(strip=True)
                            if '|' in title_text:
                                title_text = title_text.split('|')[0].strip()
                    
                    if not title_text or len(title_text) < 3:
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(random.uniform(10, 15))
                            continue
                        else:
                            return None
                    
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                    break
                    
                except TimeoutException:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(random.uniform(5, 8))
                        continue
                    else:
                        return None
                except Exception as e:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                    error_str = str(e).lower()
                    if any(err in error_str for err in ['connection', 'network', 'dns', 'err_', 'timeout']):
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(random.uniform(5, 8))
                            continue
                        else:
                            return None
                    else:
                        raise
                    
            except Exception as e:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
                recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                if not recovery['should_retry']:
                    return None
                if retry_count < max_retries - 1:
                    wait_time = recovery['wait_time']
                    delay = random.uniform(wait_time[0], wait_time[1])
                    time.sleep(delay)
                    retry_count += 1
                    continue
                else:
                    return None
        
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        
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
            # Extract title - Multiple strategies
            title_elem = soup.find('h1', class_='product-title')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_elem = soup.find('h1')
                if title_elem:
                    product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_elem = soup.find('meta', property='og:title')
                if title_elem:
                    product_data['title'] = title_elem.get('content', '').strip()
            
            if not product_data['title']:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    if '|' in title_text:
                        title_text = title_text.split('|')[0].strip()
                    product_data['title'] = title_text
            
            if not product_data['title'] or len(product_data['title']) < 3:
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - Multiple strategies
            # Priority 1: Extract from JSON data (sku_stripped) - RevolutionParts
            product_data_script = soup.find('script', id='product_data', type='application/json')
            if product_data_script:
                try:
                    json_data = json.loads(product_data_script.string)
                    if 'sku_stripped' in json_data and json_data['sku_stripped']:
                        product_data['pn'] = str(json_data['sku_stripped']).upper()
                        # Also get the formatted SKU for display
                        if 'sku' in json_data and json_data['sku']:
                            product_data['sku'] = str(json_data['sku']).upper()
                        else:
                            product_data['sku'] = product_data['pn']
                except json.JSONDecodeError:
                    pass
            
            # Priority 2: Extract from HTML - h2.sku-display or span.sku-display
            if not product_data['sku']:
                sku_elem = soup.find('h2', class_='sku-display')
                if not sku_elem:
                    sku_elem = soup.find('span', class_='sku-display')
                if sku_elem:
                    sku_text = sku_elem.get_text(strip=True)
                    if sku_text:
                        product_data['sku'] = sku_text.upper()
                        # Strip hyphens for PN
                        product_data['pn'] = re.sub(r'[-\s]', '', sku_text).upper()
            
            # Priority 3: Extract from add-to-cart button data-sku-stripped attribute
            if not product_data['pn']:
                add_to_cart_btn = soup.find('button', class_='add-to-cart')
                if add_to_cart_btn:
                    sku_stripped = add_to_cart_btn.get('data-sku-stripped', '')
                    if sku_stripped:
                        product_data['pn'] = sku_stripped.upper()
                        # Also get the formatted SKU
                        sku_formatted = add_to_cart_btn.get('data-sku', '')
                        if sku_formatted:
                            product_data['sku'] = sku_formatted.upper()
                        else:
                            product_data['sku'] = product_data['pn']
            
            # Priority 4: Extract from URL pattern: /oem-parts/nissan-wheel-{sku_stripped} or /product/...
            if not product_data['pn']:
                url_match = re.search(r'/oem-parts/nissan-[^/]+-([a-z0-9]+)(?:-|$|\?)', url, re.I)
                if not url_match:
                    url_match = re.search(r'/product/[^/]+-([a-z0-9]+)(?:-|$|\?)', url, re.I)
                if url_match:
                    product_data['pn'] = url_match.group(1).upper()
                    if not product_data['sku']:
                        product_data['sku'] = product_data['pn']
            
            # Priority 5: Generic SKU extraction
            if not product_data['sku']:
                sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
                if not sku_elem:
                    sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
                if sku_elem:
                    sku_text = self.safe_find_text(soup, sku_elem)
                    if sku_text:
                        product_data['sku'] = sku_text.upper()
                        product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Ensure SKU is set if we have PN
            if product_data['pn'] and not product_data['sku']:
                product_data['sku'] = product_data['pn']
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract price - Multiple strategies
            price_elem = soup.find('strong', id='product_price')
            if not price_elem:
                price_elem = soup.find('strong', class_='sale-price-value')
            if not price_elem:
                price_elem = soup.find('strong', class_='sale-price-amount')
            if not price_elem:
                price_elem = soup.find('span', class_=re.compile(r'price|sale.*price', re.I))
            if not price_elem:
                price_elem = soup.find('div', class_=re.compile(r'price|sale.*price', re.I))
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Also try from JSON data
            if not product_data['actual_price'] and product_data_script:
                try:
                    json_data = json.loads(product_data_script.string)
                    if 'price' in json_data and json_data['price']:
                        product_data['actual_price'] = self.extract_price(str(json_data['price']))
                except:
                    pass
            
            # Extract MSRP - Multiple strategies
            msrp_elem = soup.find('span', id='product_price2')
            if not msrp_elem:
                msrp_elem = soup.find('span', class_='list-price-value')
            if not msrp_elem:
                msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Also try from JSON data
            if not product_data['msrp'] and product_data_script:
                try:
                    json_data = json.loads(product_data_script.string)
                    if 'msrp' in json_data and json_data['msrp']:
                        product_data['msrp'] = self.extract_price(str(json_data['msrp']))
                except:
                    pass
            
            # Also try from add-to-cart button data attribute
            if not product_data['msrp']:
                add_to_cart_btn = soup.find('button', class_='add-to-cart')
                if add_to_cart_btn:
                    msrp_attr = add_to_cart_btn.get('data-msrp', '')
                    if msrp_attr:
                        product_data['msrp'] = self.extract_price(msrp_attr)
            
            # Extract image - Multiple strategies
            img_elem = soup.find('img', class_='product-main-image')
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src')
                if img_url:
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"{self.base_url}{img_url}"
                    else:
                        product_data['image_url'] = img_url
            
            # Fallback: Try img inside a.product-main-image-link
            if not product_data['image_url']:
                img_link = soup.find('a', class_='product-main-image-link')
                if img_link:
                    img_elem = img_link.find('img')
                    if img_elem:
                        img_url = img_elem.get('src') or img_elem.get('data-src')
                        if img_url:
                            if img_url.startswith('//'):
                                product_data['image_url'] = f"https:{img_url}"
                            elif img_url.startswith('/'):
                                product_data['image_url'] = f"{self.base_url}{img_url}"
                            else:
                                product_data['image_url'] = img_url
            
            # Fallback: Try any image with cdn-product-images or cdn-illustrations
            if not product_data['image_url']:
                img_elem = soup.find('img', src=re.compile(r'cdn-(product-images|illustrations)', re.I))
                if not img_elem:
                    img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
                if not img_elem:
                    img_elem = soup.find('img', itemprop='image')
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description - Multiple strategies
            desc_elem = soup.find('li', class_='description')
            if desc_elem:
                desc_body = desc_elem.find('span', class_='description_body')
                if desc_body:
                    desc_p = desc_body.find('p')
                    if desc_p:
                        product_data['description'] = desc_p.get_text(strip=True)
                    else:
                        product_data['description'] = desc_body.get_text(strip=True)
            
            # Fallback: Try from JSON data
            if not product_data['description'] and product_data_script:
                try:
                    json_data = json.loads(product_data_script.string)
                    if 'description' in json_data and json_data['description']:
                        desc_text = str(json_data['description'])
                        # Remove HTML tags if present
                        desc_text = re.sub(r'<[^>]+>', '', desc_text)
                        if desc_text and len(desc_text.strip()) > 5:
                            product_data['description'] = desc_text.strip()
                except:
                    pass
            
            # Fallback: Generic description extraction
            if not product_data['description']:
                desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
                if desc_elem:
                    desc_text = self.safe_find_text(soup, desc_elem)
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    product_data['description'] = desc_text.strip()
            
            # Extract "Also Known As" - Multiple strategies
            also_known_elem = soup.find('li', class_='also_known_as')
            if also_known_elem:
                also_known_value = also_known_elem.find('h2', class_='list-value')
                if not also_known_value:
                    also_known_value = also_known_elem.find('span', class_='list-value')
                if also_known_value:
                    also_known_text = also_known_value.get_text(strip=True)
                    if also_known_text and len(also_known_text) > 3:
                        product_data['also_known_as'] = also_known_text
            
            # Fallback: Try from JSON data
            if not product_data['also_known_as'] and product_data_script:
                try:
                    json_data = json.loads(product_data_script.string)
                    if 'also_known_as' in json_data and json_data['also_known_as']:
                        product_data['also_known_as'] = str(json_data['also_known_as']).strip()
                except:
                    pass
            
            # Extract "Replaces" - Multiple strategies
            replaces_elem = soup.find('li', class_='product-superseded-list')
            if replaces_elem:
                replaces_value = replaces_elem.find('h2', class_='list-value')
                if not replaces_value:
                    replaces_value = replaces_elem.find('span', class_='list-value')
                if replaces_value:
                    replaces_text = replaces_value.get_text(strip=True)
                    if replaces_text:
                        product_data['replaces'] = replaces_text
            
            # Extract fitment data - Multiple strategies
            # Method 1: Extract from JSON data in script tag
            if product_data_script:
                try:
                    json_data = json.loads(product_data_script.string)
                    fitments = json_data.get('fitment', []) or json_data.get('vehicles', [])
                    if fitments:
                        for fitment_entry in fitments:
                            try:
                                year = str(fitment_entry.get('year', '')).strip()
                                make = str(fitment_entry.get('make', '')).strip()
                                model = str(fitment_entry.get('model', '')).strip()
                                
                                # Handle trims array
                                trims = fitment_entry.get('trims', [])
                                if not trims:
                                    trim = str(fitment_entry.get('trim', '')).strip()
                                    trims = [trim] if trim else ['']
                                
                                # Handle engines array
                                engines = fitment_entry.get('engines', [])
                                if not engines:
                                    engine = str(fitment_entry.get('engine', '')).strip()
                                    engines = [engine] if engine else ['']
                                
                                # Generate all combinations: trim × engine
                                for trim_val in trims:
                                    for engine_val in engines:
                                        product_data['fitments'].append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': str(trim_val).strip() if trim_val else '',
                                            'engine': str(engine_val).strip() if engine_val else ''
                                        })
                            except:
                                continue
                except json.JSONDecodeError:
                    pass
            
            # Method 2: Extract from HTML fitment table
            if not product_data['fitments']:
                fitment_table = soup.find('table', class_='fitment-table')
                if fitment_table:
                    tbody = fitment_table.find('tbody', class_='fitment-table-body')
                    if tbody:
                        rows = tbody.find_all('tr', class_='fitment-row')
                        for row in rows:
                            try:
                                # Try class-based selectors first (more reliable)
                                year_cell = row.find('td', class_='fitment-year')
                                make_cell = row.find('td', class_='fitment-make')
                                model_cell = row.find('td', class_='fitment-model')
                                trim_cell = row.find('td', class_='fitment-trim')
                                engine_cell = row.find('td', class_='fitment-engine')
                                
                                if year_cell and make_cell and model_cell:
                                    year = year_cell.get_text(strip=True)
                                    make = make_cell.get_text(strip=True)
                                    model = model_cell.get_text(strip=True)
                                    trim_text = trim_cell.get_text(strip=True) if trim_cell else ''
                                    engine_text = engine_cell.get_text(strip=True) if engine_cell else ''
                                    
                                    # Split trim values by comma (e.g., "Evolution GSR, Evolution MR")
                                    trims = [t.strip() for t in trim_text.split(',')] if trim_text else ['']
                                    # Split engine values by comma if multiple engines exist
                                    engines = [e.strip() for e in engine_text.split(',')] if engine_text else ['']
                                    
                                    # Generate all combinations: trim × engine
                                    for trim_val in trims:
                                        for engine_val in engines:
                                            product_data['fitments'].append({
                                                'year': year,
                                                'make': make,
                                                'model': model,
                                                'trim': trim_val,
                                                'engine': engine_val
                                            })
                                else:
                                    # Fallback to index-based if class-based fails
                                    cells = row.find_all('td')
                                    if len(cells) >= 5:
                                        year = cells[0].get_text(strip=True)
                                        make = cells[1].get_text(strip=True)
                                        model = cells[2].get_text(strip=True)
                                        trim_text = cells[3].get_text(strip=True)
                                        engine_text = cells[4].get_text(strip=True)
                                        
                                        # Split trim values by comma
                                        trims = [t.strip() for t in trim_text.split(',')] if trim_text else ['']
                                        # Split engine values by comma
                                        engines = [e.strip() for e in engine_text.split(',')] if engine_text else ['']
                                        
                                        # Generate all combinations: trim × engine
                                        for trim_val in trims:
                                            for engine_val in engines:
                                                product_data['fitments'].append({
                                                    'year': year,
                                                    'make': make,
                                                    'model': model,
                                                    'trim': trim_val,
                                                    'engine': engine_val
                                                })
                            except:
                                continue
            
            # Method 3: Fallback - Extract from generic script tag
            if not product_data['fitments']:
                product_data_script_generic = soup.find('script', type='application/json')
                if product_data_script_generic and product_data_script_generic.string:
                    try:
                        product_json = json.loads(product_data_script_generic.string)
                        fitments = product_json.get('fitment', []) or product_json.get('vehicles', [])
                        if fitments:
                            for fitment_entry in fitments:
                                try:
                                    year = str(fitment_entry.get('year', '')).strip()
                                    make = str(fitment_entry.get('make', '')).strip()
                                    model = str(fitment_entry.get('model', '')).strip()
                                    trim = str(fitment_entry.get('trim', '')).strip()
                                    engine = str(fitment_entry.get('engine', '')).strip()
                                    product_data['fitments'].append({
                                        'year': year,
                                        'make': make,
                                        'model': model,
                                        'trim': trim,
                                        'engine': engine
                                    })
                                except:
                                    continue
                    except:
                        pass
            
            if not product_data['fitments']:
                product_data['fitments'].append({
                    'year': '',
                    'make': '',
                    'model': '',
                    'trim': '',
                    'engine': ''
                })
            
            self.logger.info(f"✅ Successfully scraped: {self.safe_str(product_data['title'])}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"❌ Error scraping product {url}: {self.safe_str(e)}")
            return None

