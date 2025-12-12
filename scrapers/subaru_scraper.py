"""Scraper for parts.subaru.com (Subaru parts)"""
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

class SubaruScraper(BaseScraper):
    """Scraper for parts.subaru.com"""
    
    def __init__(self):
        super().__init__('subaru', use_selenium=True)
        self.base_url = 'https://parts.subaru.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from parts.subaru.com - single search page only"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products from single search page...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            validated_urls = []
            for url in product_urls:
                # Product URLs: /p/Subaru__/Product-Name/ID/PartNumber.html
                # Category URLs: /Subaru__/Category.html or /productSearch.aspx
                is_product = re.search(r'/p/Subaru__/[^/]+/\d+/[^/]+\.html', url, re.I)
                is_category = re.search(r'/Subaru__/[^/]+\.html$|/productSearch\.aspx', url, re.I)
                
                if is_product and not is_category:
                    validated_urls.append(url)
            
            product_urls = validated_urls
            self.logger.info(f"Final validated product URLs: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """Search for wheels using site search - single page with 250 results, no pagination"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Search URL with numResults=250 to get all products on one page (no pagination)
            search_url = f"{self.base_url}/productSearch.aspx?ukey_make=5806&modelYear=0&ukey_model=0&ukey_trimLevel=0&ukey_driveline=0&ukey_Category=0&numResults=250&sortOrder=Relevance&ukey_tag=0&isOnSale=0&isAccessory=0&isPerformance=0&showAllModels=1&searchTerm=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                self.driver.get(search_url)
                time.sleep(3)
            except Exception as e:
                self.logger.error(f"Error loading search page: {str(e)}")
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links to appear using Selenium
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/']"))
                )
                self.logger.info("Product links detected on page")
            except TimeoutException:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products (lazy loading)
            self._scroll_to_load_content()
            
            # Wait a bit more for any dynamic content
            time.sleep(3)
            
            # Try to find product links using Selenium first (more reliable for dynamic content)
            try:
                selenium_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/p/Subaru__/']")
                self.logger.info(f"Found {len(selenium_links)} product links via Selenium")
                
                for link_elem in selenium_links:
                    try:
                        href = link_elem.get_attribute('href')
                        if href:
                            # Normalize URL
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove query parameters and fragments
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            
                            # Only collect individual product pages
                            if '/p/Subaru__/' in full_url and full_url.endswith('.html'):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    except Exception as e:
                        self.logger.debug(f"Error extracting link from Selenium element: {str(e)}")
                        continue
            except Exception as e:
                self.logger.debug(f"Error finding links via Selenium: {str(e)}")
            
            # Also try BeautifulSoup parsing as fallback
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Try multiple patterns to find product links (SimplePart platform)
            # Pattern 1: /p/Subaru__/Product-Name/ID/PartNumber.html
            product_links = soup.find_all('a', href=re.compile(r'/p/Subaru__/', re.I))
            
            # Pattern 2: Try looking in product containers/rows
            if not product_links:
                product_containers = soup.find_all(['div', 'li', 'tr'], class_=re.compile(r'product|item|part|row', re.I))
                for container in product_containers:
                    container_links = container.find_all('a', href=re.compile(r'/p/Subaru__/', re.I))
                    product_links.extend(container_links)
            
            # Pattern 3: Look for any link with /p/ pattern (case-insensitive)
            if not product_links:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    if href and '/p/' in href.lower() and 'subaru' in href.lower():
                        product_links.append(link)
            
            # Pattern 4: Try looking for links ending with .html in product sections
            if not product_links:
                product_sections = soup.find_all(['div', 'section', 'table', 'tbody'], class_=re.compile(r'product|result|search|item|row', re.I))
                for section in product_sections:
                    section_links = section.find_all('a', href=re.compile(r'\.html', re.I))
                    for link in section_links:
                        href = link.get('href', '')
                        if href and ('/p/' in href.lower() or 'subaru' in href.lower()):
                            product_links.append(link)
            
            # Pattern 5: Use JavaScript to find all links (most comprehensive)
            if not product_links:
                try:
                    js_links = self.driver.execute_script("""
                        var links = [];
                        var allLinks = document.querySelectorAll('a[href]');
                        for (var i = 0; i < allLinks.length; i++) {
                            var href = allLinks[i].href || allLinks[i].getAttribute('href');
                            if (href && href.toLowerCase().indexOf('/p/subaru__/') !== -1 && href.toLowerCase().endsWith('.html')) {
                                links.push(href);
                            }
                        }
                        return links;
                    """)
                    self.logger.info(f"Found {len(js_links)} product links via JavaScript")
                    for js_link in js_links:
                        if js_link and js_link not in product_urls:
                            # Normalize URL
                            full_url = js_link if js_link.startswith('http') else f"{self.base_url}{js_link}"
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            if '/p/Subaru__/' in full_url and full_url.endswith('.html'):
                                product_urls.append(full_url)
                except Exception as e:
                    self.logger.debug(f"Error finding links via JavaScript: {str(e)}")
            
            self.logger.info(f"Found {len(product_links)} potential product links via BeautifulSoup")
            
            # Extract and validate product URLs from BeautifulSoup results
            for link in product_links:
                href = link.get('href', '')
                if not href:
                    continue
                
                # Normalize URL
                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                
                # Remove query parameters and fragments
                if '?' in full_url:
                    full_url = full_url.split('?')[0]
                if '#' in full_url:
                    full_url = full_url.split('#')[0]
                full_url = full_url.rstrip('/')
                
                # Only collect individual product pages
                # SimplePart product URLs: /p/Subaru__/Product-Name/ID/PartNumber.html
                if '/p/Subaru__/' in full_url and full_url.endswith('.html'):
                    if full_url not in product_urls:
                        product_urls.append(full_url)
            
            # Remove duplicates and sort
            product_urls = sorted(list(set(product_urls)))
            self.logger.info(f"Extracted {len(product_urls)} unique product URLs from search page")
            
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
        """Scrape single product from parts.subaru.com"""
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
            # Extract title - SimplePart: span.prodDescriptH2
            title_elem = soup.find('span', class_='prodDescriptH2')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            # Fallback: h1 or h1 > span
            if not product_data['title']:
                title_elem = soup.find('h1')
                if title_elem:
                    span_elem = title_elem.find('span')
                    if span_elem:
                        product_data['title'] = span_elem.get_text(strip=True)
                    else:
                        product_data['title'] = title_elem.get_text(strip=True)
            
            # Fallback: meta itemprop="name"
            if not product_data['title']:
                title_meta = soup.find('meta', itemprop='name')
                if title_meta:
                    product_data['title'] = title_meta.get('content', '').strip()
            
            # Fallback: title tag
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
            
            # Extract SKU/Part Number - SimplePart: span.stock-code-text > strong
            # Look for: <span itemprop="value" class="body-3 stock-code-text"><strong>28111AJ250     </strong></span>
            stock_code_elem = soup.find('span', class_='stock-code-text')
            if not stock_code_elem:
                # Fallback: try with itemprop="value"
                stock_code_elem = soup.find('span', {'itemprop': 'value', 'class': re.compile(r'stock-code-text', re.I)})
            if stock_code_elem:
                strong_elem = stock_code_elem.find('strong')
                if strong_elem:
                    pn_text = strong_elem.get_text(strip=True)
                    if pn_text:
                        # Remove extra spaces and clean (handles cases like "28111AJ250     ")
                        product_data['pn'] = re.sub(r'\s+', '', pn_text).upper()
                        product_data['sku'] = product_data['pn']
            
            # Fallback: Extract from URL pattern: /p/Subaru__/Product-Name/ID/PartNumber.html
            if not product_data['pn']:
                url_match = re.search(r'/p/Subaru__/[^/]+/\d+/([^/]+)\.html', url)
                if url_match:
                    pn_from_url = url_match.group(1).upper()
                    # Remove any extra spaces or special characters
                    product_data['pn'] = re.sub(r'\s+', '', pn_from_url)
                    product_data['sku'] = product_data['pn']
            
            # Fallback: meta itemprop="sku"
            if not product_data['pn']:
                sku_meta = soup.find('meta', itemprop='sku')
                if sku_meta:
                    sku_value = sku_meta.get('content', '').strip()
                    if sku_value:
                        product_data['pn'] = re.sub(r'\s+', '', sku_value).upper()
                        product_data['sku'] = product_data['pn']
            
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
            
            # Extract price - SimplePart: span.productPriceSpan.money-3
            price_elem = soup.find('span', class_='productPriceSpan')
            if not price_elem:
                price_elem = soup.find('span', class_=re.compile(r'productPriceSpan|money-3', re.I))
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Fallback: meta itemprop="price" or JSON-LD
            if not product_data['actual_price']:
                price_meta = soup.find('meta', itemprop='price')
                if price_meta:
                    price_value = price_meta.get('content', '').strip()
                    if price_value:
                        product_data['actual_price'] = self.extract_price(price_value)
            
            # Also try from JSON-LD script tag
            if not product_data['actual_price']:
                json_ld_script = soup.find('script', type='application/ld+json')
                if json_ld_script:
                    try:
                        json_ld_data = json.loads(json_ld_script.string)
                        if isinstance(json_ld_data, dict) and 'offers' in json_ld_data:
                            offers = json_ld_data['offers']
                            if isinstance(offers, dict) and 'price' in offers:
                                product_data['actual_price'] = self.extract_price(str(offers['price']))
                    except:
                        pass
            
            # Extract MSRP - SimplePart: Look for price-header-heading "MSRP" followed by price
            msrp_container = soup.find('div', class_='price-header-heading')
            if msrp_container and 'MSRP' in msrp_container.get_text(strip=True):
                price_container = msrp_container.find_next('div', class_='price-header-price')
                if price_container:
                    msrp_text = price_container.get_text(strip=True)
                    product_data['msrp'] = self.extract_price(msrp_text)
            
            # Fallback: Try to find MSRP in price header quadrant
            if not product_data['msrp']:
                price_header = soup.find('div', class_='price-header-title')
                if price_header:
                    msrp_elem = price_header.find('div', class_='price-header-price')
                    if msrp_elem:
                        msrp_text = msrp_elem.get_text(strip=True)
                        product_data['msrp'] = self.extract_price(msrp_text)
            
            # If MSRP not found, use actual_price as fallback
            if not product_data['msrp'] and product_data['actual_price']:
                product_data['msrp'] = product_data['actual_price']
            
            # Extract image - SimplePart: img[itemprop="image"] in assembly preview
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
            
            # Fallback: Look for assembly preview image
            if not product_data['image_url']:
                assembly_img = soup.find('img', class_='assemblyPreviewFullImg')
                if assembly_img:
                    img_url = assembly_img.get('src') or assembly_img.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Fallback: meta itemprop="image" or JSON-LD
            if not product_data['image_url']:
                image_meta = soup.find('meta', itemprop='image')
                if image_meta:
                    img_url = image_meta.get('content', '').strip()
                    if img_url:
                        product_data['image_url'] = img_url
            
            # Extract description - SimplePart: div.item-desc > p
            desc_div = soup.find('div', class_='item-desc')
            if desc_div:
                desc_paragraphs = desc_div.find_all('p')
                desc_texts = []
                for p in desc_paragraphs:
                    p_text = p.get_text(strip=True, separator=' ')
                    if p_text:
                        desc_texts.append(p_text)
                if desc_texts:
                    product_data['description'] = ' '.join(desc_texts).strip()
            
            # Fallback: meta itemprop="description"
            if not product_data['description']:
                desc_meta = soup.find('meta', itemprop='description')
                if desc_meta:
                    desc_text = desc_meta.get('content', '').strip()
                    if desc_text:
                        product_data['description'] = desc_text
            
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
            
            # Fallback: Try from JSON-LD script tag
            if not product_data['also_known_as']:
                json_ld_script = soup.find('script', type='application/ld+json')
                if json_ld_script:
                    try:
                        json_ld_data = json.loads(json_ld_script.string)
                        if isinstance(json_ld_data, dict) and 'name' in json_ld_data:
                            # Also known as might be in alternative names or similar fields
                            pass
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
            
            # Extract fitment data - SimplePart: whatThisFitsFitment and whatThisFitsYears divs
            # Structure: <div class="col-md-12">
            #             <div class="col-lg-12">
            #               <div class="whatThisFitsFitment col-lg-6"><span>Subaru Outback 2.5L CVT Plus </span></div>
            #               <div class="whatThisFitsYears col-lg-6"><span><a href="...">2014</a></span></div>
            #             </div>
            #           </div>
            # Look for fitment data in col-lg-12 containers that contain both fitment and years divs
            fitment_container = soup.find('div', class_='col-md-12')
            if fitment_container:
                fitment_rows = fitment_container.find_all('div', class_='col-lg-12')
            else:
                # Fallback: look for all col-lg-12 divs
                fitment_rows = soup.find_all('div', class_='col-lg-12')
            
            for row in fitment_rows:
                fitment_cell = row.find('div', class_='whatThisFitsFitment')
                years_cell = row.find('div', class_='whatThisFitsYears')
                
                if fitment_cell and years_cell:
                    # Extract vehicle description
                    vehicle_desc = fitment_cell.get_text(strip=True)
                    
                    if not vehicle_desc:
                        continue
                    
                    # Make is always "Subaru"
                    make = 'Subaru'
                    
                    # Parse vehicle description: "Subaru Outback 2.5L CVT Plus"
                    # Remove "Subaru" prefix
                    vehicle_desc_clean = re.sub(r'^Subaru\s+', '', vehicle_desc, flags=re.I).strip()
                    
                    # Extract model (first word after "Subaru")
                    words = vehicle_desc_clean.split()
                    model = ''
                    if len(words) >= 1:
                        model = words[0]  # "Outback", "Forester", etc.
                    
                    # Extract engine (pattern like "2.5L CVT", "3.6L V6", "2.0L Turbo", etc.)
                    # Look for patterns: "X.XL" or "XL" followed by transmission/engine type (CVT, V6, Turbo, etc.)
                    engine_match = re.search(r'(\d+\.?\d*L\s+[A-Z0-9/]+(?:\s+[A-Z0-9/]+)?)', vehicle_desc_clean, re.I)
                    engine = engine_match.group(1).strip() if engine_match else ''
                    
                    # Extract trim - typically the last word(s) after engine
                    # Examples: "Plus", "Premium", "Limited", "Touring", "Base", "Sport", etc.
                    trim = ''
                    if engine:
                        # Remove model and engine to get trim
                        trim_text = vehicle_desc_clean
                        trim_text = re.sub(r'^' + re.escape(model) + r'\s+', '', trim_text, flags=re.I)
                        trim_text = re.sub(r'^' + re.escape(engine) + r'\s+', '', trim_text, flags=re.I)
                        trim = trim_text.strip()
                    else:
                        # If no engine found, try to extract trim from end
                        # Remove model
                        trim_text = vehicle_desc_clean
                        trim_text = re.sub(r'^' + re.escape(model) + r'\s+', '', trim_text, flags=re.I)
                        # Common trim patterns for Subaru
                        trim_match = re.search(r'\b(Plus|Premium|Limited|Touring|Base|Sport|XT|STI|WRX|Onyx|Wilderness|Outdoor|Convenience)\b', trim_text, re.I)
                        if trim_match:
                            trim = trim_match.group(1).strip()
                        else:
                            # If no match, take the remaining text as trim
                            trim = trim_text.strip()
                    
                    # Extract years from years_cell
                    year_links = years_cell.find_all('a')
                    years = []
                    if year_links:
                        for link in year_links:
                            year_text = link.get_text(strip=True)
                            # Extract 4-digit year
                            year_match = re.search(r'(\d{4})', year_text)
                            if year_match:
                                years.append(year_match.group(1))
                    else:
                        # Fallback: extract years from text (could be comma-separated)
                        years_text = years_cell.get_text(strip=True)
                        # Try to find year ranges first (e.g., "2010-2015")
                        year_range_matches = re.findall(r'(\d{4})\s*-\s*(\d{4})', years_text)
                        if year_range_matches:
                            for start_year, end_year in year_range_matches:
                                try:
                                    start = int(start_year)
                                    end = int(end_year)
                                    years.extend([str(y) for y in range(start, end + 1)])
                                except:
                                    pass
                        else:
                            # Extract individual years (comma-separated)
                            year_matches = re.findall(r'(\d{4})', years_text)
                            years = year_matches
                    
                    # Create fitment entry for each year (handle multiple years per fitment)
                    if years:
                        for year in years:
                            product_data['fitments'].append({
                                'year': year,
                                'make': make,
                                'model': model,
                                'trim': trim,
                                'engine': engine
                            })
                    else:
                        # If no years found, create one entry without year
                        product_data['fitments'].append({
                            'year': '',
                            'make': make,
                            'model': model,
                            'trim': trim,
                            'engine': engine
                        })
            
            # Fallback: Try to extract from guided navigation year links
            if not product_data['fitments']:
                guided_nav = soup.find('div', class_='guided-nav')
                if guided_nav:
                    year_links = guided_nav.find_all('a', class_='guided-nav__body__item')
                    if year_links:
                        # Extract year from link text or href
                        for link in year_links:
                            year_text = link.get_text(strip=True)
                            year_match = re.search(r'(\d{4})', year_text)
                            if year_match:
                                year = year_match.group(1)
                                # Make is always Subaru
                                # Model and other details might be in the page context
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': 'Subaru',
                                    'model': '',
                                    'trim': '',
                                    'engine': ''
                                })
            
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

