"""Scraper for www.hondapartsonline.net (Honda parts)"""
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

class HondaScraper(BaseScraper):
    """Scraper for www.hondapartsonline.net"""
    
    def __init__(self):
        super().__init__('honda', use_selenium=True)
        self.base_url = 'https://www.hondapartsonline.net'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.hondapartsonline.net"""
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
        """Search for wheels using site search"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Load initial search page
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
            
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/product/'], a[href*='/parts/'], a[href*='/oem-parts/'], a[href*='/p/']"))
                )
            except:
                pass
            
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Find product links - try multiple patterns
            product_links = (soup.find_all('a', href=re.compile(r'/product/')) +
                           soup.find_all('a', href=re.compile(r'/parts/')) +
                           soup.find_all('a', href=re.compile(r'/oem-parts/')) +
                           soup.find_all('a', href=re.compile(r'/p/')))
            
            for link in product_links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    # Remove fragment (#)
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    # Extract only the base product URL, remove page-specific query params
                    if any(pattern in full_url for pattern in ['/product/', '/parts/', '/oem-parts/', '/p/']):
                        if '?' in full_url:
                            full_url = full_url.split('?')[0]
                        # Normalize trailing slashes
                        full_url = full_url.rstrip('/')
                        if full_url not in product_urls:
                            product_urls.append(full_url)
            
            self.logger.info(f"Initial page: Found {len(product_links)} product links, {len(product_urls)} unique URLs")
            
            # Handle pagination - iterate through all pages
            page_num = 2
            max_pages = 2000  # Safety limit
            consecutive_empty_pages = 0
            max_consecutive_empty = 4  # Stop after 4 consecutive pages with no new products
            
            while page_num <= max_pages:
                try:
                    self.logger.info(f"Loading page {page_num}...")
                    
                    # Try multiple pagination URL patterns
                    pagination_urls = [
                        f"{self.base_url}/search?search_str=wheel&page={page_num}",
                        f"{self.base_url}/search?search_str=wheel&p={page_num}",
                        f"{self.base_url}/search?search_str=wheel&pageNumber={page_num}",
                        f"{self.base_url}/search?q=wheel&page={page_num}",
                    ]
                    
                    page_loaded = False
                    pag_url_used = None
                    
                    for pag_url in pagination_urls:
                        try:
                            self.logger.debug(f"Trying pagination URL: {pag_url}")
                            
                            # Increase timeout for pagination pages
                            original_pag_timeout = self.page_load_timeout
                            try:
                                self.page_load_timeout = 60
                                self.driver.set_page_load_timeout(60)
                                
                                # Load the page directly
                                self.driver.get(pag_url)
                                time.sleep(2)  # Wait for page to load
                                
                                # Wait for product links to appear
                                try:
                                    WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/product/'], a[href*='/parts/'], a[href*='/oem-parts/'], a[href*='/p/']"))
                                    )
                                except:
                                    self.logger.debug(f"Product links not found immediately on {pag_url}, continuing...")
                                
                                # Check if page loaded successfully (has product links)
                                page_links_check = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/product/'], a[href*='/parts/'], a[href*='/oem-parts/'], a[href*='/p/']")
                                if len(page_links_check) > 0:
                                    # Page loaded successfully
                                    page_loaded = True
                                    pag_url_used = pag_url
                                    self.logger.info(f"✓ Successfully loaded page {page_num} using URL: {pag_url}")
                                    break
                            except Exception as pag_error:
                                error_str = str(pag_error).lower()
                                if 'timeout' in error_str:
                                    self.logger.debug(f"Timeout loading {pag_url}, trying next pattern...")
                                continue
                            finally:
                                # Restore timeout
                                try:
                                    self.page_load_timeout = original_pag_timeout
                                    self.driver.set_page_load_timeout(original_pag_timeout)
                                except:
                                    pass
                        except Exception as pag_error:
                            continue
                    
                    if not page_loaded:
                        self.logger.warning(f"Could not load page {page_num} with any URL pattern")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages failed to load")
                            break
                        page_num += 1
                        continue
                    
                    # Scroll to load all products on this page
                    try:
                        last_height = self.driver.execute_script("return document.body.scrollHeight")
                        scroll_attempts = 0
                        no_change_count = 0
                        
                        while scroll_attempts < 30:  # Limit per page
                            try:
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(1)
                                new_height = self.driver.execute_script("return document.body.scrollHeight")
                                
                                if new_height == last_height:
                                    no_change_count += 1
                                    if no_change_count >= 3:
                                        break
                                else:
                                    no_change_count = 0
                                last_height = new_height
                                scroll_attempts += 1
                            except Exception as scroll_error:
                                self.logger.warning(f"Error during pagination scroll: {str(scroll_error)}")
                                break
                    except Exception as scroll_init_error:
                        self.logger.warning(f"Error initializing pagination scroll: {str(scroll_init_error)}")
                    
                    # Extract product links from this page
                    try:
                        html = self.driver.page_source
                    except Exception as page_source_error:
                        self.logger.warning(f"Error accessing page_source on page {page_num}: {str(page_source_error)}")
                        # Try to get HTML via get_page() as fallback
                        html = self.get_page(pag_url_used, use_selenium=True, wait_time=1)
                        if not html:
                            self.logger.warning(f"Could not retrieve page source for page {page_num}, skipping")
                            page_num += 1
                            continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    page_links = (soup.find_all('a', href=re.compile(r'/product/')) +
                                 soup.find_all('a', href=re.compile(r'/parts/')) +
                                 soup.find_all('a', href=re.compile(r'/oem-parts/')) +
                                 soup.find_all('a', href=re.compile(r'/p/')))
                    
                    page_urls_count = 0
                    for link in page_links:
                        href = link.get('href', '')
                        if href:
                            # Normalize URL: extract base product URL (remove query params that change per page)
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove fragment (#)
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            
                            # IMPORTANT: Extract only the base product URL, remove page-specific query params
                            # This ensures we get unique products across pages
                            if any(pattern in full_url for pattern in ['/product/', '/parts/', '/oem-parts/', '/p/']):
                                # Extract just the product path, remove all query params
                                if '?' in full_url:
                                    full_url = full_url.split('?')[0]
                            
                            # Normalize trailing slashes
                            full_url = full_url.rstrip('/')
                            
                            if full_url not in product_urls:
                                product_urls.append(full_url)
                                page_urls_count += 1
                    
                    self.logger.info(f"Page {page_num}: Found {len(page_links)} product links, {page_urls_count} new unique URLs (total: {len(product_urls)})")
                    
                    # Check if we got new products
                    if page_urls_count == 0:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages with no new products")
                            break
                    else:
                        consecutive_empty_pages = 0  # Reset counter if we found new products
                    
                    # Add delay between pages
                    time.sleep(random.uniform(2, 4))
                    page_num += 1
                    
                except Exception as page_error:
                    self.logger.error(f"Error processing page {page_num}: {str(page_error)}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages with errors")
                        break
                    page_num += 1
                    continue
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
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
        """Scrape single product from www.hondapartsonline.net"""
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
            # Extract title - try multiple selectors (priority: h1 > og:title > title tag)
            title_elem = soup.find('h1')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                # Try meta og:title
                og_title = soup.find('meta', property='og:title')
                if og_title:
                    product_data['title'] = og_title.get('content', '').strip()
            
            if not product_data['title'] or len(product_data['title']) < 3:
                # Try title tag as last resort
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # Clean up title: "2018 Honda Accord Wheel 12345 | Honda Parts Online"
                    if '|' in title_text:
                        title_text = title_text.split('|')[0].strip()
                    product_data['title'] = title_text
            
            if not product_data['title'] or len(product_data['title']) < 3:
                self.logger.warning(f"⚠️ No valid title found for {url}")
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - try multiple selectors
            sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                sku_elem = soup.find('h2', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                # Try to extract from title or URL if it contains part number
                # Example: "2018 Honda Accord Wheel 12345"
                title_words = product_data['title'].split()
                for word in reversed(title_words):
                    if word.isdigit() and len(word) >= 6:  # Part numbers are usually 6+ digits
                        product_data['sku'] = word
                        product_data['pn'] = self.clean_sku(word)
                        break
            
            if sku_elem and not product_data['sku']:
                product_data['sku'] = sku_elem.get_text(strip=True)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    self.logger.info(f"⏭️ Skipping non-wheel product: {product_data['title']}")
                    return None
            except Exception as e:
                self.logger.warning(f"Error checking if wheel product: {str(e)}")
                return None
            
            # Extract sale price - try multiple selectors
            sale_price_elem = soup.find('strong', id='product_price')
            if not sale_price_elem:
                sale_price_elem = soup.find('strong', class_=re.compile(r'sale.*price', re.I))
            if not sale_price_elem:
                sale_price_elem = soup.find('span', id='product_price')
            if not sale_price_elem:
                sale_price_elem = soup.find('span', class_=re.compile(r'sale.*price', re.I))
            if not sale_price_elem:
                sale_price_elem = soup.find('div', class_=re.compile(r'sale.*price|price.*value', re.I))
            if not sale_price_elem:
                # Try meta tag
                price_meta = soup.find('meta', itemprop='price')
                if price_meta:
                    product_data['actual_price'] = self.extract_price(price_meta.get('content', ''))
            
            if sale_price_elem and not product_data['actual_price']:
                price_text = sale_price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Extract MSRP - try multiple selectors
            msrp_elem = soup.find('span', id='product_price2')
            if not msrp_elem:
                msrp_elem = soup.find('span', class_=re.compile(r'list.*price', re.I))
            if not msrp_elem:
                msrp_elem = soup.find('div', class_=re.compile(r'msrp|list.*price', re.I))
            if not msrp_elem:
                # Try to find "compared" price (MSRP is often shown as comparison)
                msrp_elem = soup.find('span', class_=re.compile(r'compared|compare.*price', re.I))
            
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Extract image URL - try multiple sources
            img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
            if not img_elem:
                img_elem = soup.find('img', itemprop='image')
            if not img_elem:
                # Try to find any product-related image
                img_elem = soup.find('img', {'src': re.compile(r'product|part|wheel', re.I)})
            
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src') or img_elem.get('data-original')
                if img_url:
                    # Normalize image URL
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"{self.base_url}{img_url}"
                    else:
                        product_data['image_url'] = img_url
            
            # Fallback: try og:image meta tag
            if not product_data['image_url']:
                og_image = soup.find('meta', property='og:image')
                if og_image:
                    img_url = og_image.get('content', '')
                if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description - try multiple sources
            # First try meta og:description (often contains structured description)
            desc_elem = soup.find('meta', property='og:description')
            if desc_elem:
                product_data['description'] = desc_elem.get('content', '').strip()
            
            # If not found, try HTML elements
            if not product_data['description']:
                desc_elem = soup.find('span', class_='description_body')
                if not desc_elem:
                    desc_elem = soup.find('li', class_='description')
                    if desc_elem:
                        desc_elem = desc_elem.find('span', class_='list-value')
                if not desc_elem:
                    desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
                if not desc_elem:
                    desc_elem = soup.find('p', class_=re.compile(r'description', re.I))
                
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True, separator=' ')
                desc_text = re.sub(r'\s+', ' ', desc_text)
                product_data['description'] = desc_text.strip()
            
            # Extract also_known_as (Other Names)
            also_known_elem = soup.find('li', class_='also_known_as')
            if also_known_elem:
                value_elem = also_known_elem.find('h2', class_='list-value')
                if not value_elem:
                    value_elem = also_known_elem.find('span', class_='list-value')
                if value_elem:
                    product_data['also_known_as'] = value_elem.get_text(strip=True)
            
            # Extract replaces
            replaces_elem = soup.find('li', class_='product-superseded-list')
            if not replaces_elem:
                replaces_elem = soup.find('li', class_=re.compile(r'superseded|replaces', re.I))
            if replaces_elem:
                value_elem = replaces_elem.find('h2', class_='list-value')
                if not value_elem:
                    value_elem = replaces_elem.find('span', class_='list-value')
                if value_elem:
                    product_data['replaces'] = value_elem.get_text(strip=True)
            
            # Extract fitment data from JSON script tag
            # Priority: script with id="product_data" and type="application/json"
            script_elem = soup.find('script', {'type': 'application/json', 'id': 'product_data'})
            if not script_elem:
                # Try just id="product_data"
                script_elem = soup.find('script', id='product_data')
            if not script_elem:
                # Try alternative script tags with type="application/json"
                script_tags = soup.find_all('script', type='application/json')
                for tag in script_tags:
                    if tag.string and ('fitment' in tag.string.lower() or 'vehicle' in tag.string.lower()):
                        script_elem = tag
                        break
            
            if script_elem and script_elem.string:
                try:
                    product_json = json.loads(script_elem.string)
                    
                    # Try multiple possible keys for fitment data
                    fitments = product_json.get('fitment', [])
                    if not fitments:
                        fitments = product_json.get('fitments', [])
                    if not fitments:
                        fitments = product_json.get('vehicles', [])
                    if not fitments and isinstance(product_json, dict):
                        # Sometimes fitment is nested
                        for key in ['product', 'data', 'details']:
                            if key in product_json and isinstance(product_json[key], dict):
                                fitments = product_json[key].get('fitment', []) or product_json[key].get('fitments', [])
                                if fitments:
                                    break
                    
                    if fitments:
                        for fitment in fitments:
                            try:
                                year = str(fitment.get('year', '')).strip()
                                make = fitment.get('make', '').strip()
                                model = fitment.get('model', '').strip()
                                trims = fitment.get('trims', [])
                                engines = fitment.get('engines', [])
                                
                                # Handle case where trim/engine might be single values
                                if not isinstance(trims, list):
                                    trims = [trims] if trims else ['']
                                if not isinstance(engines, list):
                                    engines = [engines] if engines else ['']
                                
                                # Create a row for each trim/engine combination
                                if not trims:
                                    trims = ['']
                                if not engines:
                                    engines = ['']
                                
                                for trim in trims:
                                    for engine in engines:
                                        product_data['fitments'].append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': str(trim).strip() if trim else '',
                                            'engine': str(engine).strip() if engine else ''
                                        })
                            except Exception as fitment_error:
                                self.logger.debug(f"Error processing fitment: {str(fitment_error)}")
                                continue
                    
                    if product_data['fitments']:
                        self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations")
                    
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Error parsing JSON: {str(e)}")
                except Exception as e:
                    self.logger.warning(f"Error extracting fitments: {str(e)}")
            
            # If no fitments found, still return the product with empty fitment
            if not product_data['fitments']:
                self.logger.warning(f"⚠️ No fitment data found for {product_data['title']}")
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
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return None

