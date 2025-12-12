"""Scraper for autoparts.toyota.com (Toyota parts)"""
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

class ToyotaScraper(BaseScraper):
    """Scraper for autoparts.toyota.com"""
    
    def __init__(self):
        super().__init__('toyota', use_selenium=True)
        self.base_url = 'https://autoparts.toyota.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from autoparts.toyota.com"""
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
            
            # Initial search URL
            search_url = f"{self.base_url}/search?search_query=wheel"
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/product/'], a[href*='/parts/'], a[href*='/oem-parts/'], div[class*='product'], article[class*='product']"))
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
                    # Try multiple pagination URL patterns - Toyota uses p= parameter
                    pagination_urls = [
                        f"{self.base_url}/search?search_query=wheel&p={page_num}",
                        f"{self.base_url}/search?search_query=wheel&page={page_num}",
                        f"{self.base_url}/search?q=wheel&p={page_num}",
                    ]
                    
                    page_loaded = False
                    pag_url_used = None
                    
                    for pag_url in pagination_urls:
                        try:
                            self.page_load_timeout = 60
                            self.driver.set_page_load_timeout(60)
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                continue
                            
                            # Wait for products to load
                            try:
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/product/'], a[href*='/parts/'], a[href*='/oem-parts/'], div[class*='product'], article[class*='product']"))
                                )
                            except:
                                pass
                            
                            # Scroll to load all products on this page
                            self._scroll_to_load_content()
                            pag_html = self.driver.page_source
                            soup = BeautifulSoup(pag_html, 'lxml')
                            
                            page_loaded = True
                            pag_url_used = pag_url
                            break
                        except Exception as e:
                            self.logger.debug(f"Failed to load pagination URL {pag_url}: {str(e)}")
                            continue
                        finally:
                            try:
                                self.page_load_timeout = original_timeout
                                self.driver.set_page_load_timeout(original_timeout)
                            except:
                                pass
                    
                    if not page_loaded:
                        self.logger.warning(f"Page {page_num} could not be loaded, stopping pagination")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            break
                        page_num += 1
                        continue
                    
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
        """Extract product URLs from a search results page by finding wheel-related product titles"""
        page_count = 0
        
        # Strategy 1: Find product titles containing "wheel" and extract their associated product URLs
        # Toyota specific: Look for <p data-atom="typography" data-size="body16" class="sc-dntaoT sc-kFFtEL eAapJK wLKKB">
        title_elems = soup.find_all('p', {
            'data-atom': 'typography',
            'data-size': 'body16'
        })
        
        for title_elem in title_elems:
            title_text = title_elem.get_text(strip=True).lower()
            # Check if title contains "wheel" (case-insensitive)
            if 'wheel' in title_text:
                # Navigate to find the product link associated with this title
                # The link is likely in a parent container (product card)
                product_link = None
                
                # Method 1: Find the product card container and look for link within it
                # Look for common product card containers
                container = None
                parent = title_elem.parent
                max_parent_depth = 10
                depth = 0
                
                while parent and depth < max_parent_depth:
                    # Check if this parent looks like a product card/container
                    parent_class = parent.get('class', [])
                    parent_class_str = ' '.join(parent_class).lower() if parent_class else ''
                    parent_data = parent.get('data-atom', '') or parent.get('data-molecule', '') or ''
                    
                    # Check if it's a product card or container
                    if (any(keyword in parent_class_str for keyword in ['product', 'card', 'item']) or
                        'product' in parent_data.lower() or
                        parent.name == 'a'):
                        container = parent
                        break
                    parent = parent.parent
                    depth += 1
                
                # If we found a container, look for the product link
                if container:
                    if container.name == 'a' and re.search(r'/product/', container.get('href', ''), re.I):
                        product_link = container
                    else:
                        # Look for link with /product/ pattern in the container
                        link = container.find('a', href=re.compile(r'/product/', re.I))
                        if link:
                            product_link = link
                
                # Method 2: If not found, search in all parent elements for any product link
                if not product_link:
                    parent = title_elem.parent
                    depth = 0
                    while parent and depth < max_parent_depth:
                        link = parent.find('a', href=re.compile(r'/product/', re.I))
                        if link:
                            product_link = link
                            break
                        if parent.name == 'a' and re.search(r'/product/', parent.get('href', ''), re.I):
                            product_link = parent
                            break
                        parent = parent.parent
                        depth += 1
                
                # Method 3: Search in siblings and nearby elements
                if not product_link:
                    # Check previous and next siblings
                    for sibling in title_elem.find_previous_siblings(limit=3):
                        if sibling.name == 'a' and re.search(r'/product/', sibling.get('href', ''), re.I):
                            product_link = sibling
                            break
                        link = sibling.find('a', href=re.compile(r'/product/', re.I))
                        if link:
                            product_link = link
                            break
                    
                    if not product_link:
                        for sibling in title_elem.find_next_siblings(limit=3):
                            if sibling.name == 'a' and re.search(r'/product/', sibling.get('href', ''), re.I):
                                product_link = sibling
                                break
                            link = sibling.find('a', href=re.compile(r'/product/', re.I))
                            if link:
                                product_link = link
                                break
                
                if product_link:
                    href = product_link.get('href', '')
                    if href:
                        full_url = self._normalize_product_url(href)
                        if full_url and full_url not in product_urls:
                            product_urls.append(full_url)
                            page_count += 1
                            self.logger.debug(f"Found wheel product: {title_text[:50]} -> {full_url}")
        
        # Strategy 2: Fallback - Look for product containers with wheel-related titles
        if page_count == 0:
            self.logger.info("Trying to find products in containers with wheel-related content...")
            product_containers = (
                soup.find_all('div', class_=re.compile(r'product|item|result|card', re.I)) +
                soup.find_all('article', class_=re.compile(r'product|item|result|card', re.I)) +
                soup.find_all('li', class_=re.compile(r'product|item|result|card', re.I)) +
                soup.find_all('a', href=re.compile(r'/product/', re.I))
            )
            
            for container in product_containers:
                # Check if container has wheel-related text
                container_text = container.get_text(strip=True).lower()
                if 'wheel' in container_text:
                    # Find product link in this container
                    if container.name == 'a':
                        link = container
                    else:
                        link = container.find('a', href=re.compile(r'/product/', re.I))
                    
                    if link:
                        href = link.get('href', '')
                        if href:
                            full_url = self._normalize_product_url(href)
                            if full_url and full_url not in product_urls:
                                product_urls.append(full_url)
                                page_count += 1
        
        # Strategy 3: Fallback - Direct search for all product links (if no wheel-specific products found)
        if page_count == 0:
            self.logger.info("No wheel products found by title, trying direct link search...")
            product_links = soup.find_all('a', href=re.compile(r'/product/', re.I))
            
            for link in product_links:
                href = link.get('href', '')
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
        """Scrape single product from autoparts.toyota.com"""
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
            # Extract title - Toyota specific: h1.sc-fguEHw.oiszD or h1
            title_elem = soup.find('h1', class_=re.compile(r'sc-fguEHw|oiszD', re.I))
            if not title_elem:
                title_elem = soup.find('h1')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            # Fallback: Try JSON-LD data
            if not product_data['title']:
                json_ld_script = soup.find('script', type='application/ld+json')
                if json_ld_script:
                    try:
                        json_ld_data = json.loads(json_ld_script.string)
                        if isinstance(json_ld_data, dict) and 'name' in json_ld_data:
                            product_data['title'] = json_ld_data['name'].strip()
                    except:
                        pass
            
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
            
            # Extract SKU/Part Number - Toyota specific
            # Priority 1: Extract from HTML - Toyota specific: p with class containing "jzOfyj" in Product Specs section
            # Look for Part Number in Product Specs section
            json_ld_script = soup.find('script', type='application/ld+json')
            
            # Find Product Specs section
            specs_section = soup.find('div', {'data-molecule': 'product-detail', 'data-part': 'specs'})
            if specs_section:
                # Find all p tags with typography
                spec_items = specs_section.find_all('p', {'data-atom': 'typography', 'data-size': 'body14'})
                for i, item in enumerate(spec_items):
                    text = item.get_text(strip=True)
                    # Look for "Part Number" label followed by the value
                    if text and 'part number' in text.lower() and i + 1 < len(spec_items):
                        part_number_elem = spec_items[i + 1]
                        part_number_text = part_number_elem.get_text(strip=True)
                        if part_number_text:
                            product_data['sku'] = part_number_text.upper()
                            product_data['pn'] = re.sub(r'[-\s]', '', part_number_text).upper()
                            break
            
            # Priority 2: Try to find Part Number in any p tag with the specific classes
            if not product_data['sku']:
                # Look for pattern: p with class "sc-dntaoT jzOfyj" containing part number pattern
                part_number_elems = soup.find_all('p', class_=re.compile(r'jzOfyj', re.I))
                for elem in part_number_elems:
                    text = elem.get_text(strip=True)
                    # Check if it looks like a part number (e.g., "42603-20620")
                    if re.match(r'^\d{5}-\d{5}$', text) or re.match(r'^\d{5}-[A-Z0-9]{5}$', text):
                        product_data['sku'] = text.upper()
                        product_data['pn'] = re.sub(r'[-\s]', '', text).upper()
                        break
            
            # Priority 3: Extract from JSON-LD data (mpn field) - this gives PN without hyphens
            if not product_data['pn'] and json_ld_script:
                try:
                    json_ld_data = json.loads(json_ld_script.string)
                    if isinstance(json_ld_data, dict) and 'mpn' in json_ld_data:
                        product_data['pn'] = str(json_ld_data['mpn']).strip().upper()
                        # Try to reconstruct SKU with hyphens if we have PN
                        if product_data['pn'] and not product_data['sku']:
                            pn_clean = product_data['pn']
                            if len(pn_clean) >= 10:
                                product_data['sku'] = f"{pn_clean[:5]}-{pn_clean[5:]}"
                except:
                    pass
            
            # Priority 4: Extract from URL pattern
            if not product_data['pn']:
                url_match = re.search(r'/product/[^/]+-(\d+[A-Z0-9-]+)', url, re.I)
                if url_match:
                    pn_from_url = url_match.group(1).upper()
                    product_data['pn'] = re.sub(r'[-\s]', '', pn_from_url).upper()
                    if not product_data['sku']:
                        # Try to reconstruct SKU with hyphens (format: 42603-20620)
                        if len(pn_from_url) >= 10:
                            product_data['sku'] = f"{pn_from_url[:5]}-{pn_from_url[5:]}".upper()
            
            # Ensure SKU is set if we have PN (add hyphens back)
            if product_data['pn'] and not product_data['sku']:
                # Try to format PN as SKU (e.g., 4260320620 -> 42603-20620)
                pn_clean = product_data['pn']
                if len(pn_clean) >= 10 and pn_clean.isdigit():
                    product_data['sku'] = f"{pn_clean[:5]}-{pn_clean[5:]}"
                else:
                    product_data['sku'] = product_data['pn']
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract actual price - Toyota: from JSON-LD offers.price
            if json_ld_script:
                try:
                    json_ld_data = json.loads(json_ld_script.string)
                    if isinstance(json_ld_data, dict) and 'offers' in json_ld_data:
                        offers = json_ld_data['offers']
                        if isinstance(offers, dict) and 'price' in offers:
                            price_val = offers['price']
                            if price_val:
                                product_data['actual_price'] = self.extract_price(str(price_val))
                except:
                    pass
            
            # Fallback: If actual_price not found, use MSRP (if available)
            if not product_data['actual_price'] and product_data['msrp']:
                product_data['actual_price'] = product_data['msrp']
            
            # Extract MSRP - Toyota specific: span with class "ibuCTj" containing "MSRP $"
            msrp_elem = soup.find('span', class_=re.compile(r'ibuCTj', re.I))
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                # Extract MSRP value (format: "MSRP $33.42")
                msrp_match = re.search(r'MSRP\s*\$?\s*([\d,]+\.?\d*)', msrp_text, re.I)
                if msrp_match:
                    product_data['msrp'] = f"${msrp_match.group(1)}"
                else:
                    product_data['msrp'] = self.extract_price(msrp_text)
            
            # Fallback: Look for any span containing "MSRP"
            if not product_data['msrp']:
                msrp_elems = soup.find_all('span', string=re.compile(r'MSRP', re.I))
                for elem in msrp_elems:
                    msrp_text = elem.get_text(strip=True)
                    msrp_match = re.search(r'MSRP\s*\$?\s*([\d,]+\.?\d*)', msrp_text, re.I)
                    if msrp_match:
                        product_data['msrp'] = f"${msrp_match.group(1)}"
                        break
            
            # Extract image URL - Toyota specific: img with src containing "epc-images.toyota.com"
            img_elem = soup.find('img', src=re.compile(r'epc-images\.toyota\.com', re.I))
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src')
                if img_url:
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"https:{img_url}" if not img_url.startswith('http') else img_url
                    else:
                        product_data['image_url'] = img_url
            
            # Fallback: Try JSON-LD image
            if not product_data['image_url'] and json_ld_script:
                try:
                    json_ld_data = json.loads(json_ld_script.string)
                    if isinstance(json_ld_data, dict) and 'image' in json_ld_data:
                        images = json_ld_data['image']
                        if isinstance(images, list) and len(images) > 0:
                            product_data['image_url'] = images[0]
                        elif isinstance(images, str):
                            product_data['image_url'] = images
                except:
                    pass
            
            # Fallback: Try meta tag
            if not product_data['image_url']:
                img_meta = soup.find('meta', property='og:image')
                if img_meta:
                    img_url = img_meta.get('content', '').strip()
                    if img_url:
                        product_data['image_url'] = img_url
            
            # Extract description - Toyota specific: p with class containing "iKpZPZ" or "sc-iLnaUn"
            desc_elem = soup.find('p', class_=re.compile(r'iKpZPZ|sc-iLnaUn', re.I))
            if desc_elem:
                product_data['description'] = desc_elem.get_text(strip=True)
            
            # Fallback: Look in description section
            if not product_data['description']:
                desc_section = soup.find('div', {'data-molecule': 'product-detail', 'data-part': 'description'})
                if desc_section:
                    desc_paragraphs = desc_section.find_all('p', {'data-atom': 'typography'})
                    desc_texts = []
                    for p in desc_paragraphs:
                        text = p.get_text(strip=True)
                        if text and len(text) > 10:
                            desc_texts.append(text)
                    if desc_texts:
                        product_data['description'] = ' '.join(desc_texts)
            
            # Fallback: Try JSON-LD description
            if not product_data['description'] and json_ld_script:
                try:
                    json_ld_data = json.loads(json_ld_script.string)
                    if isinstance(json_ld_data, dict) and 'description' in json_ld_data:
                        desc_text = str(json_ld_data['description'])
                        # Decode HTML entities
                        desc_text = desc_text.replace('&#39;', "'").replace('&amp;', '&')
                        desc_text = re.sub(r'<[^>]+>', '', desc_text)
                        if desc_text and len(desc_text.strip()) > 5:
                            product_data['description'] = desc_text.strip()
                except:
                    pass
            
            # Fallback: Try meta tag
            if not product_data['description']:
                desc_meta = soup.find('meta', itemprop='description')
                if not desc_meta:
                    desc_meta = soup.find('meta', {'name': 'description'})
                if desc_meta:
                    product_data['description'] = desc_meta.get('content', '').strip()
            
            # Extract "Also Known As" - Toyota may not have this field, but we'll try
            also_known_elem = soup.find('li', class_='also_known_as')
            if also_known_elem:
                also_known_value = also_known_elem.find('h2', class_='list-value')
                if not also_known_value:
                    also_known_value = also_known_elem.find('span', class_='list-value')
                if also_known_value:
                    also_known_text = also_known_value.get_text(strip=True)
                    if also_known_text and len(also_known_text) > 3:
                        product_data['also_known_as'] = also_known_text
            
            # Extract "Replaces" - Toyota may not have this field, but we'll try
            replaces_elem = soup.find('li', class_='product-superseded-list')
            if replaces_elem:
                replaces_value = replaces_elem.find('h2', class_='list-value')
                if not replaces_value:
                    replaces_value = replaces_elem.find('span', class_='list-value')
                if replaces_value:
                    replaces_text = replaces_value.get_text(strip=True)
                    if replaces_text:
                        product_data['replaces'] = replaces_text
            
            # Extract fitment data - Toyota: Fitment data may be in dealer selection or fitment section
            # Look for fitment section with "Check to see if this fits" button
            fitment_section = soup.find('div', {'data-molecule': 'product-detail', 'data-part': 'fitment'})
            if fitment_section:
                # Fitment data might be loaded dynamically via JavaScript
                # For now, we'll add empty fitment entry
                pass
            
            # Try to extract from any fitment-related elements
            # Note: Toyota's fitment data appears to be loaded dynamically via dealer selection
            # We'll add a default empty fitment entry
            
            # Method 2: Extract from HTML fitment table
            if not product_data['fitments']:
                fitment_table = soup.find('table', class_='fitment-table')
                if fitment_table:
                    tbody = fitment_table.find('tbody', class_='fitment-table-body')
                    if not tbody:
                        tbody = fitment_table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr', class_='fitment-row')
                        if not rows:
                            rows = tbody.find_all('tr')
                        for row in rows:
                            try:
                                year_cell = row.find('td', class_='fitment-year')
                                make_cell = row.find('td', class_='fitment-make')
                                model_cell = row.find('td', class_='fitment-model')
                                trim_cell = row.find('td', class_='fitment-trim')
                                engine_cell = row.find('td', class_='fitment-engine')
                                
                                # Fallback: Use index-based extraction
                                if not year_cell or not make_cell:
                                    cells = row.find_all('td')
                                    if len(cells) >= 5:
                                        year_cell = cells[0]
                                        make_cell = cells[1]
                                        model_cell = cells[2]
                                        trim_cell = cells[3]
                                        engine_cell = cells[4]
                                
                                year = year_cell.get_text(strip=True) if year_cell else ''
                                make = make_cell.get_text(strip=True) if make_cell else ''
                                model = model_cell.get_text(strip=True) if model_cell else ''
                                trim = trim_cell.get_text(strip=True) if trim_cell else ''
                                engine = engine_cell.get_text(strip=True) if engine_cell else ''
                                
                                # Handle multiple values in trim/engine (comma-separated)
                                trims = [t.strip() for t in trim.split(',')] if trim else ['']
                                engines = [e.strip() for e in engine.split(',')] if engine else ['']
                                
                                # Generate all combinations
                                for trim_val in trims:
                                    for engine_val in engines:
                                        if year or make or model:
                                            product_data['fitments'].append({
                                                'year': year,
                                                'make': make,
                                                'model': model,
                                                'trim': trim_val,
                                                'engine': engine_val
                                            })
                            except:
                                continue
            
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

