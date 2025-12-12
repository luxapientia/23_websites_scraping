"""Scraper for www.jimellismazdaparts.com (Mazda parts) - SimplePart platform"""
from scrapers.base_scraper_with_extension import BaseScraperWithExtension
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

class MazdaScraper(BaseScraperWithExtension):
    """Scraper for www.jimellismazdaparts.com - Uses SimplePart platform"""
    
    def __init__(self):
        super().__init__('mazda', use_selenium=True)
        self.base_url = 'https://www.jimellismazdaparts.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.jimellismazdaparts.com"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            self.logger.info("Browsing Tire and Wheel category...")
            category_urls = self._browse_tire_wheel_category()
            product_urls.extend(category_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            validated_urls = []
            for url in product_urls:
                # Product URLs: /p/Mazda__/Product-Name/ID/PartNumber.html
                # Category URLs: /Mazda__/Category.html or /productSearch.aspx
                is_product = re.search(r'/p/Mazda__/[^/]+/\d+/[^/]+\.html', url, re.I)
                is_category = re.search(r'/Mazda__/[^/]+\.html$|/productSearch\.aspx', url, re.I)
                
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
            search_url = f"{self.base_url}/productSearch.aspx?ukey_make=995&modelYear=0&ukey_model=0&ukey_trimLevel=0&ukey_driveline=0&ukey_Category=0&numResults=250&sortOrder=Relevance&ukey_tag=0&isOnSale=0&isAccessory=0&isPerformance=0&showAllModels=1&searchTerm=wheel"
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
                selenium_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/p/Mazda__/']")
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
                            if '/p/Mazda__/' in full_url and full_url.endswith('.html'):
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
            # Pattern 1: /p/Mazda__/Product-Name/ID/PartNumber.html
            product_links = soup.find_all('a', href=re.compile(r'/p/Mazda__/', re.I))
            
            # Pattern 2: Try looking in product containers/rows
            if not product_links:
                product_containers = soup.find_all(['div', 'li', 'tr'], class_=re.compile(r'product|item|part|row', re.I))
                for container in product_containers:
                    container_links = container.find_all('a', href=re.compile(r'/p/Mazda__/', re.I))
                    product_links.extend(container_links)
            
            # Pattern 3: Look for any link with /p/ pattern (case-insensitive)
            if not product_links:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    if href and '/p/' in href.lower() and 'mazda' in href.lower():
                        product_links.append(link)
            
            # Pattern 4: Try looking for links ending with .html in product sections
            if not product_links:
                product_sections = soup.find_all(['div', 'section', 'table', 'tbody'], class_=re.compile(r'product|result|search|item|row', re.I))
                for section in product_sections:
                    section_links = section.find_all('a', href=re.compile(r'\.html', re.I))
                    for link in section_links:
                        href = link.get('href', '')
                        if href and ('/p/' in href.lower() or 'mazda' in href.lower()):
                            product_links.append(link)
            
            # Pattern 5: Use JavaScript to find all links (most comprehensive)
            if not product_links:
                try:
                    js_links = self.driver.execute_script("""
                        var links = [];
                        var allLinks = document.querySelectorAll('a[href]');
                        for (var i = 0; i < allLinks.length; i++) {
                            var href = allLinks[i].href || allLinks[i].getAttribute('href');
                            if (href && href.toLowerCase().indexOf('/p/mazda__/') !== -1 && href.toLowerCase().endsWith('.html')) {
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
                            full_url = full_url.rstrip('/')
                            if '/p/Mazda__/' in full_url and full_url.endswith('.html'):
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
                # SimplePart product URLs: /p/Mazda__/Product-Name/ID/PartNumber.html
                if '/p/Mazda__/' in full_url and full_url.endswith('.html'):
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
    
    def _browse_tire_wheel_category(self):
        """Browse Tire and Wheel category with pagination"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            category_url = f"{self.base_url}/Mazda__/Tire-and-Wheel.html"
            self.logger.info(f"Browsing category: {category_url}")
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(category_url, use_selenium=True, wait_time=2)
                if not html:
                    return product_urls
            except Exception as e:
                self.logger.error(f"Error loading category page: {str(e)}")
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Mazda__/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract total pages if pagination exists
            total_pages = 1
            try:
                pagination_links = soup.find_all('a', href=re.compile(r'Tire-and-Wheel.*page|Page', re.I))
                max_page = 1
                
                for link in pagination_links:
                    href = link.get('href', '')
                    page_match = re.search(r'[Pp]age[=:]?(\d+)', href + ' ' + link.get_text(strip=True), re.I)
                    if page_match:
                        page_num = int(page_match.group(1))
                        if page_num > max_page:
                            max_page = page_num
                    
                    link_text = link.get_text(strip=True)
                    if link_text.isdigit():
                        page_num = int(link_text)
                        if page_num > max_page:
                            max_page = page_num
                
                total_pages = max_page
                if total_pages > 1:
                    self.logger.info(f"Found pagination: {total_pages} total pages for category")
            except Exception as e:
                self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
            
            # Extract products from all pages
            for page_num in range(1, total_pages + 1):
                try:
                    if page_num > 1:
                        pag_url = f"{category_url}?page={page_num}"
                        self.logger.info(f"Loading category page {page_num}/{total_pages}: {pag_url}")
                        
                        try:
                            self.page_load_timeout = 60
                            self.driver.set_page_load_timeout(60)
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                self.logger.warning(f"Category page {page_num} content too short, skipping")
                                continue
                            
                            try:
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Mazda__/']"))
                                )
                            except:
                                pass
                            
                            self._scroll_to_load_content()
                            pag_html = self.driver.page_source
                            soup = BeautifulSoup(pag_html, 'lxml')
                        except Exception as e:
                            self.logger.warning(f"Error loading category page {page_num}: {str(e)}")
                            continue
                        finally:
                            try:
                                self.page_load_timeout = original_timeout
                                self.driver.set_page_load_timeout(original_timeout)
                            except:
                                pass
                    
                    # Extract product links from current page
                    product_links = soup.find_all('a', href=re.compile(r'/p/Mazda__/'))
                    page_count = 0
                    
                    for link in product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            
                            # Only collect individual product pages
                            if '/p/Mazda__/' in full_url and full_url.endswith('.html'):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                                    page_count += 1
                    
                    self.logger.info(f"Category page {page_num}/{total_pages}: Found {len(product_links)} product links, {page_count} new unique URLs (Total: {len(product_urls)})")
                    
                    if page_count == 0 and page_num > 1:
                        self.logger.info(f"No new products found on category page {page_num}, stopping pagination")
                        break
                    
                except Exception as e:
                    self.logger.error(f"Error processing category page {page_num}: {str(e)}")
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.info(f"Finished browsing category. Total unique URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error browsing category: {str(e)}")
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
        """Scrape single product from www.jimellismazdaparts.com"""
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
            # Extract title - SimplePart structure: h1 or h1 > span
            title_elem = soup.find('h1')
            if title_elem:
                span_elem = title_elem.find('span')
                if span_elem:
                    product_data['title'] = span_elem.get_text(strip=True)
                else:
                    product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                h2_elem = soup.find('h2', class_='subh1')
                if h2_elem:
                    product_data['title'] = h2_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_tag = soup.find('title')
                if title_tag:
                    product_data['title'] = self.safe_find_text(soup, title_tag)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - Pattern from URL: /p/Mazda__/Product-Name/ID/PartNumber.html
            url_match = re.search(r'/p/Mazda__/[^/]+/\d+/([^/]+)\.html', url)
            if url_match:
                product_data['sku'] = url_match.group(1)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Also try page
            if not product_data['sku']:
                pn_elem = soup.find('p', class_='mt-sm')
                if pn_elem:
                    raw_text = pn_elem.get_text(strip=True)
                    part_number = re.sub(r'^part\s*number\s*:?\s*', '', raw_text, flags=re.IGNORECASE)
                    if part_number:
                        product_data['sku'] = part_number
                        product_data['pn'] = self.clean_sku(part_number)
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract price - SimplePart: p#part-price-right > span.bold.text-lg
            price_container = soup.find('p', id='part-price-right')
            if price_container:
                price_spans = price_container.find_all('span')
                for span in price_spans:
                    span_classes = span.get('class', [])
                    if isinstance(span_classes, str):
                        span_classes = [span_classes]
                    span_classes_str = ' '.join(span_classes).lower()
                    if 'bold' in span_classes_str and 'text-lg' in span_classes_str:
                        price_text = span.get_text(strip=True)
                        if '€' in price_text or 'EUR' in price_text.upper():
                            price_value = self.extract_price(price_text)
                            if price_value:
                                product_data['actual_price'] = self.convert_currency(price_value, 'EUR', 'USD')
                        else:
                            product_data['actual_price'] = self.extract_price(price_text)
                        break
            
            # Extract MSRP - SimplePart: Look for list price or MSRP
            msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if msrp_elem:
                msrp_text = self.safe_find_text(soup, msrp_elem)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Fallback: Look for text containing "MSRP" or "List Price"
            if not product_data['msrp']:
                msrp_text_elem = soup.find(string=re.compile(r'MSRP|List\s+Price', re.I))
                if msrp_text_elem:
                    parent = msrp_text_elem.find_parent()
                    if parent:
                        msrp_text = parent.get_text(strip=True)
                        # Extract price from text like "MSRP: $123.45"
                        price_match = re.search(r'[\$]?([\d,]+\.?\d*)', msrp_text)
                        if price_match:
                            product_data['msrp'] = self.extract_price(price_match.group(0))
            
            # Fallback: Look in price container for list price
            if not product_data['msrp']:
                price_container = soup.find('p', id='part-price-right')
                if price_container:
                    # Look for spans with list price indicators
                    all_spans = price_container.find_all('span')
                    for span in all_spans:
                        span_text = span.get_text(strip=True)
                        span_classes = span.get('class', [])
                        if isinstance(span_classes, str):
                            span_classes = [span_classes]
                        span_classes_str = ' '.join(span_classes).lower()
                        
                        # Check if this span indicates list price (not sale price)
                        if 'list' in span_classes_str or 'msrp' in span_classes_str or 'original' in span_classes_str:
                            if 'bold' not in span_classes_str or 'text-lg' not in span_classes_str:
                                # This might be the list price (not the bold sale price)
                                product_data['msrp'] = self.extract_price(span_text)
                                break
            
            # Extract image - SimplePart: #part-image-left a > img.img-responsive
            part_image_left = soup.find('div', id='part-image-left')
            if part_image_left:
                img_link = part_image_left.find('a')
                if img_link:
                    # Try href first (full-size image)
                    img_href = img_link.get('href', '')
                    if img_href:
                        product_data['image_url'] = f"https:{img_href}" if img_href.startswith('//') else (f"{self.base_url}{img_href}" if img_href.startswith('/') else img_href)
                    
                    # Fallback to img tag
                    if not product_data['image_url']:
                        img_elem = img_link.find('img', class_='img-responsive')
                        if not img_elem:
                            img_elem = img_link.find('img')
                        if img_elem:
                            img_url = img_elem.get('src') or img_elem.get('data-src')
                            if img_url:
                                product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else (f"{self.base_url}{img_url}" if img_url.startswith('/') else img_url)
            
            # Fallback: Try any img tag with product image indicators
            if not product_data['image_url']:
                img_elem = soup.find('img', src=re.compile(r'part|product|wheel', re.I))
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else (f"{self.base_url}{img_url}" if img_url.startswith('/') else img_url)
            
            # Extract description - SimplePart: p > strong.custom-blacktext
            desc_strongs = soup.find_all('strong', class_='custom-blacktext')
            for desc_strong in desc_strongs:
                strong_text = desc_strong.get_text(strip=True)
                if 'Product Description' in strong_text:
                    desc_p = desc_strong.find_parent('p')
                    if desc_p:
                        desc_text = desc_p.get_text(strip=True, separator=' ')
                        desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                        desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                        if desc_text and len(desc_text) > 10:
                            product_data['description'] = desc_text
                            break
            
            # Extract "Also Known As" - SimplePart: Look for alternative names
            also_known_strongs = soup.find_all('strong', class_='custom-blacktext')
            for strong_elem in also_known_strongs:
                strong_text = strong_elem.get_text(strip=True)
                if any(keyword in strong_text.lower() for keyword in ['also known as', 'other names', 'alternate', 'aka']):
                    parent_p = strong_elem.find_parent('p')
                    if parent_p:
                        also_known_text = parent_p.get_text(strip=True, separator=' ')
                        also_known_text = re.sub(r'^(Also\s+Known\s+As|Other\s+Names|Alternate|AKA)\s*:?\s*', '', also_known_text, flags=re.IGNORECASE)
                        also_known_text = re.sub(r'\s+', ' ', also_known_text).strip()
                        if also_known_text and len(also_known_text) > 3:
                            product_data['also_known_as'] = also_known_text
                            break
            
            # Extract fitment - SimplePart: div#fitment table
            fitment_div = soup.find('div', id='fitment')
            if fitment_div:
                fitment_table = fitment_div.find('table')
                if fitment_table:
                    rows = fitment_table.find_all('tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 1:
                            first_cell = cells[0]
                            cell_text = first_cell.get_text(strip=True)
                            
                            # Parse: "Model (Year-Year) [Engine]" or "Model (Year) [Engine]"
                            # Extract engine (comma-separated values possible)
                            engine_match = re.search(r'\[([^\]]+)\]', cell_text)
                            engine_text = engine_match.group(1).strip() if engine_match else ''
                            # Split comma-separated engines
                            engines = [e.strip() for e in engine_text.split(',') if e.strip()] if engine_text else ['']
                            
                            model_text = re.sub(r'\s*\[[^\]]+\]', '', cell_text).strip()
                            
                            # Extract year range or single year
                            year_range_match = re.search(r'\((\d{4})\s*-\s*(\d{4})\)', model_text)
                            start_year = None
                            end_year = None
                            
                            if year_range_match:
                                start_year = int(year_range_match.group(1))
                                end_year = int(year_range_match.group(2))
                            else:
                                # Try single year: "Model (Year)"
                                single_year_match = re.search(r'\((\d{4})\)', model_text)
                                if single_year_match:
                                    start_year = int(single_year_match.group(1))
                                    end_year = int(single_year_match.group(1))
                            
                            # Remove year info from model text
                            model_clean = re.sub(r'\s*\(\d{4}\s*-\s*\d{4}\)', '', model_text).strip()
                            model_clean = re.sub(r'\s*\(\d{4}\)', '', model_clean).strip()
                            
                            make = 'Mazda'  # Default make
                            model = model_clean
                            
                            # Generate all combinations: year × engine
                            if start_year is not None and end_year is not None:
                                years = [str(y) for y in range(start_year, end_year + 1)]
                            elif start_year is not None:
                                years = [str(start_year)]
                            else:
                                years = ['']
                            
                            for year_str in years:
                                for engine_val in engines:
                                    product_data['fitments'].append({
                                        'year': year_str,
                                        'make': make,
                                        'model': model,
                                        'trim': '',
                                        'engine': engine_val
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

