"""Scraper for www.mitsubishipartswarehouse.com (Mitsubishi parts) - Auto Parts Prime platform"""
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

class MitsubishiScraper(BaseScraper):
    """Scraper for www.mitsubishipartswarehouse.com - Uses Auto Parts Prime platform"""
    
    def __init__(self):
        super().__init__('mitsubishi', use_selenium=True)
        self.base_url = 'https://www.mitsubishipartswarehouse.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.mitsubishipartswarehouse.com"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            self.logger.info("Browsing wheels accessories...")
            accessory_urls = self._browse_wheels_accessories()
            product_urls.extend(accessory_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            # Product URLs: /oem-parts/mitsubishi-{name}-{part}
            validated_urls = []
            for url in product_urls:
                # RevolutionParts platform: /oem-parts/mitsubishi-wheel-alloy-4250a689
                if '/oem-parts/mitsubishi-' in url:
                    if not any(pattern in url for pattern in ['/accessories/', '/category/', '/search']):
                        validated_urls.append(url)
            
            product_urls = validated_urls
            self.logger.info(f"Final validated product URLs: {len(product_urls)}")
            
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
            max_consecutive_empty = 4  # Stop after 4 consecutive pages with no new products
            
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
        
        # RevolutionParts structure: product links in div.catalog-product.row
        # Links can be: a.title-link or a.product-image-link with href="/oem-parts/mitsubishi-..."
        product_rows = soup.find_all('div', class_='catalog-product')
        
        for row in product_rows:
            # Try multiple selectors for product links
            link = row.find('a', class_='title-link')
            if not link:
                link = row.find('a', class_='product-image-link')
            if not link:
                link = row.find('a', href=re.compile(r'/oem-parts/'))
            
            if link:
                href = link.get('href', '')
                if href:
                    # Remove query parameters but keep the base URL
                    if '?' in href:
                        href = href.split('?')[0]
                    if '#' in href:
                        href = href.split('#')[0]
                    
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    full_url = full_url.rstrip('/')
                    
                    # Only collect individual product pages
                    if '/oem-parts/' in full_url and full_url not in product_urls:
                        product_urls.append(full_url)
                        page_count += 1
        
        # Fallback: Also check for any links with /oem-parts/ pattern
        if page_count == 0:
            all_links = soup.find_all('a', href=re.compile(r'/oem-parts/'))
            for link in all_links:
                href = link.get('href', '')
                if href:
                    if '?' in href:
                        href = href.split('?')[0]
                    if '#' in href:
                        href = href.split('#')[0]
                    
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    full_url = full_url.rstrip('/')
                    
                    if '/oem-parts/' in full_url and full_url not in product_urls:
                        product_urls.append(full_url)
                        page_count += 1
        
        return page_count
    
    def _browse_wheels_accessories(self):
        """Browse wheels accessories page"""
        product_urls = []
        
        try:
            accessory_url = f"{self.base_url}/accessories/mitsubishi-wheels.html"
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(accessory_url, use_selenium=True, wait_time=2)
                if not html:
                    return product_urls
            except Exception as e:
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            product_links = soup.find_all('a', href=re.compile(r'/genuine/mitsubishi-.*~.*\.html'))
            for link in product_links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    if '?' in full_url:
                        full_url = full_url.split('?')[0]
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    full_url = full_url.rstrip('/')
                    if full_url not in product_urls:
                        product_urls.append(full_url)
            
        except Exception as e:
            self.logger.error(f"Error browsing accessories: {str(e)}")
        
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
        """Scrape single product from www.mitsubishipartswarehouse.com"""
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
            # Extract title - Mitsubishi Parts Warehouse: h1.product-title
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
            
            # Extract SKU/Part Number - Mitsubishi Parts Warehouse
            # Priority 1: Extract from JSON data (sku_stripped)
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
            
            # Priority 4: Extract from URL pattern: /oem-parts/mitsubishi-wheel-alloy-{sku_stripped}
            if not product_data['pn']:
                url_match = re.search(r'/oem-parts/mitsubishi-[^/]+-([a-z0-9]+)(?:-|$|\?)', url, re.I)
                if url_match:
                    product_data['pn'] = url_match.group(1).upper()
                    if not product_data['sku']:
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
            
            # Extract price - Mitsubishi Parts Warehouse: strong#product_price or strong.sale-price-value
            price_elem = soup.find('strong', id='product_price')
            if not price_elem:
                price_elem = soup.find('strong', class_='sale-price-value')
            if not price_elem:
                price_elem = soup.find('strong', class_='sale-price-amount')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Extract MSRP - Mitsubishi Parts Warehouse: span#product_price2 or span.list-price-value
            msrp_elem = soup.find('span', id='product_price2')
            if not msrp_elem:
                msrp_elem = soup.find('span', class_='list-price-value')
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
            
            # Extract image - Mitsubishi Parts Warehouse: img.product-main-image
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
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description - Mitsubishi Parts Warehouse: li.description > span.description_body > p
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
            
            # Extract "Also Known As" - Mitsubishi Parts Warehouse: li.also_known_as > h2.list-value
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
            
            # Extract "Replaces" - Mitsubishi Parts Warehouse: li.product-superseded-list > h2.list-value
            replaces_elem = soup.find('li', class_='product-superseded-list')
            if replaces_elem:
                replaces_value = replaces_elem.find('h2', class_='list-value')
                if not replaces_value:
                    replaces_value = replaces_elem.find('span', class_='list-value')
                if replaces_value:
                    replaces_text = replaces_value.get_text(strip=True)
                    if replaces_text:
                        product_data['replaces'] = replaces_text
            
            # Extract fitment data - Mitsubishi Parts Warehouse: Try JSON data first, then HTML table
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

