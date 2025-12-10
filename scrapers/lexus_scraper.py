"""Scraper for lexus.oempartsonline.com (Lexus parts)"""
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

class LexusScraper(BaseScraper):
    """Scraper for lexus.oempartsonline.com"""
    
    def __init__(self):
        super().__init__('lexus', use_selenium=True)
        self.base_url = 'https://lexus.oempartsonline.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from lexus.oempartsonline.com"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            # Product URLs: /oem-parts/lexus-wheel-{sku}
            validated_urls = []
            for url in product_urls:
                # Product URLs: /oem-parts/lexus-wheel-...
                is_product = re.search(r'/oem-parts/lexus-wheel-', url, re.I)
                is_category = re.search(r'/search\?', url, re.I)
                
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
        """Search for wheels using site search with pagination"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Initial search URL
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.title-link[href*='/oem-parts/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products on the first page
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract total number of pages from pagination
            total_pages = 1
            try:
                # Get result count first
                result_count_elem = soup.find('span', class_='result-count')
                result_count = 0
                if result_count_elem:
                    result_text = result_count_elem.get_text(strip=True).replace(',', '')
                    if result_text.isdigit():
                        result_count = int(result_text)
                        self.logger.info(f"Found {result_count} total results")
                
                # Look for pagination links
                pagination_links = soup.find_all('a', class_='pagination-link')
                max_page = 1
                for link in pagination_links:
                    page_attr = link.get('data-page', '')
                    if page_attr and page_attr.isdigit():
                        page_num = int(page_attr)
                        if page_num > max_page:
                            max_page = page_num
                    
                    # Also check link text for page numbers
                    link_text = link.get_text(strip=True)
                    if link_text.isdigit():
                        page_num = int(link_text)
                        if page_num > max_page:
                            max_page = page_num
                
                # Check for "6–10" type range links which indicate more pages
                for link in pagination_links:
                    link_text = link.get_text(strip=True)
                    if '–' in link_text or '-' in link_text:
                        # Extract the higher number from range like "6–10"
                        range_match = re.search(r'(\d+)[–-](\d+)', link_text)
                        if range_match:
                            end_page = int(range_match.group(2))
                            if end_page > max_page:
                                max_page = end_page
                
                # Estimate total pages based on result count if we have it
                # RevolutionParts typically shows ~20 products per page
                if result_count > 0:
                    estimated_pages = (result_count // 20) + 1
                    if estimated_pages > max_page:
                        max_page = estimated_pages
                        self.logger.info(f"Estimated {max_page} pages based on {result_count} results")
                
                total_pages = max_page
                self.logger.info(f"Found pagination: {total_pages} total pages")
            except Exception as e:
                self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
            
            # Extract products from all pages
            for page_num in range(1, total_pages + 1):
                try:
                    if page_num > 1:
                        # Navigate to the next page
                        pag_url = f"{search_url}&page={page_num}"
                        self.logger.info(f"Loading page {page_num}/{total_pages}: {pag_url}")
                        
                        try:
                            self.page_load_timeout = 60
                            self.driver.set_page_load_timeout(60)
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                self.logger.warning(f"Page {page_num} content too short, skipping")
                                continue
                            
                            # Wait for products to load
                            try:
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.title-link[href*='/oem-parts/']"))
                                )
                            except:
                                pass
                            
                            # Scroll to load all products on this page
                            self._scroll_to_load_content()
                            pag_html = self.driver.page_source
                            soup = BeautifulSoup(pag_html, 'lxml')
                        except Exception as e:
                            self.logger.warning(f"Error loading page {page_num}: {str(e)}")
                            continue
                        finally:
                            try:
                                self.page_load_timeout = original_timeout
                                self.driver.set_page_load_timeout(original_timeout)
                            except:
                                pass
                    
                    # Extract product links from current page
                    self.logger.info(f"Extracting products from page {page_num}/{total_pages}...")
                    
                    # Product links: a.title-link with href="/oem-parts/lexus-wheel-..."
                    product_links = soup.find_all('a', class_='title-link', href=re.compile(r'/oem-parts/lexus-wheel-'))
                    
                    page_count = 0
                    for link in product_links:
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
                            if '/oem-parts/lexus-wheel-' in full_url:
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                                    page_count += 1
                    
                    self.logger.info(f"Page {page_num}/{total_pages}: Found {len(product_links)} product links, {page_count} new unique URLs (Total: {len(product_urls)})")
                    
                    # If we didn't find any new products, we might have reached the end
                    if page_count == 0 and page_num > 1:
                        self.logger.info(f"No new products found on page {page_num}, stopping pagination")
                        break
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.info(f"Finished processing all pages. Total unique product URLs found: {len(product_urls)}")
            
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
        """Scrape single product from lexus.oempartsonline.com"""
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
            # Extract title - RevolutionParts structure: h1 or h2.product-title
            title_elem = soup.find('h1')
            if not title_elem:
                title_elem = soup.find('h2', class_='product-title')
            if not title_elem:
                title_elem = soup.find('strong', class_='product-title')
            
            if title_elem:
                # Get text from h2 inside if present
                h2_elem = title_elem.find('h2')
                if h2_elem:
                    product_data['title'] = h2_elem.get_text(strip=True)
                else:
                    product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # Remove site name if present
                    if '|' in title_text:
                        title_text = title_text.split('|')[0].strip()
                    product_data['title'] = title_text
            
            if not product_data['title'] or len(product_data['title']) < 3:
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - RevolutionParts: Get stripped version (without hyphens)
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
            
            # Priority 2: Extract from add-to-cart button data-sku-stripped attribute
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
            
            # Priority 3: Extract from URL pattern: /oem-parts/lexus-wheel-{sku_stripped}
            if not product_data['pn']:
                url_match = re.search(r'/oem-parts/lexus-wheel-([a-z0-9]+)(?:-|$|\?)', url, re.I)
                if url_match:
                    product_data['pn'] = url_match.group(1).upper()
                    product_data['sku'] = product_data['pn']  # Use stripped as SKU if no formatted version
            
            # Priority 4: Extract from displayed SKU and strip hyphens
            if not product_data['pn']:
                sku_elem = soup.find('h2', class_='sku-display')
                if not sku_elem:
                    sku_elem = soup.find('span', class_='sku-display')
                
                if sku_elem:
                    sku_text = sku_elem.get_text(strip=True)
                    if sku_text:
                        product_data['sku'] = sku_text.upper()
                        # Strip hyphens for PN
                        product_data['pn'] = re.sub(r'[-\s]', '', sku_text).upper()
            
            # Priority 5: Try from catalog-product-id > a
            if not product_data['pn']:
                pn_elem = soup.find('div', class_='catalog-product-id')
                if pn_elem:
                    pn_link = pn_elem.find('a')
                    if pn_link:
                        part_number = pn_link.get_text(strip=True)
                        if part_number:
                            product_data['sku'] = part_number.upper()
                            # Strip hyphens for PN
                            product_data['pn'] = re.sub(r'[-\s]', '', part_number).upper()
            
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
            
            # Extract price - RevolutionParts: strong#product_price or strong.sale-price-value
            price_elem = soup.find('strong', id='product_price')
            if not price_elem:
                price_elem = soup.find('strong', class_='sale-price-value')
            if not price_elem:
                price_elem = soup.find('strong', class_='sale-price-amount')
            if not price_elem:
                price_elem = soup.find('div', class_='sale-price')
            
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Extract MSRP - RevolutionParts: Try list-price class or data-msrp attribute
            msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Also try from add-to-cart button data attribute
            if not product_data['msrp']:
                add_to_cart_btn = soup.find('button', class_='add-to-cart')
                if add_to_cart_btn:
                    msrp_attr = add_to_cart_btn.get('data-msrp', '')
                    if msrp_attr:
                        product_data['msrp'] = self.extract_price(msrp_attr)
            
            # Also try from JSON data in script tag
            if not product_data['msrp']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script:
                    try:
                        json_data = json.loads(product_data_script.string)
                        if 'msrp' in json_data and json_data['msrp']:
                            product_data['msrp'] = self.extract_price(str(json_data['msrp']))
                    except json.JSONDecodeError:
                        pass
            
            # Extract image - RevolutionParts: Try main product image first
            # Look for main product image link
            main_image_link = soup.find('a', class_='product-main-image-link')
            if main_image_link:
                img_url = main_image_link.get('href', '')
                if img_url:
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"{self.base_url}{img_url}"
                    else:
                        product_data['image_url'] = img_url
            
            # Fallback: Try img tag with product-main-image class
            if not product_data['image_url']:
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
            
            # Fallback: Try any image with cdn-illustrations or cdn-product-images
            if not product_data['image_url']:
                img_elem = soup.find('img', src=re.compile(r'cdn-illustrations|cdn-product-images', re.I))
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description - RevolutionParts: Try product-details-module first
            desc_elem = soup.find('li', class_='description', itemprop='description')
            if desc_elem:
                desc_body = desc_elem.find('span', class_='description_body')
                if desc_body:
                    # Get text from all p tags or direct text
                    desc_paragraphs = desc_body.find_all('p')
                    if desc_paragraphs:
                        desc_text = ' '.join([p.get_text(strip=True) for p in desc_paragraphs])
                    else:
                        desc_text = desc_body.get_text(strip=True)
                    if desc_text and len(desc_text) > 5:
                        product_data['description'] = desc_text
            
            # Fallback: Try catalog-also-known-as with "Description:"
            if not product_data['description']:
                desc_rows = soup.find_all('div', class_='catalog-also-known-as')
                for desc_row in desc_rows:
                    strong_elem = desc_row.find('strong')
                    if strong_elem and 'Description' in strong_elem.get_text(strip=True):
                        desc_content = desc_row.find('span', class_='info-row-content')
                        if desc_content:
                            desc_text = desc_content.get_text(strip=True)
                            # Remove "..." button text if present
                            desc_text = re.sub(r'\s*\(\.\.\.\)\s*$', '', desc_text)
                            if desc_text and len(desc_text) > 10:
                                product_data['description'] = desc_text
                                break
            
            # Extract "Also Known As" / Other Names - Try product-details-module first
            also_known_elem = soup.find('li', class_='also_known_as')
            if also_known_elem:
                list_value = also_known_elem.find('h2', class_='list-value')
                if not list_value:
                    list_value = also_known_elem.find('span', class_='list-value')
                if list_value:
                    other_names = list_value.get_text(strip=True)
                    if other_names:
                        product_data['also_known_as'] = other_names
            
            # Fallback: Try catalog-also-known-as with "Other Names:"
            if not product_data['also_known_as']:
                desc_rows = soup.find_all('div', class_='catalog-also-known-as')
                for row in desc_rows:
                    strong_elem = row.find('strong')
                    if strong_elem and 'Other Names' in strong_elem.get_text(strip=True):
                        other_names_content = row.find('span', class_='info-row-content')
                        if other_names_content:
                            other_names = other_names_content.get_text(strip=True)
                            # Remove "..." button text if present
                            other_names = re.sub(r'\s*\(\.\.\.\)\s*$', '', other_names)
                            if other_names:
                                product_data['also_known_as'] = other_names
                                break
            
            # Extract fitment - RevolutionParts: Try JSON data first, then HTML table
            # Method 1: Extract from JSON data in script tag (more structured)
            fitment_json_script = soup.find('script', id='product_data', type='application/json')
            if fitment_json_script:
                try:
                    json_data = json.loads(fitment_json_script.string)
                    if 'fitment' in json_data and isinstance(json_data['fitment'], list):
                        for fitment_item in json_data['fitment']:
                            year = str(fitment_item.get('year', ''))
                            make = fitment_item.get('make', '')
                            model = fitment_item.get('model', '')
                            trims = fitment_item.get('trims', [])
                            engines = fitment_item.get('engines', [])
                            
                            # If no trims or engines, create one record
                            if not trims and not engines:
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': make,
                                    'model': model,
                                    'trim': '',
                                    'engine': ''
                                })
                            else:
                                # Generate all combinations: year × make × model × trim × engine
                                trim_list = trims if trims else ['']
                                engine_list = engines if engines else ['']
                                
                                for trim in trim_list:
                                    for engine in engine_list:
                                        product_data['fitments'].append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': trim if isinstance(trim, str) else str(trim),
                                            'engine': engine if isinstance(engine, str) else str(engine)
                                        })
                except json.JSONDecodeError as e:
                    self.logger.debug(f"Failed to parse JSON fitment data: {e}")
            
            # Method 2: Extract from HTML fitment table (fallback or if JSON didn't have fitment)
            if not product_data['fitments']:
                fitment_table = soup.find('table', class_='fitment-table')
                if fitment_table:
                    tbody = fitment_table.find('tbody', class_='fitment-table-body')
                    if tbody:
                        rows = tbody.find_all('tr', class_='fitment-row')
                        for row in rows:
                            # Skip hidden rows
                            if 'fitment-hidden' in row.get('class', []):
                                continue
                            
                            year_cell = row.find('td', class_='fitment-year')
                            make_cell = row.find('td', class_='fitment-make')
                            model_cell = row.find('td', class_='fitment-model')
                            trim_cell = row.find('td', class_='fitment-trim')
                            engine_cell = row.find('td', class_='fitment-engine')
                            
                            if year_cell and make_cell and model_cell:
                                year = year_cell.get_text(strip=True)
                                make = make_cell.get_text(strip=True)
                                model = model_cell.get_text(strip=True)
                                
                                # Extract trim values (comma-separated)
                                trim_text = trim_cell.get_text(strip=True) if trim_cell else ''
                                trims = [t.strip() for t in trim_text.split(',') if t.strip()] if trim_text else ['']
                                
                                # Extract engine values (comma-separated)
                                engine_text = engine_cell.get_text(strip=True) if engine_cell else ''
                                engines = [e.strip() for e in engine_text.split(',') if e.strip()] if engine_text else ['']
                                
                                # Generate all combinations: year × make × model × trim × engine
                                for trim in trims:
                                    for engine in engines:
                                        product_data['fitments'].append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': trim,
                                            'engine': engine
                                        })
            
            # Fallback: Try summary fitment if table not found
            if not product_data['fitments']:
                fitment_summary = soup.find('div', class_='catalog-product-fitment-summary')
                if fitment_summary:
                    fitment_list = fitment_summary.find('ul', class_='catalog-fitment-summary')
                    if fitment_list:
                        make_items = fitment_list.find_all('li', class_='fitment-makes')
                        for make_item in make_items:
                            make_elem = make_item.find('strong', class_='fitment-make')
                            make_text = make_elem.get_text(strip=True) if make_elem else ''
                            make_text = re.sub(r':\s*$', '', make_text).strip()  # Remove trailing colon
                            
                            model_list = make_item.find('ul', class_='fitment-models')
                            if model_list:
                                model_items = model_list.find_all('li', class_='fitment-model')
                                for model_item in model_items:
                                    model_text = model_item.get_text(strip=True)
                                    if model_text and not model_text.startswith('('):  # Skip hidden items
                                        product_data['fitments'].append({
                                            'year': '',
                                            'make': make_text,
                                            'model': model_text,
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

