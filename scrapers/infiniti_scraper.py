"""Scraper for www.infinitipartsdeal.com (Infiniti parts)"""
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

class InfinitiScraper(BaseScraper):
    """Scraper for www.infinitipartsdeal.com"""
    
    def __init__(self):
        super().__init__('infiniti', use_selenium=True)
        self.base_url = 'https://www.infinitipartsdeal.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.infinitipartsdeal.com"""
        product_urls = []
        
        try:
            self.logger.info("Discovering wheel products from all category pages...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            product_urls = list(set(product_urls))
            self.logger.info(f"Found {len(product_urls)} unique wheel product URLs from all category pages")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """
        Visit all 7 wheel category pages one by one and extract all product URLs
        Similar to Acura scraper's method - visits multiple category URLs sequentially
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # All 7 category URLs to visit one by one
            category_urls = [
                f"{self.base_url}/oem-infiniti-spare_wheel.html",
                f"{self.base_url}/oem-infiniti-wheel_cover.html",
                f"{self.base_url}/accessories/infiniti-center_cap.html",
                f"{self.base_url}/accessories/infiniti-17_inch_wheel.html",
                f"{self.base_url}/accessories/infiniti-18_inch_wheel.html",
                f"{self.base_url}/accessories/infiniti-19_inch_wheel.html",
                f"{self.base_url}/accessories/infiniti-20_inch_wheel.html",
            ]
            
            self.logger.info(f"Starting to visit {len(category_urls)} category pages one by one...")
            
            # Visit each category URL one by one and extract products
            for idx, category_url in enumerate(category_urls, 1):
                try:
                    self.logger.info(f"[{idx}/{len(category_urls)}] Visiting category page: {category_url}")
                    category_products = self._extract_products_from_category(category_url, product_urls)
                    self.logger.info(f"[{idx}/{len(category_urls)}] Category completed: Found {len(category_products)} new products (Total so far: {len(product_urls)})")
                    
                    # Delay between categories
                    if idx < len(category_urls):
                        time.sleep(random.uniform(1, 2))
                except Exception as e:
                    self.logger.error(f"Error processing category {idx}/{len(category_urls)} ({category_url}): {str(e)}")
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.info(f"All {len(category_urls)} category pages processed. Total unique product URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _extract_products_from_category(self, category_url, existing_urls):
        """
        Extract all product URLs from a single category page, handling pagination
        Returns list of new product URLs found (not in existing_urls)
        """
        new_urls = []
        
        try:
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(category_url, use_selenium=True, wait_time=2)
                if not html:
                    self.logger.warning(f"Failed to fetch category page: {category_url}")
                    return new_urls
            except Exception as e:
                self.logger.warning(f"Error loading category page {category_url}: {str(e)}")
                return new_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links to appear
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/parts/infiniti-']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products on the first page (if lazy loading)
            self._scroll_to_load_content()
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract individual product links from the first page
            self.logger.info(f"Extracting products from {category_url} page 1...")
            
            # Try multiple patterns to find ALL product links
            # Pattern 1: Links matching /parts/infiniti-*.html (with or without ~)
            all_product_links = soup.find_all('a', href=re.compile(r'/parts/infiniti-[^/]+\.html', re.I))
            
            # Pattern 2: Look for links in product layout sections (more flexible pattern)
            product_sections = soup.find_all(['div', 'li'], class_=re.compile(r'part-desc-layout|pd-ll|product|item', re.I))
            for section in product_sections:
                section_links = section.find_all('a', href=re.compile(r'/parts/infiniti-[^/]+\.html', re.I))
                for link in section_links:
                    if link not in all_product_links:
                        all_product_links.append(link)
            
            # Pattern 3: Look for any link containing /parts/infiniti- and ending with .html
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                if href and re.search(r'/parts/infiniti-[^/]+\.html', href, re.I):
                    if link not in all_product_links:
                        all_product_links.append(link)
            
            # Pattern 4: Look for links with part number pattern in href (fallback)
            part_number_links = soup.find_all('a', href=re.compile(r'[A-Z0-9]{5,}-[A-Z0-9]{1,}[A-Z0-9]{1,}', re.I))
            for link in part_number_links:
                href = link.get('href', '')
                if href and '/parts/infiniti-' in href.lower() and href.endswith('.html'):
                    if link not in all_product_links:
                        all_product_links.append(link)
            
            self.logger.info(f"Found {len(all_product_links)} product links on page 1")
            first_page_count = 0
            
            for link in all_product_links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    
                    # Remove fragment and query params
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    if '?' in full_url:
                        full_url = full_url.split('?')[0]
                    
                    full_url = full_url.rstrip('/')
                    
                    # Only collect individual product pages (pattern: /parts/infiniti-*.html)
                    # Accept URLs with or without ~ character
                    if '/parts/infiniti-' in full_url.lower() and full_url.endswith('.html'):
                        # Filter out category/listing pages - only individual products
                        # Individual products should have part number or product name in URL
                        if re.search(r'/parts/infiniti-[^/]+\.html$', full_url, re.I):
                            if full_url not in existing_urls and full_url not in new_urls:
                                new_urls.append(full_url)
                                existing_urls.append(full_url)
                                first_page_count += 1
            
            self.logger.info(f"Page 1: Collected {first_page_count} new unique product URLs")
            
            # Handle pagination on category pages
            # Extract total page count from the page (e.g., "Page 1 of 14")
            total_pages = 1
            try:
                # Look for "Page X of Y" pattern
                page_info = soup.find(string=re.compile(r'Page\s+\d+\s+of\s+\d+', re.I))
                if page_info:
                    page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', str(page_info), re.I)
                    if page_match:
                        total_pages = int(page_match.group(1))
                        self.logger.info(f"Found pagination: {total_pages} total pages")
                
                # Alternative: Look for "X-Y of Z Results" pattern
                if total_pages == 1:
                    results_info = soup.find(string=re.compile(r'\d+-\d+\s+of\s+\d+\s+Results', re.I))
                    if results_info:
                        results_match = re.search(r'\d+-\d+\s+of\s+(\d+)', str(results_info), re.I)
                        if results_match:
                            total_results = int(results_match.group(1))
                            # Estimate pages (usually ~20 products per page)
                            estimated_pages = max(1, (total_results + 19) // 20)
                            total_pages = estimated_pages
                            self.logger.info(f"Found {total_results} total results, estimating {total_pages} pages")
                
                # Alternative: Count pagination number links
                if total_pages == 1:
                    pagination_numbers = soup.find_all('a', href=re.compile(r'page=\d+|p=\d+', re.I))
                    if pagination_numbers:
                        max_page_num = 1
                        for pag_link in pagination_numbers:
                            pag_href = pag_link.get('href', '')
                            if pag_href:
                                # Extract page number from URL
                                page_match = re.search(r'[?&](?:page|p)=(\d+)', pag_href, re.I)
                                if page_match:
                                    page_num = int(page_match.group(1))
                                    max_page_num = max(max_page_num, page_num)
                        if max_page_num > 1:
                            total_pages = max_page_num
                            self.logger.info(f"Found pagination links up to page {total_pages}")
            except Exception as e:
                self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
            
            # Extract products from remaining pages (page 1 already extracted above)
            if total_pages > 1:
                self.logger.info(f"Extracting products from pages 2-{total_pages} for {category_url}")
                
                # Try different pagination URL patterns
                pagination_patterns = [
                    f"{category_url}?page={{page_num}}",
                    f"{category_url}?p={{page_num}}",
                    f"{category_url}?pageNumber={{page_num}}",
                ]
                
                consecutive_no_new = 0
                
                for page_num in range(2, total_pages + 1):
                    if consecutive_no_new >= 3:
                        self.logger.info(f"Stopping pagination: {consecutive_no_new} consecutive pages with no new products")
                        break
                    
                    page_found = False
                    
                    for pattern in pagination_patterns:
                        pag_url = pattern.format(page_num=page_num)
                        
                        try:
                            self.logger.info(f"Loading page {page_num}/{total_pages}: {pag_url}")
                            
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                continue
                            
                            pag_soup = BeautifulSoup(pag_html, 'lxml')
                            
                            # Scroll to load all products on this page (if lazy loading)
                            try:
                                last_height = self.driver.execute_script("return document.body.scrollHeight")
                                scroll_attempts = 0
                                while scroll_attempts < 20:
                                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                    time.sleep(1)
                                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                                    if new_height == last_height:
                                        break
                                    last_height = new_height
                                    scroll_attempts += 1
                                
                                # Get updated HTML after scrolling
                                pag_html = self.driver.page_source
                                pag_soup = BeautifulSoup(pag_html, 'lxml')
                            except:
                                pass
                            
                            # Extract product links from this page (more flexible pattern)
                            pag_links = pag_soup.find_all('a', href=re.compile(r'/parts/infiniti-[^/]+\.html', re.I))
                            
                            # Also look for links in product cards/sections
                            product_sections = pag_soup.find_all(['div', 'li'], class_=re.compile(r'part-desc-layout|pd-ll|product|item', re.I))
                            for section in product_sections:
                                section_links = section.find_all('a', href=re.compile(r'/parts/infiniti-[^/]+\.html', re.I))
                                for link in section_links:
                                    if link not in pag_links:
                                        pag_links.append(link)
                            
                            # Also check all links on the page
                            all_page_links = pag_soup.find_all('a', href=True)
                            for link in all_page_links:
                                href = link.get('href', '')
                                if href and re.search(r'/parts/infiniti-[^/]+\.html', href, re.I):
                                    if link not in pag_links:
                                        pag_links.append(link)
                            
                            if len(pag_links) > 0:
                                page_found = True
                                page_product_count = 0
                                
                                self.logger.info(f"Page {page_num}: Found {len(pag_links)} product links")
                                
                                for link in pag_links:
                                    href = link.get('href', '')
                                    if href:
                                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                        
                                        # Remove fragment and query params
                                        if '#' in full_url:
                                            full_url = full_url.split('#')[0]
                                        if '?' in full_url:
                                            full_url = full_url.split('?')[0]
                                        
                                        full_url = full_url.rstrip('/')
                                        
                                        # Only collect individual product pages (with or without ~)
                                        if '/parts/infiniti-' in full_url.lower() and full_url.endswith('.html'):
                                            # Filter out category/listing pages - only individual products
                                            if re.search(r'/parts/infiniti-[^/]+\.html$', full_url, re.I):
                                                if full_url not in existing_urls and full_url not in new_urls:
                                                    new_urls.append(full_url)
                                                    existing_urls.append(full_url)
                                                    page_product_count += 1
                                
                                self.logger.info(f"Page {page_num}: Collected {page_product_count} new unique product URLs")
                                
                                if page_product_count == 0:
                                    consecutive_no_new += 1
                                else:
                                    consecutive_no_new = 0
                                
                                if page_found:
                                    break  # Found working pagination pattern
                                    
                        except Exception as e:
                            self.logger.debug(f"Error loading page {page_num} with pattern {pattern}: {str(e)}")
                            continue
                    
                    if not page_found:
                        self.logger.warning(f"Could not load page {page_num} with any pagination pattern")
                        consecutive_no_new += 1
                    
                    # Delay between pages
                    if page_num < total_pages:
                        time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            self.logger.error(f"Error extracting products from category {category_url}: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return new_urls
    
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
    
    def _expand_year_range(self, year_str):
        """
        Expand year range into individual years
        Example: "1993-1995" -> ["1993", "1994", "1995"]
        Example: "2014-2019" -> ["2014", "2015", "2016", "2017", "2018", "2019"]
        Returns list of year strings
        """
        if not year_str:
            return []
        
        year_str = str(year_str).strip()
        
        # Check for year range pattern: YYYY-YYYY
        year_range_match = re.search(r'(\d{4})\s*-\s*(\d{4})', year_str)
        if year_range_match:
            start_year = int(year_range_match.group(1))
            end_year = int(year_range_match.group(2))
            
            # Validate range (reasonable year range)
            if start_year >= 1900 and end_year <= 2100 and start_year <= end_year:
                years = []
                for year in range(start_year, end_year + 1):
                    years.append(str(year))
                return years
        
        # If not a range, return as single year
        if re.match(r'^\d{4}$', year_str):
            return [year_str]
        
        return [year_str]  # Return original if can't parse
    
    def scrape_product(self, url):
        """Scrape single product from www.infinitipartsdeal.com"""
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
            # 1. Extract title
            title_elem = soup.find('h1', class_='pn-detail-h1') or soup.find('h1')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
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
            
            # 2. Extract SKU/Part Number
            # Priority 1: JSON-LD
            try:
                json_ld_scripts = soup.find_all('script', type='application/ld+json')
                for json_ld in json_ld_scripts:
                    if json_ld and json_ld.string:
                        try:
                            data = json.loads(json_ld.string)
                            if isinstance(data, list):
                                data = data[0]
                            if isinstance(data, dict):
                                if 'sku' in data:
                                    product_data['sku'] = str(data['sku']).strip()
                                if 'mpn' in data and not product_data['sku']:
                                    product_data['sku'] = str(data['mpn']).strip()
                                if product_data['sku']:
                                    break
                        except:
                            continue
            except:
                pass
            
            # Priority 2: Product specifications table
            if not product_data['sku']:
                spec_table = soup.find('table', class_='pn-spec-list')
                if spec_table:
                    rows = spec_table.find_all('tr')
                    for row in rows:
                        tds = row.find_all('td')
                        if len(tds) >= 2:
                            label = tds[0].get_text(strip=True).lower()
                            value = tds[1].get_text(strip=True)
                            if 'manufacturer part number' in label or 'sku' in label:
                                product_data['sku'] = value
                                break
            
            # Priority 3: Extract from sub-description or title
            if not product_data['sku']:
                sub_desc = soup.find('p', class_='pn-detail-sub-desc')
                if sub_desc:
                    text = sub_desc.get_text(strip=True)
                    # Pattern: D03004GA3K or D0300-4GA3K
                    match = re.search(r'([A-Z0-9]{5,}-?[A-Z0-9]{1,}[A-Z0-9]{1,})', text, re.I)
                    if match:
                        product_data['sku'] = match.group(1).upper()
            
            # Priority 4: Extract from URL pattern: /parts/infiniti-*~{part}.html
            if not product_data['sku']:
                url_match = re.search(r'/parts/infiniti-.*~([^~]+)\.html', url, re.I)
                if url_match:
                    product_data['sku'] = url_match.group(1).upper()
            
            if product_data['sku']:
                product_data['sku'] = product_data['sku'].upper()
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # 3. Extract image URL
            # Priority 1: JSON-LD
            try:
                json_ld_scripts = soup.find_all('script', type='application/ld+json')
                for json_ld in json_ld_scripts:
                    if json_ld and json_ld.string:
                        try:
                            data = json.loads(json_ld.string)
                            if isinstance(data, list):
                                data = data[0]
                            if isinstance(data, dict) and 'image' in data:
                                imgs = data['image']
                                if isinstance(imgs, list) and imgs:
                                    product_data['image_url'] = str(imgs[0])
                                elif isinstance(imgs, str):
                                    product_data['image_url'] = imgs
                                if product_data['image_url']:
                                    break
                        except:
                            continue
            except:
                pass
            
            # Priority 2: Main product image
            if not product_data['image_url']:
                img_div = soup.find('div', class_='pn-img-img')
                if img_div:
                    img_elem = img_div.find('img')
                    if img_elem:
                        product_data['image_url'] = img_elem.get('src') or img_elem.get('data-src')
            
            # Priority 3: Fallback to any product image
            if not product_data['image_url']:
                img_elem = soup.find('img', src=re.compile(r'/resources/encry/(actual-picture|part-picture)/'))
                if img_elem:
                    product_data['image_url'] = img_elem.get('src') or img_elem.get('data-src')
            
            # Normalize image URL
            if product_data['image_url']:
                if product_data['image_url'].startswith('//'):
                    product_data['image_url'] = 'https:' + product_data['image_url']
                elif product_data['image_url'].startswith('/'):
                    product_data['image_url'] = self.base_url + product_data['image_url']
            
            # 4. Extract price
            # MSRP
            msrp_span = soup.find('span', class_='price-section-retail')
            if msrp_span:
                inner_span = msrp_span.find('span')
                if inner_span:
                    product_data['msrp'] = self.extract_price(inner_span.get_text(strip=True))
                else:
                    product_data['msrp'] = self.extract_price(msrp_span.get_text(strip=True))
            
            # Actual Price
            price_span = soup.find('span', class_='price-section-price')
            if price_span:
                product_data['actual_price'] = self.extract_price(price_span.get_text(strip=True))
            
            # 5. Extract description
            # Priority 1: Part description from detail list
            detail_list = soup.find('ul', class_='pn-detail-list')
            if detail_list:
                for li in detail_list.find_all('li'):
                    span = li.find('span')
                    if span and 'Part Description' in span.get_text(strip=True):
                        div = li.find('div')
                        if div:
                            product_data['description'] = div.get_text(strip=True)
                        break
            
            # Priority 2: Meta description
            if not product_data['description']:
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc:
                    product_data['description'] = meta_desc.get('content', '').strip()
            
            # Priority 3: Product specifications table
            if not product_data['description']:
                spec_table = soup.find('table', class_='pn-spec-list')
                if spec_table:
                    rows = spec_table.find_all('tr')
                    for row in rows:
                        tds = row.find_all('td')
                        if len(tds) >= 2:
                            label = tds[0].get_text(strip=True).lower()
                            value = tds[1].get_text(strip=True)
                            if 'part description' in label:
                                product_data['description'] = value
                                break
            
            # 6. Extract "Replaced By" / "Also Known As"
            if detail_list:
                for li in detail_list.find_all('li'):
                    span = li.find('span')
                    if span:
                        span_text = span.get_text(strip=True)
                        if 'Replaced By' in span_text:
                            div = li.find('div')
                            if div:
                                product_data['replaces'] = div.get_text(strip=True)
                        elif 'Also Known As' in span_text or 'Lookup Code' in span_text:
                            div = li.find('div')
                            if div:
                                product_data['also_known_as'] = div.get_text(strip=True)
            
            # 7. Extract fitment data from table
            fitment_table = soup.find('div', class_='fit-vehicle-list')
            if fitment_table:
                table = fitment_table.find('table', class_='fit-vehicle-list-table')
                if table:
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_row = thead.find('tr')
                        if header_row:
                            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                    
                    col_map = {}
                    combined_ymm_col = -1
                    
                    for i, h in enumerate(headers):
                        # Check for combined "Year Make Model" column
                        if 'year' in h and 'make' in h and 'model' in h:
                            combined_ymm_col = i
                            continue
                            
                        if 'year' in h: col_map['year'] = i
                        elif 'make' in h: col_map['make'] = i
                        elif 'model' in h: col_map['model'] = i
                        elif 'trim' in h or 'body' in h: col_map['trim'] = i
                        elif 'engine' in h: col_map['engine'] = i
                    
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if not cols: continue
                            
                            row_data = {
                                'year': '', 'make': 'Infiniti', 'model': '', 'trim': '', 'engine': ''
                            }
                            
                            # Handle combined Year Make Model column
                            if combined_ymm_col != -1 and combined_ymm_col < len(cols):
                                first_col_text = cols[combined_ymm_col].get_text(strip=True)
                                # Try to parse "2014-2019 Infiniti Q50" or "2014 Infiniti Q50"
                                year_match = re.search(r'(\d{4})(?:-(\d{4}))?', first_col_text)
                                if year_match:
                                    if year_match.group(2):
                                        row_data['year'] = f"{year_match.group(1)}-{year_match.group(2)}"
                                    else:
                                        row_data['year'] = year_match.group(1)
                                
                                # Extract make and model
                                parts = first_col_text.split()
                                if 'Infiniti' in parts:
                                    make_idx = parts.index('Infiniti')
                                    row_data['make'] = 'Infiniti'
                                    if make_idx + 1 < len(parts):
                                        row_data['model'] = ' '.join(parts[make_idx + 1:])
                            
                            # Handle standard columns
                            for key, idx in col_map.items():
                                if idx < len(cols):
                                    value = cols[idx].get_text(strip=True)
                                    if value:
                                        row_data[key] = value
                            
                            # Handle Trim & Engine column (combined)
                            if 'trim' not in col_map and 'engine' not in col_map:
                                # Look for "Trim & Engine" column
                                for i, h in enumerate(headers):
                                    if 'trim' in h and 'engine' in h:
                                        if i < len(cols):
                                            trim_engine_text = cols[i].get_text(strip=True)
                                            if '|' in trim_engine_text:
                                                parts = trim_engine_text.split('|')
                                                if len(parts) >= 1:
                                                    row_data['trim'] = parts[0].strip()
                                                if len(parts) >= 2:
                                                    row_data['engine'] = parts[1].strip()
                                            else:
                                                # Try to split by common patterns
                                                if 'Cyl' in trim_engine_text:
                                                    # Engine info likely at end
                                                    parts = trim_engine_text.split(',')
                                                    trim_parts = []
                                                    engine_parts = []
                                                    for part in parts:
                                                        if 'Cyl' in part or 'L' in part:
                                                            engine_parts.append(part.strip())
                                                        else:
                                                            trim_parts.append(part.strip())
                                                    row_data['trim'] = ', '.join(trim_parts) if trim_parts else trim_engine_text
                                                    row_data['engine'] = ', '.join(engine_parts) if engine_parts else ''
                                                else:
                                                    row_data['trim'] = trim_engine_text
                            
                            # Expand year ranges and split comma-separated trim/engine values
                            if row_data['year'] or row_data['model']:
                                year_value = row_data.get('year', '')
                                expanded_years = self._expand_year_range(year_value)
                                
                                # Split comma-separated trim values
                                trim_value = row_data.get('trim', '')
                                trim_list = [t.strip() for t in trim_value.split(',') if t.strip()] if trim_value else ['']
                                
                                # Split comma-separated engine values
                                engine_value = row_data.get('engine', '')
                                engine_list = [e.strip() for e in engine_value.split(',') if e.strip()] if engine_value else ['']
                                
                                # Generate all combinations: years × trims × engines
                                for year in expanded_years:
                                    for trim in trim_list:
                                        for engine in engine_list:
                                            fitment_record = {
                                                'year': year,
                                                'make': row_data.get('make', 'Infiniti'),
                                                'model': row_data.get('model', ''),
                                                'trim': trim,
                                                'engine': engine
                                            }
                                            product_data['fitments'].append(fitment_record)
            
            # Fallback: If no fitments found, add empty entry
            if not product_data['fitments']:
                product_data['fitments'].append({
                    'year': '',
                    'make': 'Infiniti',
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

