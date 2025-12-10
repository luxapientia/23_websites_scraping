"""Scraper for www.kiapartsnow.com (Kia parts) - Auto Parts Prime platform"""
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

class KiaScraper(BaseScraper):
    """Scraper for www.kiapartsnow.com - Uses Auto Parts Prime platform (similar to AcuraPartsWarehouse)"""
    
    def __init__(self):
        super().__init__('kia', use_selenium=True)
        self.base_url = 'https://www.kiapartsnow.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.kiapartsnow.com"""
        product_urls = []
        
        try:
            self.logger.info("Extracting wheel product URLs from wheel cover listing page...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            # Product URLs: /genuine/kia-{name}~{part}.html
            validated_urls = []
            for url in product_urls:
                if '/genuine/kia-' in url and '~' in url and url.endswith('.html'):
                    if not any(pattern in url for pattern in ['/accessories/', '/category/', '/oem-kia-']):
                        validated_urls.append(url)
            
            product_urls = validated_urls
            self.logger.info(f"Final validated product URLs: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """
        Visit all three wheel category listing pages and extract all product URLs, handling pagination
        URLs:
        1. https://www.kiapartsnow.com/oem-kia-wheel_cover.html
        2. https://www.kiapartsnow.com/oem-kia-spare_wheel.html
        3. https://www.kiapartsnow.com/accessories/kia-wheels.html
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # All three category URLs to visit one by one
            category_urls = [
                f"{self.base_url}/oem-kia-wheel_cover.html",
                f"{self.base_url}/oem-kia-spare_wheel.html",
                f"{self.base_url}/accessories/kia-wheels.html",
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/genuine/kia-']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products on the first page (if lazy loading)
            self._scroll_to_load_content()
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract total page count from "Page 1 of X" pattern
            total_pages = 1
            try:
                # Look for "Page X of Y" pattern
                page_info = soup.find(string=re.compile(r'Page\s+\d+\s+of\s+\d+', re.I))
                if page_info:
                    page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', str(page_info), re.I)
                    if page_match:
                        total_pages = int(page_match.group(1))
                        self.logger.info(f"Found pagination: {total_pages} total pages for {category_url}")
            except Exception as e:
                self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
            
            # Extract products from all pages
            for page_num in range(1, total_pages + 1):
                try:
                    if page_num > 1:
                        # Navigate to the next page
                        pag_url = f"{category_url}?page={page_num}"
                        self.logger.info(f"Loading page {page_num}/{total_pages}: {pag_url}")
                        
                        try:
                            self.page_load_timeout = 60
                            self.driver.set_page_load_timeout(60)
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                self.logger.warning(f"Page {page_num} content too short, skipping")
                                continue
                            
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
                    
                    # Product URLs: /genuine/kia-{name}~{part}.html
                    product_links = soup.find_all('a', href=re.compile(r'/genuine/kia-.*~.*\.html'))
                    
                    page_count = 0
                    for link in product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove fragment and query params
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            
                            full_url = full_url.rstrip('/')
                            
                            # Only collect individual product pages
                            if '/genuine/kia-' in full_url and '~' in full_url and full_url.endswith('.html'):
                                # Filter out category/listing pages
                                if not any(pattern in full_url for pattern in ['/accessories/', '/category/', '/oem-kia-']):
                                    if full_url not in existing_urls and full_url not in new_urls:
                                        new_urls.append(full_url)
                                        existing_urls.append(full_url)
                                        page_count += 1
                    
                    self.logger.info(f"Page {page_num}/{total_pages}: Found {len(product_links)} product links, {page_count} new unique URLs (Category total: {len(new_urls)})")
                    
                    # Small delay between pages
                    if page_num < total_pages:
                        time.sleep(random.uniform(1, 2))
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    continue
            
            self.logger.info(f"Completed category {category_url}: Found {len(new_urls)} new unique product URLs")
            
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
    
    def scrape_product(self, url):
        """Scrape single product from www.kiapartsnow.com"""
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
            # Extract title
            title_elem = soup.find('h1')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_tag = soup.find('title')
                if title_tag:
                    product_data['title'] = self.safe_find_text(soup, title_tag)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - from URL pattern: /genuine/kia-{name}~{part}.html
            url_match = re.search(r'/genuine/kia-.*~([^~]+)\.html', url)
            if url_match:
                product_data['sku'] = url_match.group(1)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Also try from page: "Part Number: U82002K000"
            if not product_data['sku']:
                part_number_div = soup.find('div', string=re.compile(r'Part\s+Number\s*:', re.I))
                if part_number_div:
                    part_link = part_number_div.find_next('a')
                    if part_link:
                        product_data['sku'] = part_link.get_text(strip=True)
                        product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract price - structure: $79.90 with MSRP: $105.00
            price_div = soup.find('div', class_=re.compile(r'price|sale', re.I))
            if price_div:
                price_text = price_div.get_text(strip=True)
                # Look for dollar amount
                price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
                if price_match:
                    product_data['actual_price'] = price_match.group(1).replace(',', '')
            
            # Extract MSRP
            msrp_elem = soup.find(string=re.compile(r'MSRP\s*:', re.I))
            if msrp_elem:
                msrp_parent = msrp_elem.find_parent()
                if msrp_parent:
                    msrp_text = msrp_parent.get_text(strip=True)
                    msrp_match = re.search(r'MSRP\s*:\s*\$([\d,]+\.?\d*)', msrp_text, re.I)
                    if msrp_match:
                        product_data['msrp'] = msrp_match.group(1).replace(',', '')
            
            # Extract image URL - look for main product image in pn-img-img div
            img_container = soup.find('div', class_='pn-img-img')
            if img_container:
                img_elem = img_container.find('img')
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Fallback: try other image patterns
            if not product_data['image_url']:
                img_elem = soup.find('img', src=re.compile(r'/resources/encry/actual-picture', re.I))
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description
            desc_list = soup.find('ul', class_=re.compile(r'description|spec', re.I))
            if desc_list:
                desc_items = desc_list.find_all('li')
                desc_texts = [item.get_text(strip=True) for item in desc_items]
                product_data['description'] = ' '.join(desc_texts)
            
            # Extract fitment from table structure
            # Table: fit-vehicle-list-table with columns: Year Make Model, Trim & Engine, Important vehicle option details
            fitment_table = soup.find('table', class_='fit-vehicle-list-table')
            if fitment_table:
                tbody = fitment_table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 3:
                            # First column: Year Make Model (e.g., "2009-2021 Kia Forte")
                            year_model_cell = cells[0]
                            year_model_text = year_model_cell.get_text(strip=True)
                            
                            # Extract year range and model
                            year_range_match = re.search(r'(\d{4})\s*-\s*(\d{4})', year_model_text)
                            start_year = None
                            end_year = None
                            
                            if year_range_match:
                                start_year = int(year_range_match.group(1))
                                end_year = int(year_range_match.group(2))
                            else:
                                # Try single year
                                year_match = re.search(r'(\d{4})', year_model_text)
                                if year_match:
                                    start_year = int(year_match.group(1))
                                    end_year = int(year_match.group(1))
                            
                            if start_year is not None and end_year is not None:
                                
                                # Extract model name (remove year range/single year and "Kia" prefix)
                                model_text = re.sub(r'\d{4}\s*-\s*\d{4}\s*', '', year_model_text)  # Remove year range
                                model_text = re.sub(r'^\d{4}\s+', '', model_text)  # Remove single year if still present
                                model_text = re.sub(r'^Kia\s+', '', model_text, flags=re.I).strip()
                                
                                # Second column: Trim & Engine (comma-separated, may contain pipe "|")
                                trim_engine_cell = cells[1]
                                trim_engine_text = trim_engine_cell.get_text(strip=True)
                                # Split by comma, handling formats like "High Grade|1.6L, 1.6L - Alpha DOHC"
                                trim_engines = [te.strip() for te in trim_engine_text.split(',') if te.strip()]
                                
                                # Third column: Important vehicle option details (comma-separated)
                                options_cell = cells[2]
                                options_text = options_cell.get_text(strip=True)
                                options = [opt.strip() for opt in options_text.split(',') if opt.strip()]
                                
                                # Expand year range and create fitment records for all combinations
                                # Formula: years × trim_engines × options
                                for year in range(start_year, end_year + 1):
                                    year_str = str(year)
                                    
                                    # If we have trim/engines, create one record per trim/engine
                                    if trim_engines:
                                        for trim_engine in trim_engines:
                                            # Each trim_engine is the full engine description (e.g., "1.6L - GAMMA" or "High Grade|1.6L")
                                            engine = trim_engine.strip()
                                            
                                            # If we have options, create one record per option
                                            if options:
                                                for option in options:
                                                    product_data['fitments'].append({
                                                        'year': year_str,
                                                        'make': 'Kia',
                                                        'model': model_text,
                                                        'trim': option,
                                                        'engine': engine
                                                    })
                                            else:
                                                # No options, create one record per engine
                                                product_data['fitments'].append({
                                                    'year': year_str,
                                                    'make': 'Kia',
                                                    'model': model_text,
                                                    'trim': '',
                                                    'engine': engine
                                                })
                                    else:
                                        # No trim/engine, create one record per year
                                        if options:
                                            # Create one record per option
                                            for option in options:
                                                product_data['fitments'].append({
                                                    'year': year_str,
                                                    'make': 'Kia',
                                                    'model': model_text,
                                                    'trim': option,
                                                    'engine': ''
                                                })
                                        else:
                                            # No options either, create one record per year
                                            product_data['fitments'].append({
                                                'year': year_str,
                                                'make': 'Kia',
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

