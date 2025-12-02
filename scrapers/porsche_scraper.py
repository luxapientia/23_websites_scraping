"""Scraper for parts.byersporsche.com (Porsche parts) - SimplePart platform"""
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

class PorscheScraper(BaseScraper):
    """Scraper for parts.byersporsche.com - Uses SimplePart platform"""
    
    def __init__(self):
        super().__init__('porsche', use_selenium=True)
        self.base_url = 'https://parts.byersporsche.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from parts.byersporsche.com"""
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
                # Product URLs: /p/Porsche__/Product-Name/ID/PartNumber.html
                # Category URLs: /Porsche__/Category.html or /productSearch.aspx
                is_product = re.search(r'/p/Porsche__/[^/]+/\d+/[^/]+\.html', url, re.I)
                is_category = re.search(r'/Porsche__/[^/]+\.html$|/productSearch\.aspx', url, re.I)
                
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
        """Search for wheels using site search"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            search_url = f"{self.base_url}/productSearch.aspx?searchTerm=wheel"
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Porsche__/']"))
                )
            except:
                pass
            
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            product_links = soup.find_all('a', href=re.compile(r'/p/Porsche__/'))
            
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
            
            self.logger.info(f"Found {len(product_links)} product links, {len(product_urls)} unique URLs")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
        
        return product_urls
    
    def _browse_tire_wheel_category(self):
        """Browse Tire and Wheel category"""
        product_urls = []
        
        try:
            category_url = f"{self.base_url}/Porsche__/Tire-and-Wheel.html"
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(category_url, use_selenium=True, wait_time=2)
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
            
            product_links = soup.find_all('a', href=re.compile(r'/p/Porsche__/'))
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
            self.logger.error(f"Error browsing category: {str(e)}")
        
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
        """Scrape single product from parts.byersporsche.com"""
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
            
            # Extract SKU/Part Number - Pattern from URL: /p/Porsche__/Product-Name/ID/PartNumber.html
            url_match = re.search(r'/p/Porsche__/[^/]+/\d+/([^/]+)\.html', url)
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
            
            # Extract MSRP
            msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if msrp_elem:
                msrp_text = self.safe_find_text(soup, msrp_elem)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Extract image - SimplePart: #part-image-left a > img.img-responsive
            part_image_left = soup.find('div', id='part-image-left')
            if part_image_left:
                img_link = part_image_left.find('a')
                if img_link:
                    img_elem = img_link.find('img', class_='img-responsive')
                    if img_elem:
                        img_url = img_elem.get('src') or img_elem.get('data-src')
                        if img_url:
                            product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            
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
                            
                            # Parse: "Model (Year-Year) [Engine]"
                            engine_match = re.search(r'\[([^\]]+)\]', cell_text)
                            engine = engine_match.group(1).strip() if engine_match else ''
                            
                            model_text = re.sub(r'\s*\[[^\]]+\]', '', cell_text).strip()
                            
                            year_range_match = re.search(r'\((\d{4})-(\d{4})\)', model_text)
                            if year_range_match:
                                year = year_range_match.group(1)
                                model = model_text.strip()
                            else:
                                year = ''
                                model = model_text.strip()
                            
                            make = ''
                            model_without_year = re.sub(r'\s*\(\d{4}-\d{4}\)', '', model).strip()
                            model_words = model_without_year.split()
                            if len(model_words) >= 1:
                                make = model_words[0]
                            
                            if model:
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': make,
                                    'model': model,
                                    'trim': '',
                                    'engine': engine
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

