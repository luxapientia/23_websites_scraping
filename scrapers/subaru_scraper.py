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
        """Get all wheel product URLs from parts.subaru.com"""
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
            
            search_url = f"{self.base_url}/search?q=wheel"
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/product/'], a[href*='/parts/'], a[href*='/oem-parts/']"))
                )
            except:
                pass
            
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            product_links = (soup.find_all('a', href=re.compile(r'/product/')) +
                           soup.find_all('a', href=re.compile(r'/parts/')) +
                           soup.find_all('a', href=re.compile(r'/oem-parts/')))
            
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
            # Extract title
            title_elem = soup.find('h1')
            if not title_elem:
                title_elem = soup.find('meta', property='og:title')
                if title_elem:
                    product_data['title'] = title_elem.get('content', '').strip()
                else:
                    title_tag = soup.find('title')
                    if title_tag:
                        product_data['title'] = self.safe_find_text(soup, title_tag)
            
            if title_elem and not product_data['title']:
                product_data['title'] = self.safe_find_text(soup, title_elem)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number
            sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
            if sku_elem:
                product_data['sku'] = self.safe_find_text(soup, sku_elem)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract price
            price_elem = soup.find('span', class_=re.compile(r'price|sale.*price', re.I))
            if not price_elem:
                price_elem = soup.find('div', class_=re.compile(r'price|sale.*price', re.I))
            if price_elem:
                price_text = self.safe_find_text(soup, price_elem)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Extract MSRP
            msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if msrp_elem:
                msrp_text = self.safe_find_text(soup, msrp_elem)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Extract image URL
            img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
            if not img_elem:
                img_elem = soup.find('img', itemprop='image')
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src')
                if img_url:
                    product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            
            # Extract description
            desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
            if desc_elem:
                desc_text = self.safe_find_text(soup, desc_elem)
                desc_text = re.sub(r'\s+', ' ', desc_text)
                product_data['description'] = desc_text.strip()
            
            # Extract fitment data
            product_data_script = soup.find('script', type='application/json')
            if product_data_script and product_data_script.string:
                try:
                    product_json = json.loads(product_data_script.string)
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

