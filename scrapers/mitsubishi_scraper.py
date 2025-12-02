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
            # Product URLs: /genuine/mitsubishi-{name}~{part}.html
            validated_urls = []
            for url in product_urls:
                if '/genuine/mitsubishi-' in url and '~' in url and url.endswith('.html'):
                    if not any(pattern in url for pattern in ['/accessories/', '/category/', '/oem-mitsubishi-']):
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
            
            self.logger.info(f"Found {len(product_links)} product links, {len(product_urls)} unique URLs")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
        
        return product_urls
    
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
            
            # Extract SKU/Part Number - from URL pattern: /genuine/mitsubishi-{name}~{part}.html
            url_match = re.search(r'/genuine/mitsubishi-.*~([^~]+)\.html', url)
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
            
            # Extract image URL
            img_elem = soup.find('img', src=re.compile(r'kia.*wheel', re.I))
            if not img_elem:
                img_elem = soup.find('img', class_=re.compile(r'product', re.I))
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src')
                if img_url:
                    product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            
            # Extract description
            desc_list = soup.find('ul', class_=re.compile(r'description|spec', re.I))
            if desc_list:
                desc_items = desc_list.find_all('li')
                desc_texts = [item.get_text(strip=True) for item in desc_items]
                product_data['description'] = ' '.join(desc_texts)
            
            # Extract fitment - "Fits the following Mitsubishi Models" with list items like "2010-2011 Mitsubishi Lancer"
            fitment_section = soup.find('div', string=re.compile(r'Fits the following', re.I))
            if fitment_section:
                fitment_parent = fitment_section.find_parent()
                if fitment_parent:
                    fitment_list = fitment_parent.find('ul')
                    if fitment_list:
                        fitment_items = fitment_list.find_all('li')
                        for item in fitment_items:
                            item_text = item.get_text(strip=True)
                            # Parse: "2010-2011 Mitsubishi Lancer"
                            year_match = re.search(r'(\d{4})-(\d{4})', item_text)
                            if year_match:
                                year = year_match.group(1)
                                model_text = item_text.replace(year_match.group(0), '').strip()
                                # Extract model name (remove "Mitsubishi" prefix if present)
                                model = re.sub(r'^Mitsubishi\s+', '', model_text, flags=re.I).strip()
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': 'Mitsubishi',
                                    'model': model,
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

