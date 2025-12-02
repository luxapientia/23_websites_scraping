"""Scraper for parts.bmwofsouthatlanta.com (BMW parts)"""
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

class BMWScraper(BaseScraper):
    """Scraper for parts.bmwofsouthatlanta.com"""
    
    def __init__(self):
        super().__init__('bmw', use_selenium=True)
        self.base_url = 'https://parts.bmwofsouthatlanta.com'
        
    def get_product_urls(self):
        """
        Get all wheel product URLs from parts.bmwofsouthatlanta.com
        Comprehensive discovery strategy:
        1. Search for wheels
        2. Browse Suspension category (wheels often in suspension)
        3. Extract individual product URLs
        """
        product_urls = []
        
        try:
            # Method 1: Search for wheels
            self.logger.info("Method 1: Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            self.logger.info(f"Found {len(search_urls)} URLs via search")
            
            # Method 2: Browse Suspension category (wheels often listed here)
            self.logger.info("Method 2: Browsing Suspension category...")
            category_urls = self._browse_suspension_category()
            product_urls.extend(category_urls)
            self.logger.info(f"Found {len(category_urls)} URLs via category browsing")
            
            # Remove duplicates
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # CRITICAL: Filter out category/listing pages - only keep individual product pages
            validated_urls = []
            filtered_out = []
            
            for url in product_urls:
                # Individual product URLs should match pattern: /oem-parts/bmw-...-{part-number}
                # Should NOT be:
                # - Category pages like /suspension, /brake-pads, etc.
                # - Search results pages
                
                is_product_page = False
                is_category_page = False
                
                # Check if it's a category/listing page (should be filtered out)
                category_patterns = [
                    r'^/[^/]+$',  # Root-level category pages like /suspension, /brake-pads
                    r'/search',  # Search results page
                    r'/v-bmw',  # Vehicle selector page
                    r'/select-bmw-series',  # Series selector page
                ]
                
                for pattern in category_patterns:
                    if re.search(pattern, url, re.I):
                        is_category_page = True
                        break
                
                # Check if it's an individual product page
                product_patterns = [
                    r'/oem-parts/bmw-[^/]+-\d+',  # /oem-parts/bmw-product-name-partnumber
                ]
                
                for pattern in product_patterns:
                    if re.search(pattern, url, re.I):
                        is_product_page = True
                        break
                
                if is_product_page and not is_category_page:
                    validated_urls.append(url)
                else:
                    filtered_out.append(url)
                    self.logger.debug(f"Filtered out: {url} (category_page={is_category_page}, product_page={is_product_page})")
            
            if filtered_out:
                self.logger.warning(f"Filtered out {len(filtered_out)} category/listing page URLs:")
                for url in filtered_out[:10]:  # Show first 10
                    self.logger.warning(f"  - {url}")
                if len(filtered_out) > 10:
                    self.logger.warning(f"  ... and {len(filtered_out) - 10} more")
            
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
            
            # Try to search - the search form might need special handling
            search_url = f"{self.base_url}/search?q=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            # Increase page load timeout
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                
                html = self.get_page(search_url, use_selenium=True, wait_time=2)
                if not html:
                    # Try alternative search URL pattern
                    search_url = f"{self.base_url}/?q=wheel"
                    html = self.get_page(search_url, use_selenium=True, wait_time=2)
                    if not html:
                        self.logger.error("Failed to fetch search page")
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
            
            # Wait for search results to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/bmw-']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load lazy-loaded content
            self._scroll_to_load_content()
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Find product links - pattern: /oem-parts/bmw-product-name-partnumber
            product_links = soup.find_all('a', href=re.compile(r'/oem-parts/bmw-'))
            
            for link in product_links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    # Remove query params and fragments
                    if '?' in full_url:
                        full_url = full_url.split('?')[0]
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    full_url = full_url.rstrip('/')
                    
                    if full_url not in product_urls:
                        product_urls.append(full_url)
            
            self.logger.info(f"Found {len(product_links)} product links on search page, {len(product_urls)} unique URLs")
            
            # Handle pagination
            page_num = 2
            max_pages = 500
            consecutive_empty_pages = 0
            max_consecutive_empty = 4
            
            while page_num <= max_pages:
                try:
                    self.logger.info(f"Loading search page {page_num}...")
                    
                    pagination_urls = [
                        f"{self.base_url}/search?q=wheel&page={page_num}",
                        f"{self.base_url}/search?q=wheel&p={page_num}",
                        f"{self.base_url}/?q=wheel&page={page_num}",
                    ]
                    
                    page_loaded = False
                    pag_url_used = None
                    
                    for pag_url in pagination_urls:
                        try:
                            original_pag_timeout = self.page_load_timeout
                            try:
                                self.page_load_timeout = 60
                                self.driver.set_page_load_timeout(60)
                                
                                html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                                if html and len(html) > 5000:
                                    soup_check = BeautifulSoup(html, 'lxml')
                                    page_links = soup_check.find_all('a', href=re.compile(r'/oem-parts/bmw-'))
                                    if len(page_links) > 0:
                                        page_loaded = True
                                        pag_url_used = pag_url
                                        break
                            except Exception as pag_error:
                                error_str = str(pag_error).lower()
                                if 'timeout' in error_str:
                                    self.logger.debug(f"Timeout loading {pag_url}, trying next pattern...")
                                continue
                            finally:
                                try:
                                    self.page_load_timeout = original_pag_timeout
                                    self.driver.set_page_load_timeout(original_pag_timeout)
                                except:
                                    pass
                        except:
                            continue
                    
                    if not page_loaded:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages failed")
                            break
                        page_num += 1
                        continue
                    
                    # Scroll and extract products
                    self._scroll_to_load_content()
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    page_links = soup.find_all('a', href=re.compile(r'/oem-parts/bmw-'))
                    
                    page_urls_count = 0
                    for link in page_links:
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
                                page_urls_count += 1
                    
                    self.logger.info(f"Page {page_num}: Found {len(page_links)} links, {page_urls_count} new URLs (Total: {len(product_urls)})")
                    
                    if page_urls_count == 0:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            break
                    else:
                        consecutive_empty_pages = 0
                    
                    page_num += 1
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        break
                    page_num += 1
                    continue
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _browse_suspension_category(self):
        """Browse Suspension category page and extract product URLs (wheels often listed here)"""
        product_urls = []
        
        try:
            category_url = f"{self.base_url}/suspension"
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
            
            # Wait for products to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/bmw-']"))
                )
            except:
                pass
            
            # Scroll to load content
            self._scroll_to_load_content()
            
            # Get updated HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract product links
            product_links = soup.find_all('a', href=re.compile(r'/oem-parts/bmw-'))
            
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
            
            self.logger.info(f"Found {len(product_links)} product links from {category_url}")
            
            # Handle pagination for category pages
            page_num = 2
            max_pages = 100
            consecutive_empty = 0
            
            while page_num <= max_pages:
                try:
                    pag_urls = [
                        f"{category_url}?page={page_num}",
                        f"{category_url}?p={page_num}",
                        f"{category_url}/page/{page_num}",
                    ]
                    
                    page_loaded = False
                    for pag_url in pag_urls:
                        try:
                            original_pag_timeout = self.page_load_timeout
                            try:
                                self.page_load_timeout = 60
                                self.driver.set_page_load_timeout(60)
                                
                                html = self.get_page(pag_url, use_selenium=True, wait_time=1)
                                if html and len(html) > 5000:
                                    soup_check = BeautifulSoup(html, 'lxml')
                                    if soup_check.find_all('a', href=re.compile(r'/oem-parts/bmw-')):
                                        page_loaded = True
                                        break
                            except Exception as pag_error:
                                error_str = str(pag_error).lower()
                                if 'timeout' in error_str:
                                    self.logger.debug(f"Timeout loading {pag_url}, trying next pattern...")
                                continue
                            finally:
                                try:
                                    self.page_load_timeout = original_pag_timeout
                                    self.driver.set_page_load_timeout(original_pag_timeout)
                                except:
                                    pass
                        except:
                            continue
                    
                    if not page_loaded:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            break
                        page_num += 1
                        continue
                    
                    self._scroll_to_load_content()
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    page_links = soup.find_all('a', href=re.compile(r'/oem-parts/bmw-'))
                    
                    new_count = 0
                    for link in page_links:
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
                                new_count += 1
                    
                    if new_count == 0:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            break
                    else:
                        consecutive_empty = 0
                    
                    page_num += 1
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    self.logger.debug(f"Error on category page {page_num}: {str(e)}")
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                    page_num += 1
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error browsing Suspension category: {str(e)}")
        
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
        """
        Scrape single product from parts.bmwofsouthatlanta.com
        """
        max_retries = 5
        retry_count = 0
        html = None
        
        while retry_count < max_retries:
            try:
                if not self.check_health():
                    self.logger.error("Scraper health check failed, stopping")
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
                        self.logger.warning(f"Driver error, retrying in {delay:.1f}s...")
                        time.sleep(delay)
                        retry_count += 1
                        continue
                    else:
                        return None
                
                # Load the page with timeout protection
                try:
                    self.page_load_timeout = 60
                    self.driver.set_page_load_timeout(60)
                    
                    self.driver.get(url)
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    # Cloudflare check
                    current_url_check = self.driver.current_url.lower()
                    page_preview = self.driver.page_source[:6000]
                    
                    if ('challenges.cloudflare.com' in current_url_check or '/cdn-cgi/challenge' in current_url_check or len(page_preview) < 5000) and self.has_cloudflare_challenge():
                        self.logger.info("🛡️ Cloudflare challenge detected - waiting for bypass...")
                        cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                        if not cloudflare_bypassed:
                            if len(self.driver.page_source) > 5000 and 'challenges.cloudflare.com' not in self.driver.current_url.lower():
                                self.logger.info("✓ Page accessible despite Cloudflare warning - continuing...")
                            else:
                                retry_count += 1
                                if retry_count < max_retries:
                                    wait_time = random.uniform(10, 15)
                                    self.logger.warning(f"Retrying page load in {wait_time:.1f}s...")
                                    time.sleep(wait_time)
                                    continue
                                else:
                                    return None
                        time.sleep(1)
                    
                    time.sleep(random.uniform(0.5, 1.0))
                    self.simulate_human_behavior()
                    time.sleep(random.uniform(0.5, 1.0))
                    time.sleep(random.uniform(1.5, 3.0))
                    time.sleep(random.uniform(0.5, 1.0))
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Extract title
                    title_text = ''
                    title_selectors = [
                        ("h1.product-title", By.CSS_SELECTOR),
                        ("h1", By.CSS_SELECTOR),
                        ("h2.product-title", By.CSS_SELECTOR),
                        (".product-name", By.CSS_SELECTOR),
                    ]
                    
                    for selector, by_type in title_selectors:
                        try:
                            wait = WebDriverWait(self.driver, 5)
                            if by_type == By.CSS_SELECTOR:
                                title_element = wait.until(EC.presence_of_element_located((by_type, selector)))
                            title_text = title_element.text.strip()
                            if title_text and len(title_text) >= 3:
                                break
                        except:
                            continue
                    
                    if not title_text or len(title_text) < 3:
                        title_elem = (soup.find('h1', class_=re.compile(r'product', re.I)) or 
                                     soup.find('h1') or 
                                     soup.find('h2', class_=re.compile(r'product', re.I)))
                        title_text = title_elem.get_text(strip=True) if title_elem else ''
                        
                        if not title_text or len(title_text) < 3:
                            page_title_tag = soup.find('title')
                            if page_title_tag:
                                title_text = page_title_tag.get_text(strip=True)
                                if '|' in title_text:
                                    title_text = title_text.split('|')[0].strip()
                    
                    # Check if redirected away
                    current_url = self.driver.current_url.lower()
                    if 'bmwofsouthatlanta.com' not in current_url and not current_url.startswith(('chrome-error://', 'about:')):
                        self.logger.warning(f"⚠️ Redirected away from target site: {current_url}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    
                    # Check for error pages
                    if not title_text or len(title_text) < 3:
                        page_content_length = len(html)
                        if page_content_length < 8000:
                            self.logger.warning(f"⚠️ Page appears blocked or empty: {url}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    
                    # Success
                    html = self.driver.page_source
                    self.logger.info(f"✓ Page loaded successfully, title: {title_text[:50]}")
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                    break
                    
                except TimeoutException as e:
                    self.logger.warning(f"⚠️ Page load timeout - waiting longer...")
                    
                    try:
                        wait_for_content = 8
                        waited = 0
                        content_ready = False
                        
                        while waited < wait_for_content:
                            html = self.driver.page_source
                            current_url = self.driver.current_url.lower()
                            
                            if any(err in current_url for err in ['chrome-error://', 'err_', 'dns_probe']):
                                raise Exception(f"Connection error: {current_url}")
                            
                            if html and len(html) > 8000:
                                soup = BeautifulSoup(html, 'lxml')
                                has_title = soup.find('h1')
                                has_body = soup.find('body')
                                body_text = has_body.get_text(strip=True) if has_body else ''
                                
                                if (has_title or len(body_text) > 500) and len(body_text) > 300:
                                    content_ready = True
                                    self.logger.info(f"✓ Full content loaded after {waited}s")
                                    break
                            
                            time.sleep(0.5)
                            waited += 0.5
                        
                        if not content_ready:
                            raise Exception("Full content not loaded")
                            
                    except Exception as e:
                        self.logger.warning(f"Could not get full content: {str(e)}")
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(5, 8)
                            time.sleep(wait_time)
                            continue
                        else:
                            self.page_load_timeout = original_timeout
                            self.driver.set_page_load_timeout(original_timeout)
                            return None
                    finally:
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                        
                except Exception as e:
                    error_str = str(e).lower()
                    if any(err in error_str for err in ['connection', 'network', 'dns', 'err_', 'timeout']):
                        self.logger.warning(f"⚠️ Connection/network error: {str(e)}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(5, 8)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    else:
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                        raise
                    
            except Exception as e:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
                self.logger.error(f"❌ Exception: {type(e).__name__}: {str(e)}")
                recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                
                if not recovery['should_retry']:
                    return None
                
                if retry_count < max_retries - 1:
                    wait_time = recovery['wait_time']
                    delay = random.uniform(wait_time[0], wait_time[1])
                    self.logger.warning(f"Retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    retry_count += 1
                    continue
                else:
                    return None
        
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Initialize product data
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
            title_elem = soup.find('h1', class_=re.compile(r'product', re.I))
            if not title_elem:
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
                self.logger.warning(f"⚠️ No valid title found for {url}")
                return None
            
            safe_title = self.safe_str(product_data['title'][:60] if len(product_data['title']) > 60 else product_data['title'])
            self.logger.info(f"📝 Found title: {safe_title}")
            
            # Extract SKU/Part Number - try multiple methods
            # Pattern from URL: /oem-parts/bmw-product-name-{part-number}
            # Part number is the last segment (digits) after the last dash
            url_match = re.search(r'/oem-parts/bmw-[^/]+-(\d+)(?:\?|$)', url)
            if url_match:
                product_data['sku'] = url_match.group(1)
                product_data['pn'] = self.clean_sku(product_data['sku'])
                self.logger.info(f"📦 Found SKU from URL: {self.safe_str(product_data['sku'])}")
            
            # Also try to find in page
            if not product_data['sku']:
                sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
                if not sku_elem:
                    sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
                if sku_elem:
                    product_data['sku'] = self.safe_find_text(soup, sku_elem)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if this is a wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                safe_title_preview = self.safe_str(product_data['title'][:60] if len(product_data['title']) > 60 else product_data['title'])
                self.logger.info(f"🔍 Checking: '{safe_title_preview}' -> {'✅ WHEEL' if is_wheel else '❌ SKIPPED'}")

                if not is_wheel:
                    safe_title = self.safe_str(product_data['title'])
                    self.logger.info(f"⏭️ Skipping non-wheel product: {safe_title}")
                    return None
            except Exception as e:
                self.logger.warning(f"Error checking if wheel product: {self.safe_str(e)}")
                return None
            
            # Extract price
            price_elem = soup.find('span', class_=re.compile(r'price|sale.*price', re.I))
            if not price_elem:
                price_elem = soup.find('div', class_=re.compile(r'price|sale.*price', re.I))
            if not price_elem:
                price_elem = soup.find('strong', class_=re.compile(r'price', re.I))
            if price_elem:
                price_text = self.safe_find_text(soup, price_elem)
                product_data['actual_price'] = self.extract_price(price_text)
                if product_data['actual_price']:
                    self.logger.info(f"💰 Found price: {self.safe_str(product_data['actual_price'])}")
            
            # Extract MSRP
            msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if not msrp_elem:
                msrp_elem = soup.find('del', class_=re.compile(r'price', re.I))
            if msrp_elem:
                msrp_text = self.safe_find_text(soup, msrp_elem)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Extract image URL
            img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
            if not img_elem:
                img_elem = soup.find('img', itemprop='image')
            if not img_elem:
                img_elem = soup.find('img', id=re.compile(r'product.*image', re.I))
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src')
                if img_url:
                    product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            if product_data.get('image_url') and not product_data['image_url'].startswith('http'):
                product_data['image_url'] = f"https:{product_data['image_url']}"
            
            # Extract description
            desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
            if not desc_elem:
                desc_elem = soup.find('span', class_=re.compile(r'description', re.I))
            if not desc_elem:
                desc_elem = soup.find('p', class_=re.compile(r'description', re.I))
            if desc_elem:
                desc_text = self.safe_find_text(soup, desc_elem)
                desc_text = re.sub(r'\s+', ' ', desc_text)
                product_data['description'] = desc_text.strip()
            
            # Extract fitment data - try JSON script tag
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
                                trims = fitment_entry.get('trims', []) or ['']
                                engines = fitment_entry.get('engines', []) or ['']
                                
                                if isinstance(trims, str):
                                    trims = [t.strip() for t in trims.split(',') if t.strip()]
                                elif not isinstance(trims, list):
                                    trims = ['']
                                
                                if isinstance(engines, str):
                                    engines = [e.strip() for e in engines.split(',') if e.strip()]
                                elif not isinstance(engines, list):
                                    engines = ['']
                                
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
                            except:
                                continue
                except:
                    pass
            
            # If no fitments found, add empty fitment
            if not product_data['fitments']:
                product_data['fitments'].append({
                    'year': '',
                    'make': '',
                    'model': '',
                    'trim': '',
                    'engine': ''
                })
            
            safe_title = self.safe_str(product_data['title'])
            self.logger.info(f"✅ Successfully scraped: {safe_title}")
            return product_data
            
        except Exception as e:
            safe_error = self.safe_str(e)
            self.logger.error(f"❌ Error scraping product {url}: {safe_error}")
            import traceback
            try:
                tb_str = traceback.format_exc()
                safe_tb = self.safe_str(tb_str)
                self.logger.error(safe_tb)
            except:
                pass
            return None

