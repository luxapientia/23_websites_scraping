"""Scraper for parts.audiusa.com (Audi parts)"""
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

class AudiUSAScraper(BaseScraperWithExtension):
    """Scraper for parts.audiusa.com"""
    
    def __init__(self):
        super().__init__('audiusa', use_selenium=True)
        self.base_url = 'https://parts.audiusa.com'
        
    def get_product_urls(self):
        """
        Get all wheel product URLs from the specific search URL on parts.audiusa.com
        Only collects URLs from: productSearch.aspx?ukey_make=5792&searchTerm=wheel&numResults=250
        No pagination - all results are on a single page
        """
        product_urls = []
        
        try:
            # Use the specific search URL provided
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            self.logger.info(f"Found {len(search_urls)} URLs from search page")
            
            # Remove duplicates
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # CRITICAL: Filter out category/listing pages - only keep individual product pages
            validated_urls = []
            filtered_out = []
            
            for url in product_urls:
                # Individual product URLs should match pattern: /p/Audi__/.../.../....html
                # Should NOT be:
                # - /Audi__/Tire-and-Wheel.html (category page)
                # - /accessories/Audi__.html (accessories listing)
                # - /productSearch.aspx (search results page)
                
                is_product_page = False
                is_category_page = False
                
                # Check if it's a category/listing page (should be filtered out)
                category_patterns = [
                    r'/Audi__/[^/]+\.html$',  # Category pages like /Audi__/Tire-and-Wheel.html
                    r'/accessories/[^/]+\.html$',  # Accessories listing pages
                    r'/productSearch\.aspx',  # Search results page
                    r'/Audi_\d+_\.html$',  # Year-based listing pages
                ]
                
                for pattern in category_patterns:
                    if re.search(pattern, url, re.I):
                        is_category_page = True
                        break
                
                # Check if it's an individual product page
                product_patterns = [
                    r'/p/Audi__/[^/]+/\d+/[^/]+\.html',  # /p/Audi__/Product-Name/ID/PartNumber.html
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
        """
        Search for wheels using the specific search URL
        URL: productSearch.aspx?ukey_make=5792&searchTerm=wheel&numResults=250
        No pagination - all results are on a single page
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Use the specific search URL provided
            search_url = "https://parts.audiusa.com/productSearch.aspx?ukey_make=5792&modelYear=0&ukey_model=0&ukey_trimLevel=0&ukey_driveline=0&ukey_Category=0&numResults=250&sortOrder=Relevance&ukey_tag=0&isOnSale=0&isAccessory=0&isPerformance=0&showAllModels=1&searchTerm=wheel"
            self.logger.info(f"Loading search page: {search_url}")
            
            # Increase page load timeout for search page (to allow Cloudflare to complete)
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60  # Increase to 60 seconds for search page
                self.driver.set_page_load_timeout(60)
                
                html = self.get_page(search_url, use_selenium=True, wait_time=2)
                if not html:
                    self.logger.error("Failed to fetch search page")
                    return product_urls
                
            except Exception as e:
                self.logger.error(f"Error loading search page: {str(e)}")
                return product_urls
            finally:
                # Restore original timeout
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for search results to load
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Audi__/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load lazy-loaded content (in case there's infinite scroll)
            self._scroll_to_load_content()
            
            # Get updated HTML after scrolling
            if not self.driver:
                self.logger.error("Driver not initialized")
                return product_urls
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Find product links - pattern: /p/Audi__/Product-Name/ID/PartNumber.html
            product_links = soup.find_all('a', href=re.compile(r'/p/Audi__/'))
            
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
            
            # No pagination - all results are on a single page
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _browse_tire_wheel_category(self):
        """Browse Tire and Wheel category page and extract product URLs"""
        product_urls = []
        
        try:
            category_url = f"{self.base_url}/Audi__/Tire-and-Wheel.html"
            self.logger.info(f"Browsing category: {category_url}")
            
            # Increase page load timeout for category pages
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
                # Restore original timeout
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for products to load
            if not self.driver:
                self.logger.error("Driver not initialized before WebDriverWait")
                return product_urls
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Audi__/']"))
                )
            except:
                pass
            
            # Scroll to load content
            self._scroll_to_load_content()
            
            # Get updated HTML
            if not self.driver:
                self.logger.error("Driver not initialized after get_page()")
                return product_urls
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract product links
            product_links = soup.find_all('a', href=re.compile(r'/p/Audi__/'))
            
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
                                    if soup_check.find_all('a', href=re.compile(r'/p/Audi__/')):
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
                    if not self.driver:
                        self.logger.error("Driver not initialized after pagination in category browse")
                        break
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    page_links = soup.find_all('a', href=re.compile(r'/p/Audi__/'))
                    
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
            self.logger.error(f"Error browsing Tire and Wheel category: {str(e)}")
        
        return product_urls
    
    def _browse_wheels_accessories(self):
        """Browse Wheels accessories and extract product URLs"""
        product_urls = []
        
        try:
            # Try to find wheels in accessories
            accessories_url = f"{self.base_url}/accessories/Audi__.html"
            self.logger.info(f"Browsing accessories: {accessories_url}")
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                
                html = self.get_page(accessories_url, use_selenium=True, wait_time=2)
                if not html:
                    return product_urls
            except Exception as e:
                self.logger.error(f"Error loading accessories page: {str(e)}")
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Look for "Wheels" link or section
            soup = BeautifulSoup(html, 'lxml')
            
            # Find links containing "wheel" in text or href
            wheel_links = soup.find_all('a', href=True, string=re.compile(r'wheel', re.I))
            if not wheel_links:
                wheel_links = soup.find_all('a', href=re.compile(r'wheel', re.I))
            
            for link in wheel_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()
                if 'wheel' in link_text or 'wheel' in href.lower():
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    if '?' in full_url:
                        full_url = full_url.split('?')[0]
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    
                    # If it's a product page, add it
                    if '/p/Audi__/' in full_url:
                        if full_url not in product_urls:
                            product_urls.append(full_url)
                    # If it's a category/listing page, browse it
                    elif '/accessories/' in full_url or '/Audi__/' in full_url:
                        # Browse this subcategory
                        subcategory_urls = self._browse_category_page(full_url)
                        product_urls.extend(subcategory_urls)
            
            # Also extract direct product links from accessories page
            product_links = soup.find_all('a', href=re.compile(r'/p/Audi__/'))
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
            self.logger.error(f"Error browsing wheels accessories: {str(e)}")
        
        return product_urls
    
    def _browse_category_page(self, category_url):
        """Browse a category/listing page and extract product URLs"""
        product_urls = []
        
        try:
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                
                html = self.get_page(category_url, use_selenium=True, wait_time=2)
                if not html:
                    return product_urls
            except Exception as e:
                self.logger.debug(f"Error loading category page {category_url}: {str(e)}")
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            self._scroll_to_load_content()
            if not self.driver:
                self.logger.error("Driver not initialized after get_page()")
                return product_urls
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            product_links = soup.find_all('a', href=re.compile(r'/p/Audi__/'))
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
            self.logger.debug(f"Error browsing category page: {str(e)}")
        
        return product_urls
    
    def _scroll_to_load_content(self):
        """Scroll page to load lazy-loaded content"""
        if not self.driver:
            self.logger.warning("Driver not initialized, skipping scroll")
            return
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
        Scrape single product from parts.audiusa.com
        """
        max_retries = 5
        retry_count = 0
        html = None
        
        # Add delay before each product page request to avoid rate limiting
        if retry_count == 0:  # Only delay on first attempt, not retries
            delay = random.uniform(3, 6)  # 3-6 seconds between product pages
            self.logger.debug(f"Waiting {delay:.1f}s before loading product page...")
            time.sleep(delay)
        
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
                    if not self.driver:
                        self.logger.error("Driver not initialized after get()")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    current_url_check = self.driver.current_url.lower()
                    page_preview = self.driver.page_source[:6000]
                    
                    if ('challenges.cloudflare.com' in current_url_check or '/cdn-cgi/challenge' in current_url_check or len(page_preview) < 5000) and self.has_cloudflare_challenge():
                        self.logger.info("🛡️ Cloudflare challenge detected - waiting for bypass...")
                        cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                        if not cloudflare_bypassed:
                            if not self.driver:
                                self.logger.error("Driver not initialized after Cloudflare wait")
                                retry_count += 1
                                if retry_count < max_retries:
                                    wait_time = random.uniform(10, 15)
                                    self.logger.warning(f"Retrying page load in {wait_time:.1f}s...")
                                    time.sleep(wait_time)
                                    continue
                                else:
                                    return None
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
                    if not self.driver:
                        self.logger.error("Driver not initialized before checking redirect")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    current_url = self.driver.current_url.lower()
                    if 'audiusa.com' not in current_url and not current_url.startswith(('chrome-error://', 'about:')):
                        self.logger.warning(f"⚠️ Redirected away from target site: {current_url}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    
                    # Enhanced blocking detection
                    page_content_length = len(html)
                    page_text_lower = html.lower()
                    
                    # Check for blocking messages in page content
                    blocking_keywords = [
                        'you have been blocked',
                        'access denied',
                        'blocked',
                        'forbidden',
                        '403 forbidden',
                        'access to this page has been denied',
                        'your request has been blocked',
                        'this request has been blocked',
                        'unusual traffic',
                        'suspicious activity',
                    ]
                    
                    is_blocked_message = any(keyword in page_text_lower for keyword in blocking_keywords)
                    
                    # Check for critical error URLs
                    critical_error_patterns = [
                        'chrome-error://',
                        'err_connection',
                        'dns_probe',
                        '/404',
                        '/403',
                        '?404',
                        '?403',
                        'error=404',
                        'error=403',
                        'status=404',
                        'status=403',
                    ]
                    has_critical_error = any(pattern in current_url for pattern in critical_error_patterns)
                    
                    # Check if page has substantial content (indicates it's not blocked)
                    has_substantial_content = page_content_length > 8000  # At least 8KB of content
                    
                    # Check for product-specific elements to verify it's a product page
                    product_indicators = [
                        soup.find('span', class_=re.compile(r'sku|part.*number|stock-code', re.I)),
                        soup.find('div', class_=re.compile(r'product.*price|price.*product', re.I)),
                        soup.find('div', class_=re.compile(r'product.*info|product.*details', re.I)),
                        soup.find('button', class_=re.compile(r'add.*cart|buy.*now', re.I)),
                        soup.find('span', {'itemprop': 'price'}),
                        soup.find('span', class_=lambda x: x and 'stock-code-text' in ' '.join(x) if isinstance(x, list) else 'stock-code-text' in str(x)),
                    ]
                    has_product_elements = any(product_indicators)
                    
                    # Check if title is just domain (indicates blocking)
                    title_lower = title_text.lower() if title_text else ''
                    title_is_domain = title_lower in ['parts.audiusa.com', 'audiusa.com', 'audiusa', '']
                    
                    # Determine if page is blocked
                    is_likely_blocked = (
                        is_blocked_message or 
                        (title_is_domain and not has_substantial_content and not has_product_elements)
                    )
                    
                    # Don't mark as error if page has valid product content
                    if has_critical_error and has_substantial_content and has_product_elements and title_text and len(title_text) > 3:
                        has_critical_error = False
                        self.logger.debug("False positive error detection avoided: valid product page")
                    
                    is_error_page = has_critical_error or is_likely_blocked
                    
                    if is_error_page:
                        error_type = "error page" if has_critical_error else "blocked"
                        self.logger.warning(f"⚠️ {error_type.capitalize()} detected on attempt {retry_count + 1}, title: '{title_text[:50]}', content: {page_content_length} bytes, has_product_elements: {has_product_elements}")
                        retry_count += 1
                        if retry_count < max_retries:
                            # Anti-blocking cooldown: wait significantly longer when blocked
                            if error_type == "blocked":
                                # Blocked = wait 30-60 seconds (progressive, increased for anti-blocking)
                                base_wait = 30 + (retry_count * 10)  # 30s, 40s, 50s, 60s, 70s
                                wait_time = random.uniform(base_wait, base_wait + 15)
                                self.logger.warning(f"⚠️ BLOCKED - Extended cooldown: {wait_time:.1f} seconds before retry...")
                                
                                # If blocked multiple times, restart browser session
                                if retry_count >= 2:
                                    self.logger.info("🔄 Restarting browser session due to repeated blocking...")
                                    try:
                                        if self.driver:
                                            self.driver.quit()
                                        self.driver = None
                                        time.sleep(random.uniform(5, 10))
                                        self.ensure_driver()
                                        self.logger.info("✓ Browser session restarted")
                                    except Exception as restart_error:
                                        self.logger.warning(f"Error restarting browser: {str(restart_error)}")
                            else:
                                # Error page = wait 10-15 seconds
                                wait_time = random.uniform(10, 15)
                                self.logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                            
                            time.sleep(wait_time)
                            
                            # If blocked multiple times, add extra human-like behavior
                            if error_type == "blocked" and retry_count >= 2:
                                self.logger.info("Simulating extended human behavior after blocking...")
                                time.sleep(random.uniform(10, 20))
                                self.simulate_human_behavior()
                            
                            continue
                        else:
                            self.logger.error(f"❌ Failed after {max_retries} attempts - {error_type}")
                            return None
                    
                    # Check for error pages (fallback for empty pages)
                    if not title_text or len(title_text) < 3:
                        if page_content_length < 8000:
                            self.logger.warning(f"⚠️ Page appears empty: {url}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    
                    # Success
                    if not self.driver:
                        self.logger.error("Driver not initialized before final page_source access")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
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
                            if not self.driver:
                                self.logger.error("Driver not initialized during timeout recovery")
                                raise Exception("Driver not initialized")
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
            # Extract title - Actual structure: <h2 class="panel-title"><span class="prodDescriptH2">Wheel</span>
            h2_title = soup.find('h2', class_='panel-title')
            if h2_title:
                span_title = h2_title.find('span', class_='prodDescriptH2')
                if span_title:
                    product_data['title'] = span_title.get_text(strip=True)
                else:
                    product_data['title'] = h2_title.get_text(strip=True)
            
            # Fallback: <h1> or <h1> > <span>
            if not product_data['title']:
                title_elem = soup.find('h1')
                if title_elem:
                    span_elem = title_elem.find('span')
                    if span_elem:
                        product_data['title'] = span_elem.get_text(strip=True)
                    else:
                        product_data['title'] = title_elem.get_text(strip=True)
            
            # Fallback: <title> tag
            if not product_data['title']:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # Extract from "1J0601027M03C - Wheel - Genuine Audi Part" -> "Wheel"
                    if ' - ' in title_text:
                        parts = title_text.split(' - ')
                        if len(parts) >= 2:
                            product_data['title'] = parts[1].strip()
                    else:
                        product_data['title'] = title_text
            
            # Fallback: meta og:title
            if not product_data['title']:
                title_elem = soup.find('meta', property='og:title')
                if title_elem:
                    product_data['title'] = title_elem.get('content', '').strip()
            
            if not product_data['title'] or len(product_data['title']) < 3:
                self.logger.warning(f"⚠️ No valid title found for {url}")
                return None
            
            safe_title = self.safe_str(product_data['title'][:60] if len(product_data['title']) > 60 else product_data['title'])
            self.logger.info(f"📝 Found title: {safe_title}")
            
            # Extract SKU/Part Number - Actual structure: <span itemprop="value" class="body-3 stock-code-text"><strong>1J0601027M03C</strong></span>
            # PRIORITY: Extract from itemprop="value" with stock-code-text class
            sku_span = soup.find('span', {'itemprop': 'value', 'class': lambda x: x and 'stock-code-text' in ' '.join(x) if isinstance(x, list) else 'stock-code-text' in str(x)})
            if not sku_span:
                # Try finding by class only
                sku_span = soup.find('span', class_=lambda x: x and 'stock-code-text' in ' '.join(x) if isinstance(x, list) else 'stock-code-text' in str(x))
            
            if sku_span:
                strong_elem = sku_span.find('strong')
                if strong_elem:
                    part_number = strong_elem.get_text(strip=True)
                else:
                    part_number = sku_span.get_text(strip=True)
                
                if part_number:
                    product_data['sku'] = part_number
                    product_data['pn'] = self.clean_sku(part_number)
                    self.logger.info(f"📦 Found SKU from page: {self.safe_str(product_data['sku'])}")
            
            # Fallback: Pattern from URL: /p/Audi__/Product-Name/ID/PartNumber.html
            if not product_data['sku']:
                url_match = re.search(r'/p/Audi__/[^/]+/\d+/([^/]+)\.html', url)
                if url_match:
                    product_data['sku'] = url_match.group(1)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Fallback: meta itemprop="sku"
            if not product_data['sku']:
                sku_meta = soup.find('meta', {'itemprop': 'sku'})
                if sku_meta:
                    product_data['sku'] = sku_meta.get('content', '').strip()
                    product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Fallback: try other selectors
            if not product_data['sku']:
                sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
                if not sku_elem:
                    sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
                if sku_elem:
                    product_data['sku'] = self.safe_find_text(soup, sku_elem)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Extract replaces (supersessions) - Actual structure: <span class="body-3 alt-stock-code-text"><strong>1J0-601-027-M-03C; 1J0-601-027-M03C; 1J0601027M 03C</strong></span>
            replaces_span = soup.find('span', class_=lambda x: x and 'alt-stock-code-text' in ' '.join(x) if isinstance(x, list) else 'alt-stock-code-text' in str(x))
            if replaces_span:
                strong_elem = replaces_span.find('strong')
                if strong_elem:
                    replaces_text = strong_elem.get_text(strip=True)
                else:
                    replaces_text = replaces_span.get_text(strip=True)
                
                if replaces_text:
                    # Split by semicolon and clean each part
                    replaces_parts = [part.strip() for part in replaces_text.split(';') if part.strip()]
                    if replaces_parts:
                        # Join with comma or keep as is
                        product_data['replaces'] = ', '.join(replaces_parts)
                        self.logger.info(f"🔄 Found replaces: {self.safe_str(product_data['replaces'][:60])}")
            
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
            
            # Extract price - Actual structure: <meta itemprop="price" content="179.50"> or <span class="productPriceSpan money-3">$ 179.50</span>
            # PRIORITY: Extract from meta itemprop="price"
            price_meta = soup.find('meta', {'itemprop': 'price'})
            if price_meta:
                price_content = price_meta.get('content', '').strip()
                if price_content:
                    try:
                        product_data['actual_price'] = float(price_content)
                        self.logger.info(f"💰 Found price from meta: {self.safe_str(product_data['actual_price'])}")
                    except (ValueError, TypeError):
                        pass
            
            # Fallback: <span class="productPriceSpan money-3">$ 179.50</span>
            if not product_data['actual_price']:
                price_span = soup.find('span', class_=lambda x: x and 'productPriceSpan' in ' '.join(x) if isinstance(x, list) else 'productPriceSpan' in str(x))
                if not price_span:
                    price_span = soup.find('span', class_=lambda x: x and 'money-3' in ' '.join(x) if isinstance(x, list) else 'money-3' in str(x))
                
                if price_span:
                    price_text = price_span.get_text(strip=True)
                    if price_text:
                        if '€' in price_text or 'EUR' in price_text.upper():
                            price_value = self.extract_price(price_text)
                            if price_value:
                                product_data['actual_price'] = self.convert_currency(price_value, 'EUR', 'USD')
                                self.logger.info(f"💰 Found price (EUR converted): {self.safe_str(product_data['actual_price'])}")
                        else:
                            price_value = self.extract_price(price_text)
                            if price_value:
                                product_data['actual_price'] = price_value
                                self.logger.info(f"💰 Found price: {self.safe_str(product_data['actual_price'])}")
            
            # Fallback: JSON-LD structured data
            if not product_data['actual_price']:
                json_ld_script = soup.find('script', type='application/ld+json')
                if json_ld_script and json_ld_script.string:
                    try:
                        json_data = json.loads(json_ld_script.string)
                        if isinstance(json_data, dict) and 'offers' in json_data:
                            offers = json_data['offers']
                            if isinstance(offers, dict) and 'price' in offers:
                                try:
                                    product_data['actual_price'] = float(offers['price'])
                                    self.logger.info(f"💰 Found price from JSON-LD: {self.safe_str(product_data['actual_price'])}")
                                except (ValueError, TypeError):
                                    pass
                    except (json.JSONDecodeError, KeyError, AttributeError):
                        pass
            
            # Fallback: try other price selectors
            if not product_data['actual_price']:
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
            
            # Extract MSRP - Actual structure: <div class="col-md-4 col-xs-4 text-right money-strike msrpRow">MSRP: $ 1,196.99</div>
            msrp_elem = soup.find('div', class_=lambda x: x and isinstance(x, list) and 'msrpRow' in ' '.join(x))
            if not msrp_elem:
                msrp_elem = soup.find('div', class_=lambda x: x and 'msrpRow' in str(x) if isinstance(x, str) else False)
            if not msrp_elem:
                # Try finding by text content containing "MSRP:"
                all_divs = soup.find_all('div')
                for div in all_divs:
                    div_text = div.get_text(strip=True)
                    if 'MSRP:' in div_text.upper():
                        msrp_elem = div
                        break
            if not msrp_elem:
                msrp_elem = soup.find('span', class_=re.compile(r'list.*price|msrp', re.I))
            if not msrp_elem:
                msrp_elem = soup.find('del', class_=re.compile(r'price', re.I))
            if msrp_elem:
                msrp_text = self.safe_find_text(soup, msrp_elem)
                # Extract price from text like "MSRP: $ 1,196.99"
                product_data['msrp'] = self.extract_price(msrp_text)
                if product_data['msrp']:
                    self.logger.info(f"💰 Found MSRP: {self.safe_str(product_data['msrp'])}")
            
            # Extract image - Actual structure: <img itemprop="image" src="..." class="img-responsive img-thumbnail">
            # PRIORITY: Extract from itemprop="image"
            img_elem = soup.find('img', {'itemprop': 'image'})
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src')
                if img_url:
                    product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
                    if not product_data['image_url'].startswith('http'):
                        product_data['image_url'] = f"https:{product_data['image_url']}"
            
            # Fallback: JSON-LD structured data
            if not product_data.get('image_url'):
                json_ld_script = soup.find('script', type='application/ld+json')
                if json_ld_script and json_ld_script.string:
                    try:
                        json_data = json.loads(json_ld_script.string)
                        if isinstance(json_data, dict) and 'image' in json_data:
                            images = json_data['image']
                            if isinstance(images, list) and len(images) > 0:
                                product_data['image_url'] = images[0]
                            elif isinstance(images, str):
                                product_data['image_url'] = images
                            if product_data.get('image_url'):
                                self.logger.info(f"🖼️ Found image from JSON-LD: {self.safe_str(product_data['image_url'][:60])}")
                    except (json.JSONDecodeError, KeyError, AttributeError):
                        pass
            
            # Fallback: #part-image-left a > img.img-responsive
            if not product_data.get('image_url'):
                part_image_left = soup.find('div', id='part-image-left')
                if part_image_left:
                    img_link = part_image_left.find('a')
                    if img_link:
                        img_elem = img_link.find('img', class_='img-responsive')
                        if img_elem:
                            img_url = img_elem.get('src') or img_elem.get('data-src')
                            if img_url:
                                product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            
            # Fallback: try other image selectors
            if not product_data.get('image_url'):
                img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
                if not img_elem:
                    img_elem = soup.find('img', id=re.compile(r'product.*image', re.I))
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src')
                    if img_url:
                        product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            
            if product_data.get('image_url') and not product_data['image_url'].startswith('http'):
                product_data['image_url'] = f"https:{product_data['image_url']}"
            
            # Extract description - Actual structure: <div class="item-desc"><p>Rim. Spare wheel. WHEEL Disc. <br>A wheel / Rim of a vehicle...</p><p><b>Fits TT</b>...</p></div>
            # PRIORITY: Extract from <div class="item-desc">
            desc_div = soup.find('div', class_=lambda x: x and 'item-desc' in ' '.join(x) if isinstance(x, list) else 'item-desc' in str(x))
            if desc_div:
                desc_paragraphs = desc_div.find_all('p')
                desc_parts = []
                for p in desc_paragraphs:
                    p_text = p.get_text(strip=True, separator=' ')
                    if p_text:
                        desc_parts.append(p_text)
                
                if desc_parts:
                    desc_text = ' '.join(desc_parts)
                    desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                    if desc_text and len(desc_text) > 10:
                        product_data['description'] = desc_text
                        self.logger.info(f"📄 Found description: {self.safe_str(desc_text[:60])}")
            
            # Fallback: p > strong.custom-blacktext with "Product Description"
            if not product_data['description']:
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
                                self.logger.info(f"📄 Found description: {self.safe_str(desc_text[:60])}")
                                break
            
            # Fallback: meta itemprop="description"
            if not product_data['description']:
                desc_meta = soup.find('meta', {'itemprop': 'description'})
                if desc_meta:
                    desc_text = desc_meta.get('content', '').strip()
                    if desc_text:
                        product_data['description'] = desc_text
                        self.logger.info(f"📄 Found description from meta: {self.safe_str(desc_text[:60])}")
            
            # Fallback: try other description selectors
            if not product_data['description']:
                desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
                if not desc_elem:
                    desc_elem = soup.find('span', class_=re.compile(r'description', re.I))
                if not desc_elem:
                    desc_elem = soup.find('p', class_=re.compile(r'description', re.I))
                if desc_elem:
                    desc_text = self.safe_find_text(soup, desc_elem)
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    product_data['description'] = desc_text.strip()
            
            # Extract fitment - Need to click "What This Fits" tab and "Show More" button to load all fitment data
            # Steps:
            # 1. Click the "What This Fits" tab
            # 2. Wait for tab panel to load
            # 3. Click "Show More" button
            # 4. Wait for all fitment data to load
            # 5. Extract fitment data from updated HTML
            
            fitment_rows = []
            selenium_extraction_success = False  # Initialize flag for Selenium extraction
            
            if self.driver:
                selenium_extraction_success = False  # Reset flag
                try:
                    self.logger.info("🔍 Clicking 'What This Fits' tab to load fitment data...")
                    
                    # Step 1: Find and click the "What This Fits" tab
                    # Structure: <li id="WhatThisFitsTabComponent"><a href="#WhatThisFitsTabComponent">What This Fits</a></li>
                    try:
                        # Wait for tab to be present
                        wait = WebDriverWait(self.driver, 10)
                        
                        # Try multiple selectors for the tab
                        tab_selectors = [
                            (By.ID, 'WhatThisFitsTabComponent_TAB'),
                            (By.CSS_SELECTOR, 'a[href="#WhatThisFitsTabComponent"]'),
                            (By.CSS_SELECTOR, 'li#WhatThisFitsTabComponent a'),
                            (By.XPATH, '//a[contains(text(), "What This Fits")]'),
                        ]
                        
                        tab_clicked = False
                        for selector_type, selector_value in tab_selectors:
                            try:
                                tab_element = wait.until(EC.element_to_be_clickable((selector_type, selector_value)))
                                # Scroll into view
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab_element)
                                time.sleep(0.5)
                                tab_element.click()
                                tab_clicked = True
                                self.logger.info("✓ Clicked 'What This Fits' tab")
                                break
                            except Exception as e:
                                continue
                        
                        if not tab_clicked:
                            self.logger.warning("⚠️ Could not find or click 'What This Fits' tab")
                        else:
                            # Step 2: Wait for tab panel to be visible and loaded
                            time.sleep(1)  # Initial wait for tab panel to appear
                            
                            # Wait for tab panel to be visible
                            try:
                                tab_panel = wait.until(EC.presence_of_element_located((By.ID, 'WhatThisFitsTabComponent_TABPANEL')))
                                # Wait a bit more for content to render
                                time.sleep(1)
                                self.logger.info("✓ Tab panel loaded")
                            except Exception as e:
                                self.logger.warning(f"⚠️ Tab panel not found: {str(e)}")
                            
                            # Step 3: Find and click "Show More" button
                            # Structure: <button class="btn btn-link showMoreBtnLink">Show More</button>
                            try:
                                show_more_selectors = [
                                    (By.CSS_SELECTOR, 'button.showMoreBtnLink'),
                                    (By.CSS_SELECTOR, 'button.btn-link.showMoreBtnLink'),
                                    (By.XPATH, '//button[contains(text(), "Show More")]'),
                                    (By.XPATH, '//button[contains(@class, "showMore")]'),
                                ]
                                
                                show_more_clicked = False
                                for selector_type, selector_value in show_more_selectors:
                                    try:
                                        show_more_button = wait.until(EC.element_to_be_clickable((selector_type, selector_value)))
                                        # Scroll into view
                                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", show_more_button)
                                        time.sleep(0.5)
                                        show_more_button.click()
                                        show_more_clicked = True
                                        self.logger.info("✓ Clicked 'Show More' button")
                                        break
                                    except Exception as e:
                                        continue
                                
                                if not show_more_clicked:
                                    self.logger.warning("⚠️ Could not find or click 'Show More' button (may not be present)")
                                
                                # Step 4: Wait for all fitment data to load
                                # Wait longer for dynamic content to load
                                time.sleep(2)
                                
                                # Wait for fitment rows to appear - try multiple selectors
                                fitment_loaded = False
                                fitment_selectors = [
                                    'div.whatThisFitsFitment',
                                    'div.whatThisFitsFitment span',
                                    'div.col-lg-12 .whatThisFitsFitment',
                                    'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
                                    'div.whatThisFitsYears',
                                ]
                                
                                for selector in fitment_selectors:
                                    try:
                                        # Use longer timeout for dynamic content
                                        extended_wait = WebDriverWait(self.driver, 15)
                                        extended_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                                        fitment_loaded = True
                                        self.logger.info(f"✓ Fitment data loaded (found via: {selector})")
                                        break
                                    except Exception as e:
                                        continue
                                
                                if not fitment_loaded:
                                    # Try waiting for any content in the tab panel
                                    try:
                                        extended_wait = WebDriverWait(self.driver, 10)
                                        extended_wait.until(lambda driver: len(driver.find_elements(By.CSS_SELECTOR, 'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12')) > 0)
                                        fitment_loaded = True
                                        self.logger.info("✓ Fitment data loaded (found via row count check)")
                                    except Exception as e:
                                        # Try JavaScript check as fallback
                                        try:
                                            row_count = self.driver.execute_script(
                                                "return document.querySelectorAll('div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12').length;"
                                            )
                                            if row_count > 0:
                                                fitment_loaded = True
                                                self.logger.info(f"✓ Fitment data loaded (found {row_count} rows via JavaScript)")
                                            else:
                                                self.logger.warning(f"⚠️ Fitment rows not found via JavaScript (count: {row_count}), but continuing anyway")
                                        except Exception as js_error:
                                            self.logger.warning(f"⚠️ Fitment rows check failed, but continuing anyway: {str(js_error)}")
                                
                                # Additional wait for all content to fully render
                                time.sleep(2)
                                
                                # Final check: verify elements exist before extraction
                                try:
                                    final_check = self.driver.execute_script(
                                        "return document.querySelectorAll('div.whatThisFitsFitment, div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12').length;"
                                    )
                                    if final_check > 0:
                                        self.logger.info(f"✓ Verified {final_check} fitment elements exist before extraction")
                                    else:
                                        self.logger.warning(f"⚠️ No fitment elements found in final check (count: {final_check})")
                                except Exception as e:
                                    self.logger.debug(f"Final check error (non-critical): {str(e)}")
                                
                            except Exception as e:
                                self.logger.warning(f"⚠️ Error clicking 'Show More': {str(e)}")
                            
                            # Step 5: Extract fitment data directly using Selenium (more reliable for dynamic content)
                            # Use Selenium to find elements directly instead of BeautifulSoup
                            try:
                                # Find all fitment rows using Selenium
                                fitment_row_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12')
                                
                                if not fitment_row_elements:
                                    # Try alternative selector
                                    fitment_row_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.whatThisFitsFitment')
                                
                                if fitment_row_elements:
                                    self.logger.info(f"✓ Found {len(fitment_row_elements)} fitment rows via Selenium")
                                    
                                    # Extract data from each row using Selenium
                                    for row_element in fitment_row_elements:
                                        try:
                                            # Get the row's HTML and parse it
                                            row_html = row_element.get_attribute('outerHTML')
                                            if not row_html:
                                                continue
                                            
                                            row_soup = BeautifulSoup(row_html, 'lxml')
                                            
                                            # Find the vehicle description
                                            fitment_div = row_soup.find('div', class_=lambda x: x and ('whatThisFitsFitment' in str(x) if x else False))
                                            if not fitment_div:
                                                # Try finding by any div with the class
                                                fitment_div = row_soup.find('div', class_=re.compile(r'whatThisFitsFitment', re.I))
                                            
                                            if not fitment_div:
                                                continue
                                            
                                            vehicle_span = fitment_div.find('span')
                                            if not vehicle_span:
                                                continue
                                            
                                            vehicle_text = vehicle_span.get_text(strip=True)
                                            if not vehicle_text:
                                                continue
                                            
                                            # Find years using Selenium directly from the row element
                                            years = []
                                            try:
                                                years_div_element = row_element.find_element(By.CSS_SELECTOR, 'div.whatThisFitsYears')
                                                year_links = years_div_element.find_elements(By.TAG_NAME, 'a')
                                                
                                                for link in year_links:
                                                    href = link.get_attribute('href') or ''
                                                    year_match = re.search(r'/p/Audi_(\d{4})', href)
                                                    if year_match:
                                                        years.append(year_match.group(1))
                                                    else:
                                                        # Try text content
                                                        link_text = link.text.strip()
                                                        if link_text and link_text.isdigit() and len(link_text) == 4:
                                                            years.append(link_text)
                                                
                                                # If no links, try text content
                                                if not years:
                                                    years_text = years_div_element.text
                                                    year_matches = re.findall(r'\b(\d{4})\b', years_text)
                                                    years = [y for y in year_matches if 1900 <= int(y) <= 2100]
                                            except Exception as year_error:
                                                self.logger.debug(f"Error extracting years: {str(year_error)}")
                                            
                                            # Parse vehicle description
                                            make = 'Audi'
                                            words = vehicle_text.split()
                                            if len(words) >= 2:
                                                if words[0].lower() == 'audi' and len(words) >= 2:
                                                    model = words[1]  # "A5"
                                                else:
                                                    model = f"{words[0]} {words[1]}"
                                            else:
                                                model = vehicle_text.replace('Audi', '').strip() if 'Audi' in vehicle_text else vehicle_text
                                            
                                            # Extract engine and trim
                                            parse_text = vehicle_text
                                            if parse_text.startswith('Audi '):
                                                parse_text = parse_text[5:].strip()
                                            
                                            engine_match = re.search(r'(\d+\.?\d*L)', parse_text)
                                            if engine_match:
                                                engine_start = engine_match.start()
                                                transmission_pattern = r'\s+(A/T|M/T|CVT|AUTO|MANUAL)\s+'
                                                transmission_match = re.search(transmission_pattern, parse_text[engine_start:])
                                                
                                                if transmission_match:
                                                    engine_end = engine_start + transmission_match.end()
                                                    engine = parse_text[engine_start:engine_end].strip()
                                                    trim = parse_text[engine_end:].strip()
                                                else:
                                                    trim_keywords = r'(Premium|Prestige|Base|Sport|S-Line|Quattro|SE|LE|Limited|Edition|Hatchback|Sedan|SUV|Coupe|Convertible|Wagon|Cabriolet)'
                                                    trim_match = re.search(r'\s+' + trim_keywords, parse_text[engine_start:], re.IGNORECASE)
                                                    if trim_match:
                                                        engine_end = engine_start + trim_match.start()
                                                        engine = parse_text[engine_start:engine_end].strip()
                                                        trim = parse_text[engine_end:].strip()
                                                    else:
                                                        engine = parse_text[engine_start:].strip()
                                                        trim = ''
                                            else:
                                                transmission_match = re.search(r'\s+(A/T|M/T|CVT|AUTO|MANUAL)\s+', parse_text)
                                                if transmission_match:
                                                    trim = parse_text[transmission_match.end():].strip()
                                                    before_transmission = parse_text[:transmission_match.start()].strip()
                                                    parts = before_transmission.split(' ', 1)
                                                    engine = parts[1].strip() if len(parts) >= 2 else ''
                                                else:
                                                    trim_keywords = r'(Premium|Prestige|Base|Sport|S-Line|Quattro|SE|LE|Limited|Edition|Hatchback|Sedan|SUV|Coupe|Convertible|Wagon|Cabriolet)'
                                                    trim_match = re.search(r'\s+' + trim_keywords, parse_text, re.IGNORECASE)
                                                    if trim_match:
                                                        trim = parse_text[trim_match.start():].strip()
                                                        before_trim = parse_text[:trim_match.start()].strip()
                                                        parts = before_trim.split(' ', 1)
                                                        engine = parts[1].strip() if len(parts) >= 2 else ''
                                                    else:
                                                        engine = ''
                                                        trim = ''
                                            
                                            # Create fitment entries for each year
                                            if years:
                                                for year in years:
                                                    product_data['fitments'].append({
                                                        'year': year,
                                                        'make': make,
                                                        'model': model,
                                                        'trim': trim,
                                                        'engine': engine
                                                    })
                                                self.logger.info(f"🚗 Found {len(years)} fitment(s): {model} ({', '.join(years)})")
                                            else:
                                                product_data['fitments'].append({
                                                    'year': '',
                                                    'make': make,
                                                    'model': model,
                                                    'trim': trim,
                                                    'engine': engine
                                                })
                                        
                                        except Exception as row_error:
                                            self.logger.debug(f"Error processing fitment row: {str(row_error)}")
                                            continue
                                    
                                    # Mark that we found fitment rows (Selenium extraction successful)
                                    selenium_extraction_success = True
                                
                            except Exception as selenium_error:
                                self.logger.warning(f"⚠️ Error extracting fitment via Selenium: {str(selenium_error)}")
                                selenium_extraction_success = False
                                # Fallback to BeautifulSoup
                                html = self.driver.page_source
                                soup = BeautifulSoup(html, 'lxml')
                                fitment_rows = []
                            
                            # Fallback: Extract fitment data from HTML if Selenium extraction didn't work
                            if not selenium_extraction_success:
                                html = self.driver.page_source
                                soup = BeautifulSoup(html, 'lxml')
                                
                                # Method 1: Extract from "What This Fits" tab panel
                                fitment_tab = soup.find('div', id='WhatThisFitsTabComponent_TABPANEL')
                                if fitment_tab:
                                    fitment_rows = fitment_tab.find_all('div', class_=lambda x: x and isinstance(x, list) and 'col-lg-12' in x)
                                    if fitment_rows:
                                        self.logger.info(f"✓ Found {len(fitment_rows)} fitment rows in tab panel (fallback)")
                                
                                # Method 2: Try finding rows by whatThisFitsFitment class directly
                                if not fitment_rows:
                                    fitment_tab = soup.find('div', id='WhatThisFitsTabComponent_TABPANEL')
                                    if fitment_tab:
                                        fitment_divs = fitment_tab.find_all('div', class_=lambda x: x and isinstance(x, list) and 'whatThisFitsFitment' in ' '.join(x))
                                        if fitment_divs:
                                            fitment_rows = []
                                            for fitment_div in fitment_divs:
                                                parent_row = fitment_div.find_parent('div', class_=lambda x: x and isinstance(x, list) and 'col-lg-12' in x)
                                                if parent_row and parent_row not in fitment_rows:
                                                    fitment_rows.append(parent_row)
                                            if fitment_rows:
                                                self.logger.info(f"✓ Found {len(fitment_rows)} fitment rows via whatThisFitsFitment class (fallback)")
                            
                            if not fitment_rows:
                                self.logger.warning("⚠️ No fitment rows found after clicking Show More")
                            
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error interacting with fitment tab: {str(e)}")
                        # Ensure soup is available for fallback extraction
                        if 'soup' not in locals():
                            html = self.driver.page_source if self.driver else ''
                            soup = BeautifulSoup(html, 'lxml') if html else soup
                        fitment_rows = []
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Error extracting fitment data: {str(e)}")
                    import traceback
                    self.logger.debug(traceback.format_exc())
            
            # Check if Selenium extraction was successful
            selenium_extraction_success = 'selenium_extraction_success' in locals() and selenium_extraction_success
            
            # Only process fitment_rows if they were extracted via BeautifulSoup (not Selenium)
            # Selenium extraction already populated product_data['fitments']
            if selenium_extraction_success:
                # Selenium extraction already populated fitments, skip BeautifulSoup processing
                self.logger.info(f"✓ Fitment data already extracted via Selenium ({len(product_data['fitments'])} entries)")
            elif fitment_rows and isinstance(fitment_rows, list) and len(fitment_rows) > 0:
                self.logger.info(f"🔍 Processing {len(fitment_rows)} fitment rows via BeautifulSoup...")
                for idx, row in enumerate(fitment_rows):
                    # Find the vehicle description span
                    fitment_div = row.find('div', class_=lambda x: x and isinstance(x, list) and 'whatThisFitsFitment' in ' '.join(x))
                    if not fitment_div:
                        # Try finding by class string
                        fitment_div = row.find('div', class_=lambda x: x and 'whatThisFitsFitment' in str(x) if isinstance(x, str) else False)
                    if not fitment_div:
                        # Try finding any div with whatThisFitsFitment in class
                        fitment_div = row.find('div', class_=re.compile(r'whatThisFitsFitment', re.I))
                    if not fitment_div:
                        self.logger.debug(f"⚠️ Row {idx+1}: No fitment div found, skipping")
                        continue
                    
                    vehicle_span = fitment_div.find('span')
                    if not vehicle_span:
                        continue
                    
                    vehicle_text = vehicle_span.get_text(strip=True)
                    if not vehicle_text:
                        continue
                    
                    # Parse vehicle description: "Audi A5 2.0L MILD HYBRID EV-GAS (MHEV) A/T Quattro Premium Convertible"
                    # Make: Always "Audi"
                    make = 'Audi'
                    
                    # Extract model: "Audi A5" (first two words, removing "Audi" prefix)
                    words = vehicle_text.split()
                    if len(words) >= 2:
                        # Skip "Audi" if present, take next word as model
                        if words[0].lower() == 'audi' and len(words) >= 2:
                            model = words[1]  # "A5"
                        else:
                            model = f"{words[0]} {words[1]}"  # "Audi S7"
                    else:
                        model = vehicle_text.replace('Audi', '').strip() if 'Audi' in vehicle_text else vehicle_text
                    
                    # Extract engine and trim
                    # Pattern: "Audi A5 2.0L MILD HYBRID EV-GAS (MHEV) A/T Quattro Premium Convertible"
                    # Model: "A5" (already extracted above)
                    # Engine: "2.0L MILD HYBRID EV-GAS (MHEV) A/T" (from engine size to transmission)
                    # Trim: "Quattro Premium Convertible" (after transmission)
                    
                    # Remove "Audi" prefix if present for easier parsing
                    parse_text = vehicle_text
                    if parse_text.startswith('Audi '):
                        parse_text = parse_text[5:].strip()  # Remove "Audi "
                    
                    # Find engine size pattern (e.g., "2.0L", "2.9L", "3.0L")
                    engine_match = re.search(r'(\d+\.?\d*L)', parse_text)
                    if engine_match:
                        engine_start = engine_match.start()
                        
                        # Find transmission pattern (A/T, M/T, CVT, AUTO, MANUAL)
                        # Look for transmission after engine start
                        transmission_pattern = r'\s+(A/T|M/T|CVT|AUTO|MANUAL)\s+'
                        transmission_match = re.search(transmission_pattern, parse_text[engine_start:])
                        
                        if transmission_match:
                            # Engine includes everything from engine size to transmission (inclusive)
                            engine_end = engine_start + transmission_match.end()
                            engine = parse_text[engine_start:engine_end].strip()
                            # Trim is everything after transmission
                            trim = parse_text[engine_end:].strip()
                        else:
                            # No transmission found, try to find trim by looking for common trim keywords
                            # Common trim patterns: Premium, Prestige, Base, Sport, Quattro, etc.
                            trim_keywords = r'(Premium|Prestige|Base|Sport|S-Line|Quattro|SE|LE|Limited|Edition|Hatchback|Sedan|SUV|Coupe|Convertible|Wagon|Cabriolet)'
                            trim_match = re.search(r'\s+' + trim_keywords, parse_text[engine_start:], re.IGNORECASE)
                            if trim_match:
                                engine_end = engine_start + trim_match.start()
                                engine = parse_text[engine_start:engine_end].strip()
                                trim = parse_text[engine_end:].strip()
                            else:
                                # No trim pattern found, engine is from engine size to end
                                engine = parse_text[engine_start:].strip()
                                trim = ''
                    else:
                        # No engine size pattern found
                        # Try to find transmission to split model/engine from trim
                        transmission_match = re.search(r'\s+(A/T|M/T|CVT|AUTO|MANUAL)\s+', parse_text)
                        if transmission_match:
                            # Everything after transmission is trim
                            trim = parse_text[transmission_match.end():].strip()
                            # Everything before transmission might contain engine info
                            before_transmission = parse_text[:transmission_match.start()].strip()
                            # Remove model name (first word) to get engine
                            parts = before_transmission.split(' ', 1)
                            if len(parts) >= 2:
                                engine = parts[1].strip()
                            else:
                                engine = ''
                        else:
                            # No clear structure, try to find trim keywords
                            trim_keywords = r'(Premium|Prestige|Base|Sport|S-Line|Quattro|SE|LE|Limited|Edition|Hatchback|Sedan|SUV|Coupe|Convertible|Wagon|Cabriolet)'
                            trim_match = re.search(r'\s+' + trim_keywords, parse_text, re.IGNORECASE)
                            if trim_match:
                                trim = parse_text[trim_match.start():].strip()
                                # Everything before trim might be engine
                                before_trim = parse_text[:trim_match.start()].strip()
                                parts = before_trim.split(' ', 1)
                                if len(parts) >= 2:
                                    engine = parts[1].strip()
                                else:
                                    engine = ''
                            else:
                                # No clear structure, no engine or trim
                                engine = ''
                                trim = ''
                    
                    # Find the years div
                    years_div = row.find('div', class_=lambda x: x and isinstance(x, list) and 'whatThisFitsYears' in ' '.join(x))
                    if not years_div:
                        # Try finding by class string
                        years_div = row.find('div', class_=lambda x: x and 'whatThisFitsYears' in str(x) if isinstance(x, str) else False)
                    if not years_div:
                        # Try finding by regex
                        years_div = row.find('div', class_=re.compile(r'whatThisFitsYears', re.I))
                    
                    if years_div:
                        # Find all year links - they can be in <a> tags or just text
                        year_links = years_div.find_all('a', href=True)
                        years = []
                        
                        # First try extracting from href URLs
                        for link in year_links:
                            href = link.get('href', '')
                            # Pattern: /p/Audi_2022_... or /p/Audi_2022_A5-...
                            year_match = re.search(r'/p/Audi_(\d{4})', href)
                            if year_match:
                                years.append(year_match.group(1))
                            else:
                                # Fallback: extract year from link text
                                link_text = link.get_text(strip=True)
                                if link_text and link_text.isdigit() and len(link_text) == 4:
                                    years.append(link_text)
                        
                        # If no years from links, try extracting from text content
                        if not years:
                            years_span = years_div.find('span')
                            if years_span:
                                years_text = years_span.get_text(strip=True)
                                # Extract years from text like "2022, 2023" or "2022,2023"
                                year_matches = re.findall(r'\b(\d{4})\b', years_text)
                                years = [y for y in year_matches if 1900 <= int(y) <= 2100]
                        
                        # Create a fitment entry for each year
                        if years:
                            for year in years:
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': make,
                                    'model': model,
                                    'trim': trim,
                                    'engine': engine
                                })
                            self.logger.info(f"🚗 Found {len(years)} fitment(s): {model} ({', '.join(years)})")
                        else:
                            # No years found, add one entry without year
                            product_data['fitments'].append({
                                'year': '',
                                'make': make,
                                'model': model,
                                'trim': trim,
                                'engine': engine
                            })
                    else:
                        # No years div found, add one entry without year
                        product_data['fitments'].append({
                            'year': '',
                            'make': make,
                            'model': model,
                            'trim': trim,
                            'engine': engine
                        })
            
            # Method 3: Extract from guided navigation year links
            if not product_data['fitments']:
                guided_nav = soup.find('div', class_=lambda x: x and 'guided-nav' in ' '.join(x) if isinstance(x, list) else 'guided-nav' in str(x))
                if guided_nav:
                    year_links = guided_nav.find_all('a', href=re.compile(r'/p/Audi_\d+_/'))
                    for link in year_links:
                        href = link.get('href', '')
                        year_match = re.search(r'/p/Audi_(\d{4})_/', href)
                        if year_match:
                            year = year_match.group(1)
                            link_text = link.get_text(strip=True)
                            model = link_text if link_text else 'Audi'
                            product_data['fitments'].append({
                                'year': year,
                                'make': 'Audi',
                                'model': model,
                                'trim': '',
                                'engine': ''
                            })
            
            # Method 4: Look for fitment table in <div id="fitment" class="tab-pane p-xs active">
            if not product_data['fitments']:
                fitment_div = soup.find('div', id='fitment')
                if not fitment_div:
                    # Also try with class "tab-pane p-xs active"
                    fitment_divs = soup.find_all('div', class_=lambda x: x and isinstance(x, list))
                    for div in fitment_divs:
                        classes = ' '.join(div.get('class', [])).lower()
                        if 'tab-pane' in classes and 'p-xs' in classes and 'active' in classes:
                            fitment_div = div
                            break
                
                if fitment_div:
                    fitment_table = fitment_div.find('table')
                    if fitment_table:
                        rows = fitment_table.find_all('tr')
                        for row in rows[1:]:  # Skip header
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 1:
                                first_cell = cells[0]  # Only use first cell
                                cell_text = first_cell.get_text(strip=True)
                                
                                # Example: "Range Rover Evoque (2012-2018)" or "Model (Year-Year) [Engine]"
                                # Extract engine from brackets if present
                                engine_match = re.search(r'\[([^\]]+)\]', cell_text)
                                engine = engine_match.group(1).strip() if engine_match else ''
                                
                                # Remove engine brackets from model text
                                model_text = re.sub(r'\s*\[[^\]]+\]', '', cell_text).strip()
                                
                                # Extract year range: "(2012-2018)" - use first year
                                year_range_match = re.search(r'\((\d{4})-(\d{4})\)', model_text)
                                if year_range_match:
                                    year = year_range_match.group(1)  # First year: 2012
                                    model = model_text.strip()  # Keep full model: "Range Rover Evoque (2012-2018)"
                                else:
                                    year = ''
                                    model = model_text.strip()
                                
                                # Extract make from model (first word)
                                make = ''
                                model_without_year = re.sub(r'\s*\(\d{4}-\d{4}\)', '', model).strip()
                                model_words = model_without_year.split()
                                if len(model_words) >= 1:
                                    make = model_words[0]
                                
                                # Only add if we have a model
                                if model:
                                    product_data['fitments'].append({
                                        'year': year,
                                        'make': make,
                                        'model': model,
                                        'trim': '',
                                        'engine': engine
                                    })
                                    self.logger.debug(f"Extracted fitment: {model} (year={year}, make={make}, engine={engine})")
            
            # Method 5: Extract from description text (e.g., "Fits TT")
            if not product_data['fitments'] and product_data.get('description'):
                # Look for "Fits" pattern in description
                fits_match = re.search(r'Fits\s+([A-Za-z0-9\s]+)', product_data['description'], re.IGNORECASE)
                if fits_match:
                    model_text = fits_match.group(1).strip()
                    if model_text:
                        product_data['fitments'].append({
                            'year': '',
                            'make': 'Audi',
                            'model': model_text,
                            'trim': '',
                            'engine': ''
                        })
            
            # Fallback: try JSON script tag
            if not product_data['fitments']:
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

