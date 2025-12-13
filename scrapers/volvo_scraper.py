"""Scraper for usparts.volvocars.com (Volvo parts)"""
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

class VolvoScraper(BaseScraperWithExtension):
    """Scraper for usparts.volvocars.com"""
    
    def __init__(self):
        super().__init__('volvo', use_selenium=True)
        self.base_url = 'https://usparts.volvocars.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from usparts.volvocars.com (single search page, no pagination)"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products from single search page...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            product_urls = list(set(product_urls))
            self.logger.info(f"Found {len(product_urls)} unique wheel product URLs from search page")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """Search for wheels using the specific search URL (no pagination)"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Use the specific search URL (no pagination, single page only)
            search_url = f"{self.base_url}/productSearch.aspx?ukey_make=5772&modelYear=0&ukey_model=0&ukey_trimLevel=0&ukey_driveline=0&ukey_Category=0&numResults=250&sortOrder=Relevance&ukey_tag=0&isOnSale=0&isAccessory=0&isPerformance=0&showAllModels=1&searchTerm=wheel"
            
            self.logger.info(f"Searching: {search_url}")
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(search_url, use_selenium=True, wait_time=2)
                if not html or len(html) < 5000:
                    self.logger.error("Could not load search page")
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
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/'], a[href*='/product/'], div[class*='product'], article[class*='product']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products (lazy-loaded content)
            self._scroll_to_load_content()
            time.sleep(2)  # Additional wait for dynamic content
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract products from the page
            page_count = self._extract_products_from_page(soup, product_urls)
            self.logger.info(f"Found {page_count} unique product URLs from search page (Total: {len(product_urls)})")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _extract_products_from_page(self, soup, product_urls):
        """Extract product URLs from a search results page"""
        initial_count = len(product_urls)
        
        # Strategy 1: SimplePart pattern - /p/Volvo__/Product-Name/ID/PartNumber.html
        # This is the primary pattern for Volvo site (if using SimplePart platform)
        simplepart_links = soup.find_all('a', href=re.compile(r'/p/Volvo__/', re.I))
        for link in simplepart_links:
            href = link.get('href', '')
            if href:
                full_url = self._normalize_product_url(href)
                if full_url and full_url not in product_urls:
                    product_urls.append(full_url)
        
        # Strategy 2: Generic SimplePart pattern - /p/Brand__/Product-Name/ID/PartNumber.html
        if len(product_urls) == initial_count:
            simplepart_links = soup.find_all('a', href=re.compile(r'/p/[^/]+__/[^/]+/\d+/[^/]+\.html', re.I))
            for link in simplepart_links:
                href = link.get('href', '')
                if href:
                    full_url = self._normalize_product_url(href)
                    if full_url and full_url not in product_urls:
                        product_urls.append(full_url)
        
        # Strategy 3: Look for product containers/rows (common in e-commerce sites)
        if len(product_urls) == initial_count:
            product_containers = soup.find_all(['div', 'article', 'li'], class_=re.compile(r'product|item', re.I))
            for container in product_containers:
                link = container.find('a', href=re.compile(r'/p/|/product/|/parts/|/oem-parts/', re.I))
                if link:
                    href = link.get('href', '')
                    if href:
                        full_url = self._normalize_product_url(href)
                        if full_url and full_url not in product_urls:
                            product_urls.append(full_url)
        
        # Strategy 4: Direct link search for product URLs (all patterns)
        if len(product_urls) == initial_count:
            product_links = (soup.find_all('a', href=re.compile(r'/p/[^/]+__/', re.I)) +
                           soup.find_all('a', href=re.compile(r'/product/')) +
                           soup.find_all('a', href=re.compile(r'/parts/')) +
                           soup.find_all('a', href=re.compile(r'/oem-parts/')))
            
            for link in product_links:
                href = link.get('href', '')
                if href:
                    full_url = self._normalize_product_url(href)
                    if full_url and full_url not in product_urls:
                        product_urls.append(full_url)
        
        # Strategy 5: Selenium direct element finding (for dynamically loaded content)
        if len(product_urls) == initial_count and self.driver:
            try:
                # Find all links with SimplePart pattern
                selenium_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/p/Volvo__/'], a[href*='/p/'], a[href*='/product/']")
                for elem in selenium_links:
                    try:
                        href = elem.get_attribute('href')
                        if href:
                            full_url = self._normalize_product_url(href)
                            if full_url and full_url not in product_urls:
                                product_urls.append(full_url)
                    except:
                        continue
            except Exception as e:
                self.logger.debug(f"Selenium element finding failed: {str(e)}")
        
        # Strategy 6: JavaScript execution fallback (for dynamically loaded content)
        if len(product_urls) == initial_count and self.driver:
            try:
                # Execute JavaScript to find all product links
                js_links = self.driver.execute_script("""
                    var links = [];
                    var allLinks = document.querySelectorAll('a[href*="/p/Volvo__/"], a[href*="/p/"], a[href*="/product/"], a[href*="/parts/"], a[href*="/oem-parts/"]');
                    for (var i = 0; i < allLinks.length; i++) {
                        var href = allLinks[i].getAttribute('href');
                        if (href && (href.includes('/p/Volvo__/') || href.match(/\\/p\\/[^\\/]+__\\/[^\\/]+\\/\\d+\\/[^\\/]+\\.html/) || href.includes('/product/') || href.includes('/parts/'))) {
                            links.push(href);
                        }
                    }
                    return links;
                """)
                
                for href in js_links:
                    if href:
                        full_url = self._normalize_product_url(href)
                        if full_url and full_url not in product_urls:
                            product_urls.append(full_url)
            except Exception as e:
                self.logger.debug(f"JavaScript fallback failed: {str(e)}")
        
        page_count = len(product_urls) - initial_count
        return page_count
    
    def _normalize_product_url(self, href):
        """Normalize and validate a product URL"""
        if not href:
            return None
        
        # Convert relative URLs to absolute
        if href.startswith('http'):
            full_url = href
        else:
            full_url = f"{self.base_url}{href}" if href.startswith('/') else f"{self.base_url}/{href}"
        
        # Remove query parameters and fragments
        if '?' in full_url:
            full_url = full_url.split('?')[0]
        if '#' in full_url:
            full_url = full_url.split('#')[0]
        
        full_url = full_url.rstrip('/')
        
        # Filter out non-product pages
        # Accept URLs with /product/, /parts/, /oem-parts/, or SimplePart pattern /p/Brand__/
        if not any(pattern in full_url for pattern in ['/product/', '/parts/', '/oem-parts/', '/p/']):
            return None
        
        # For SimplePart URLs (/p/Brand__/Product-Name/ID/PartNumber.html), validate structure
        if '/p/' in full_url:
            # SimplePart pattern: /p/Volvo__/Wheel/ID/PartNumber.html
            # Should have at least 5 path segments: p, Brand__, Product-Name, ID, PartNumber.html
            url_parts = [p for p in full_url.split('/') if p]
            if len(url_parts) < 5:
                return None
            # Must end with .html
            if not full_url.endswith('.html'):
                return None
            # Must contain Brand__ pattern
            if not re.search(r'/p/[^/]+__/', full_url):
                return None
        
        # Filter out category/listing pages - only individual products
        # For non-SimplePart URLs, reject URLs that look like category pages
        if '/p/' not in full_url and full_url.count('/') < 4:
            return None
        
        return full_url
    
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
    
    def _wait_for_element_fully_loaded(self, selector, timeout=15, check_stable=True):
        """Wait for element to be fully loaded and stable (no changes for 1 second)"""
        wait = WebDriverWait(self.driver, timeout)
        try:
            element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            
            if check_stable:
                # Wait for element to be stable (no DOM changes)
                stable_count = 0
                last_html = element.get_attribute('outerHTML')
                for _ in range(10):  # Check for up to 1 second
                    time.sleep(0.1)
                    current_html = element.get_attribute('outerHTML')
                    if current_html == last_html:
                        stable_count += 1
                        if stable_count >= 3:  # Stable for 0.3 seconds
                            break
                    else:
                        stable_count = 0
                        last_html = current_html
            
            # Additional wait for any JavaScript to finish
            time.sleep(0.5)
            return element
        except TimeoutException:
            return None
    
    def _wait_for_tab_panel_loaded(self, timeout=30):
        """
        Wait for tab panel to be fully loaded after clicking tab.
        Ensures ALL data is loaded, not just the panel container.
        Returns True if panel is fully loaded, False otherwise.
        """
        if not self.driver:
            return False
        
        wait = WebDriverWait(self.driver, timeout)
        selectors = [
            'div#fitments.tab-pane.active',
            'div#ctl00_Content_PageBody_ProductTabsLegacy_UpdatePanel_applications',
            'div[id*="fitment"][class*="active"]',
            'div.tab-pane.active',
            'div#WhatThisFitsTabComponent_TABPANEL',
        ]
        
        for selector in selectors:
            try:
                # Step 1: Wait for panel element to be present and visible
                element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                wait.until(EC.visibility_of(element))
                
                # Step 2: Wait for loading indicators to disappear
                self.logger.debug("Waiting for loading indicators to disappear...")
                for _ in range(20):  # Wait up to 4 seconds for loading to complete
                    time.sleep(0.2)
                    try:
                        loading_indicators = self.driver.find_elements(By.CSS_SELECTOR, 
                            'div.loading, div.spinner, div[class*="loading"], div[class*="spinner"], '
                            'img[src*="loading"], img[src*="spinner"], .ajax-loader, .loading-overlay')
                        visible_loading = [ind for ind in loading_indicators if ind.is_displayed()]
                        if not visible_loading:
                            break
                    except:
                        break
                
                # Step 3: Wait for content to be stable (not changing)
                self.logger.debug("Waiting for content to stabilize...")
                stable_count = 0
                last_inner_html = ''
                last_content_length = 0
                
                for check_round in range(15):  # Check for up to 3 seconds
                    time.sleep(0.2)
                    try:
                        current_inner_html = element.get_attribute('innerHTML') or ''
                        current_length = len(current_inner_html.strip())
                        
                        if current_inner_html == last_inner_html and current_length == last_content_length:
                            stable_count += 1
                            if stable_count >= 3:  # Stable for 0.6 seconds
                                break
                        else:
                            stable_count = 0
                            last_inner_html = current_inner_html
                            last_content_length = current_length
                    except:
                        break
                
                # Step 4: Verify that actual content exists
                inner_html = element.get_attribute('innerHTML') or ''
                if len(inner_html.strip()) < 100:
                    self.logger.debug(f"Tab panel content too short ({len(inner_html.strip())} chars), continuing check...")
                    continue
                
                # Step 5: Check for actual fitment data elements
                try:
                    content_selectors = [
                        'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr',
                        'div.whatThisFitsFitment',
                        'div.col-lg-12',
                        'table tbody tr',
                        'div[class*="fitment"]',
                        'div[class*="application"]',
                    ]
                    
                    has_content = False
                    for content_selector in content_selectors:
                        try:
                            full_selector = f"{selector} {content_selector}"
                            content_elements = self.driver.find_elements(By.CSS_SELECTOR, full_selector)
                            if content_elements:
                                for elem in content_elements[:5]:
                                    try:
                                        text = elem.text.strip()
                                        if text and len(text) > 10:
                                            has_content = True
                                            break
                                    except:
                                        continue
                                if has_content:
                                    break
                        except:
                            continue
                    
                    if not has_content:
                        try:
                            has_content_js = self.driver.execute_script(f"""
                                var panel = document.querySelector('{selector}');
                                if (!panel) return false;
                                var text = panel.innerText || panel.textContent || '';
                                return text.trim().length > 50;
                            """)
                            if not has_content_js:
                                self.logger.debug("Tab panel exists but no content found yet, continuing...")
                                continue
                        except:
                            pass
                except:
                    pass
                
                # Step 6: Final wait for any remaining async operations
                time.sleep(1)
                
                # Step 7: Final verification
                final_inner_html = element.get_attribute('innerHTML') or ''
                if len(final_inner_html.strip()) > 100:
                    self.logger.info(f"✓ Tab panel fully loaded with {len(final_inner_html.strip())} chars of content")
                    return True
                
            except Exception as e:
                self.logger.debug(f"Tab panel selector {selector} failed: {str(e)}")
                continue
        
        self.logger.warning("⚠️ Tab panel not fully loaded after timeout")
        return False
    
    def _find_and_click_show_more(self, max_attempts=5, wait_between_attempts=2):
        """
        Find and click 'Show More' button with multiple attempts and better detection.
        Returns True if button was found and clicked, False otherwise.
        """
        if not self.driver:
            return False
        
        wait = WebDriverWait(self.driver, 15)
        show_more_selectors = [
            (By.ID, 'ctl00_Content_PageBody_ProductTabsLegacy_showAllApplications'),
            (By.CSS_SELECTOR, 'a.showMoreBtnLink'),
            (By.CSS_SELECTOR, 'a.btn-link.showMoreBtnLink'),
            (By.CSS_SELECTOR, 'button.showMoreBtnLink'),
            (By.CSS_SELECTOR, 'button.btn-link.showMoreBtnLink'),
            (By.XPATH, '//a[contains(text(), "Show More")]'),
            (By.XPATH, '//button[contains(text(), "Show More")]'),
            (By.XPATH, '//a[contains(@class, "showMore")]'),
            (By.XPATH, '//button[contains(@class, "showMore")]'),
        ]
        
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    time.sleep(wait_between_attempts)
                
                try:
                    show_more_exists = self.driver.execute_script("""
                        var buttons = document.querySelectorAll('button, a');
                        for (var i = 0; i < buttons.length; i++) {
                            var text = (buttons[i].textContent || buttons[i].innerText || '').toLowerCase();
                            var className = (buttons[i].className || '').toLowerCase();
                            if ((text.indexOf('show more') !== -1 || className.indexOf('showmore') !== -1) && 
                                buttons[i].offsetParent !== null) {
                                return true;
                            }
                        }
                        return false;
                    """)
                    
                    if not show_more_exists:
                        if attempt == 0:
                            self.logger.debug("'Show More' button not found via JavaScript check")
                        continue
                except:
                    pass
                
                for selector_type, selector_value in show_more_selectors:
                    try:
                        elements = self.driver.find_elements(selector_type, selector_value)
                        if not elements:
                            continue
                        
                        for element in elements:
                            try:
                                if not element.is_displayed():
                                    continue
                                
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
                                time.sleep(0.5)
                                
                                element = wait.until(EC.element_to_be_clickable((selector_type, selector_value)))
                                element.click()
                                self.logger.info(f"✓ Clicked 'Show More' button (attempt {attempt + 1})")
                                time.sleep(1)
                                return True
                            except Exception as e:
                                continue
                    except Exception as e:
                        continue
                
            except Exception as e:
                self.logger.debug(f"Attempt {attempt + 1} to find 'Show More' button failed: {str(e)}")
                continue
        
        self.logger.info("ℹ️ 'Show More' button not found after multiple attempts (may not be present or already clicked)")
        return False
    
    def _wait_for_fitment_data_loaded(self, timeout=30, min_rows=1):
        """
        Wait for fitment rows to be loaded and verify they actually exist.
        Returns True if rows are found, False otherwise.
        """
        if not self.driver:
            return False
        
        wait = WebDriverWait(self.driver, timeout)
        selectors = [
            'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr',
            'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
            'div.whatThisFitsFitment',
            'div[class*="whatThisFits"]',
        ]
        
        for check_round in range(5):
            try:
                if check_round > 0:
                    time.sleep(2)
                
                for selector in selectors:
                    try:
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        
                        if len(elements) >= min_rows:
                            valid_count = 0
                            for elem in elements[:10]:
                                try:
                                    text = elem.text.strip()
                                    html = elem.get_attribute('outerHTML') or ''
                                    if text or (html and len(html) > 50):
                                        valid_count += 1
                                except:
                                    pass
                            
                            if valid_count > 0:
                                self.logger.info(f"✓ Fitment data loaded: found {len(elements)} elements ({valid_count} with content) via {selector}")
                                time.sleep(2)
                                return True
                    except Exception as e:
                        continue
                
                try:
                    row_count = self.driver.execute_script("""
                        var count = 0;
                        var selectors = [
                            'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr',
                            'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
                            'div.whatThisFitsFitment'
                        ];
                        for (var i = 0; i < selectors.length; i++) {
                            var elements = document.querySelectorAll(selectors[i]);
                            if (elements.length > count) {
                                count = elements.length;
                            }
                        }
                        return count;
                    """)
                    
                    if row_count >= min_rows:
                        self.logger.info(f"✓ Fitment data loaded: found {row_count} rows via JavaScript")
                        time.sleep(2)
                        return True
                except:
                    pass
                
            except Exception as e:
                continue
        
        self.logger.warning(f"⚠️ Fitment rows not found after {timeout}s (checked {check_round + 1} times)")
        return False
    
    def scrape_product(self, url):
        """Scrape single product from usparts.volvocars.com"""
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
            # Extract title - Multiple strategies
            title_elem = soup.find('h1', class_='product-title')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_elem = soup.find('h1')
                if title_elem:
                    product_data['title'] = title_elem.get_text(strip=True)
            
            # SimplePart platform: span.prodDescriptH2
            if not product_data['title']:
                title_elem = soup.find('span', class_='prodDescriptH2')
                if title_elem:
                    product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_meta = soup.find('meta', property='og:title')
                if title_meta:
                    product_data['title'] = title_meta.get('content', '').strip()
            
            if not product_data['title']:
                title_meta = soup.find('meta', itemprop='name')
                if title_meta:
                    product_data['title'] = title_meta.get('content', '').strip()
            
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
            
            # Extract PN (Part Number) - SimplePart: span.stock-code-text > strong
            pn_elem = soup.find('span', class_='stock-code-text')
            if not pn_elem:
                # Fallback: try with itemprop="value"
                pn_elem = soup.find('span', {'itemprop': 'value', 'class': re.compile(r'stock-code-text', re.I)})
            if pn_elem:
                strong_elem = pn_elem.find('strong')
                if strong_elem:
                    pn_text = strong_elem.get_text(strip=True)
                    if pn_text:
                        # Remove extra spaces and clean (handles cases like "123456789     ")
                        product_data['pn'] = re.sub(r'\s+', '', pn_text).upper()
            
            # Extract SKU - SimplePart: span.alt-stock-code-text > strong (first part before semicolon)
            sku_elem = soup.find('span', class_='alt-stock-code-text')
            if sku_elem:
                strong_elem = sku_elem.find('strong')
                if strong_elem:
                    sku_text = strong_elem.get_text(strip=True)
                    # Extract first part before semicolon
                    if ';' in sku_text:
                        sku_text = sku_text.split(';')[0].strip()
                    product_data['sku'] = sku_text
            
            # Extract Replaces - SimplePart: span.alt-stock-code-text > strong (second part after semicolon)
            if sku_elem:
                strong_elem = sku_elem.find('strong')
                if strong_elem:
                    replaces_text = strong_elem.get_text(strip=True)
                    # Extract second part after semicolon
                    if ';' in replaces_text:
                        replaces_parts = replaces_text.split(';')
                        if len(replaces_parts) > 1:
                            product_data['replaces'] = replaces_parts[1].strip()
            
            # Fallback: URL pattern extraction for PN
            if not product_data['pn']:
                # SimplePart pattern: /p/Brand__/Product-Name/ID/PartNumber.html
                url_match = re.search(r'/p/[^/]+__/[^/]+/\d+/([^/]+)\.html', url)
                if url_match:
                    product_data['pn'] = re.sub(r'\s+', '', url_match.group(1)).upper()
            
            # Fallback: Set SKU to PN if not found separately
            if not product_data['sku']:
                product_data['sku'] = product_data['pn']
            
            # Fallback: Meta tags
            if not product_data['pn']:
                sku_meta = soup.find('meta', itemprop='sku')
                if sku_meta:
                    pn_text = sku_meta.get('content', '').strip()
                    product_data['pn'] = re.sub(r'\s+', '', pn_text).upper()
                    if not product_data['sku']:
                        product_data['sku'] = product_data['pn']
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract actual price - Multiple strategies
            # Strategy 1: JSON-LD
            json_ld_script = soup.find('script', type='application/ld+json')
            if json_ld_script:
                try:
                    json_ld = json.loads(json_ld_script.string)
                    if isinstance(json_ld, dict):
                        offers = json_ld.get('offers', {})
                        if isinstance(offers, dict):
                            price = offers.get('price', '')
                            if price:
                                product_data['actual_price'] = f"${price}" if not str(price).startswith('$') else str(price)
                except:
                    pass
            
            # Strategy 2: SimplePart - span.productPriceSpan (may have money-3 class)
            if not product_data['actual_price']:
                price_elem = soup.find('span', class_='productPriceSpan')
                if price_elem:
                    price_text = self.safe_find_text(soup, price_elem)
                    product_data['actual_price'] = self.extract_price(price_text)
            
            # Strategy 3: Generic price elements
            if not product_data['actual_price']:
                price_elem = soup.find('span', class_=re.compile(r'price|sale.*price', re.I))
                if not price_elem:
                    price_elem = soup.find('div', class_=re.compile(r'price|sale.*price', re.I))
                if not price_elem:
                    price_elem = soup.find('strong', id=re.compile(r'product.*price|price', re.I))
                if price_elem:
                    price_text = self.safe_find_text(soup, price_elem)
                    product_data['actual_price'] = self.extract_price(price_text)
            
            # Strategy 4: Meta tags
            if not product_data['actual_price']:
                price_meta = soup.find('meta', itemprop='price')
                if price_meta:
                    price_content = price_meta.get('content', '').strip()
                    if price_content:
                        product_data['actual_price'] = f"${price_content}" if not price_content.startswith('$') else price_content
            
            # Extract MSRP - Multiple strategies
            # Strategy 1: SimplePart - div.price-header-price
            msrp_elem = soup.find('div', class_='price-header-price')
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                # Remove HTML comments and extract price
                msrp_text = re.sub(r'<!.*?>', '', msrp_text).strip()
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Strategy 2: Generic MSRP elements
            if not product_data['msrp']:
                msrp_elem = soup.find('div', class_='msrpRow')
                if msrp_elem:
                    msrp_text = msrp_elem.get_text(strip=True)
                    # Look for "MSRP: $ X,XXX.XX" pattern
                    msrp_match = re.search(r'MSRP:\s*\$?\s*([\d,]+\.?\d*)', msrp_text, re.I)
                    if msrp_match:
                        product_data['msrp'] = f"${msrp_match.group(1)}"
            
            # Strategy 3: price-header-heading containing "MSRP"
            if not product_data['msrp']:
                msrp_container = soup.find('div', class_='price-header-heading')
                if msrp_container and 'MSRP' in msrp_container.get_text(strip=True):
                    price_container = msrp_container.find_next('div', class_='price-header-price')
                    if price_container:
                        msrp_text = price_container.get_text(strip=True)
                        product_data['msrp'] = self.extract_price(msrp_text)
            
            # Fallback: Use actual_price if MSRP not found
            if not product_data['msrp']:
                product_data['msrp'] = product_data['actual_price']
            
            # Extract image URL - Multiple strategies
            # Strategy 1: img[itemprop="image"]
            img_elem = soup.find('img', itemprop='image')
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-ukey_assemblyimage')
                if img_url:
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"{self.base_url}{img_url}"
                    else:
                        product_data['image_url'] = img_url
            
            # Strategy 2: Product image classes
            if not product_data['image_url']:
                img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Strategy 3: Meta tags
            if not product_data['image_url']:
                img_meta = soup.find('meta', property='og:image')
                if not img_meta:
                    img_meta = soup.find('meta', itemprop='image')
                if img_meta:
                    img_url = img_meta.get('content', '').strip()
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description - Multiple strategies
            # Strategy 1: SimplePart - div.item-desc > p
            desc_container = soup.find('div', class_='item-desc')
            if desc_container:
                desc_paragraphs = desc_container.find_all('p')
                if desc_paragraphs:
                    desc_texts = [p.get_text(strip=True) for p in desc_paragraphs if p.get_text(strip=True)]
                    product_data['description'] = ' '.join(desc_texts)
            
            # Strategy 2: Generic description elements
            if not product_data['description']:
                desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
                if not desc_elem:
                    desc_elem = soup.find('p', class_=re.compile(r'description', re.I))
                if desc_elem:
                    desc_text = self.safe_find_text(soup, desc_elem)
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    product_data['description'] = desc_text.strip()
            
            # Strategy 3: Meta tags
            if not product_data['description']:
                desc_meta = soup.find('meta', property='og:description')
                if not desc_meta:
                    desc_meta = soup.find('meta', itemprop='description')
                if desc_meta:
                    product_data['description'] = desc_meta.get('content', '').strip()
            
            # Extract also_known_as - Generic search
            also_known_elems = soup.find_all(['span', 'div', 'p'], string=re.compile(r'also known as|other names|aka', re.I))
            for elem in also_known_elems:
                parent = elem.parent
                if parent:
                    text = parent.get_text(strip=True)
                    if text:
                        product_data['also_known_as'] = text
                        break
            
            # Extract replaces - Already extracted above from alt-stock-code-text
            # Fallback: Generic search if not found
            if not product_data['replaces']:
                replaces_elems = soup.find_all(['span', 'div', 'h2'], string=re.compile(r'replaces|supersedes', re.I))
                for elem in replaces_elems:
                    parent = elem.parent
                    if parent:
                        text = parent.get_text(strip=True)
                        if text:
                            product_data['replaces'] = text
                            break
            
            # Extract fitment data - SimplePart: whatThisFitsFitment and whatThisFitsYears divs
            # Similar structure to Subaru/Volkswagen
            fitment_container = soup.find('div', class_='whatThisFitsContainer')
            if not fitment_container:
                fitment_container = soup.find('div', class_='col-md-12')
            
            if fitment_container:
                # Find all fitment rows (div.col-lg-12)
                fitment_rows = fitment_container.find_all('div', class_='col-lg-12')
                for row in fitment_rows:
                    try:
                        # Extract fitment text (model, trim, engine)
                        fitment_elem = row.find('div', class_='whatThisFitsFitment')
                        if not fitment_elem:
                            continue
                        
                        fitment_span = fitment_elem.find('span')
                        if not fitment_span:
                            continue
                        
                        fitment_text = fitment_span.get_text(strip=True)
                        # Remove "Volvo " prefix if present
                        fitment_text = re.sub(r'^Volvo\s+', '', fitment_text, flags=re.I)
                        
                        # Extract years
                        years_elem = row.find('div', class_='whatThisFitsYears')
                        years = []
                        if years_elem:
                            # Extract from links
                            year_links = years_elem.find_all('a')
                            for link in year_links:
                                year_text = link.get_text(strip=True)
                                if year_text:
                                    years.append(year_text)
                            
                            # Also check for comma-separated text
                            if not years:
                                years_span = years_elem.find('span')
                                if years_span:
                                    years_text = years_span.get_text(strip=True)
                                    # Split by comma and extract years
                                    year_parts = re.split(r',\s*', years_text)
                                    for part in year_parts:
                                        # Extract year from text (e.g., "2021", "2022")
                                        year_match = re.search(r'\b(19|20)\d{2}\b', part)
                                        if year_match:
                                            years.append(year_match.group(0))
                        
                        # Parse fitment text to extract model, trim, engine
                        # Similar to Volkswagen parsing
                        model = ''
                        trim = ''
                        engine = ''
                        
                        # Extract model (first part, usually ends before engine description)
                        model_match = re.match(r'^([A-Z0-9\.\-\s]+?)(?:\s+-\s*[A-Z]|\s+-\s*cylinder|\s+\d+\.\d+L)', fitment_text, re.I)
                        if model_match:
                            model = model_match.group(1).strip()
                        else:
                            # Fallback: extract first word/phrase (usually model name)
                            first_part = fitment_text.split(' -')[0].strip()
                            if first_part:
                                model = first_part
                        
                        # Extract transmission and drivetrain info
                        trans_drive_match = re.search(r'(A/T|M/T)\s+(AWD|RWD|FWD)', fitment_text, re.I)
                        
                        # Extract engine (everything between model and transmission)
                        if trans_drive_match:
                            # Engine is between model and transmission
                            engine_start = len(model) if model else 0
                            engine_end = trans_drive_match.start()
                            engine_text = fitment_text[engine_start:engine_end].strip()
                            # Clean up engine text
                            engine_text = re.sub(r'^[\s\-]+|[\s\-]+$', '', engine_text)
                            engine = engine_text
                            
                            # Extract trim (everything after transmission/drivetrain, before body type)
                            trim_start = trans_drive_match.end()
                            remaining = fitment_text[trim_start:].strip()
                            # Remove common body type suffixes
                            remaining = re.sub(r'\s+(Sport\s+Utility|Sedan|Hatchback|Coupe|Convertible|Wagon|SUV)$', '', remaining, flags=re.I)
                            trim = remaining.strip()
                        else:
                            # Fallback: try to find A/T or M/T pattern
                            at_match = re.search(r'(A/T|M/T)', fitment_text, re.I)
                            if at_match:
                                parts = fitment_text.split(at_match.group(0), 1)
                                if len(parts) == 2:
                                    before_at = parts[0].strip()
                                    if model:
                                        engine = before_at.replace(model, '').strip()
                                        engine = re.sub(r'^[\s\-]+|[\s\-]+$', '', engine)
                                    after_at = parts[1].strip()
                                    after_at = re.sub(r'\s+(Sport\s+Utility|Sedan|Hatchback|Coupe|Convertible|Wagon|SUV)$', '', after_at, flags=re.I)
                                    trim = after_at.strip()
                            else:
                                # Last resort: split by dashes
                                parts = re.split(r'\s+-\s+', fitment_text, 2)
                                if len(parts) >= 1:
                                    model = parts[0].strip()
                                if len(parts) >= 2:
                                    engine = parts[1].strip()
                                if len(parts) >= 3:
                                    trim = parts[2].strip()
                        
                        # Create fitment entry for each year
                        if years:
                            for year in years:
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': 'Volvo',
                                    'model': model,
                                    'trim': trim,
                                    'engine': engine
                                })
                        else:
                            # If no years found, create one entry with empty year
                            product_data['fitments'].append({
                                'year': '',
                                'make': 'Volvo',
                                'model': model,
                                'trim': trim,
                                'engine': engine
                            })
                    except Exception as e:
                        self.logger.debug(f"Error parsing fitment row: {str(e)}")
                        continue
            
            # Fallback: JSON data (RevolutionParts)
            if not product_data['fitments']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        fitments = product_json.get('fitment', []) or product_json.get('vehicles', [])
                        if fitments:
                            for fitment_entry in fitments:
                                try:
                                    year = str(fitment_entry.get('year', '')).strip()
                                    make = str(fitment_entry.get('make', '')).strip() or 'Volvo'
                                    model = str(fitment_entry.get('model', '')).strip()
                                    trim = str(fitment_entry.get('trim', '')).strip()
                                    engine = str(fitment_entry.get('engine', '')).strip()
                                    
                                    # Handle multiple trims/engines by creating combinations
                                    trims = [t.strip() for t in trim.split(',')] if trim else ['']
                                    engines = [e.strip() for e in engine.split(',')] if engine else ['']
                                    
                                    for t in trims:
                                        for e in engines:
                                            product_data['fitments'].append({
                                                'year': year,
                                                'make': make,
                                                'model': model,
                                                'trim': t,
                                                'engine': e
                                            })
                                except:
                                    continue
                    except:
                        pass
            
            # Fallback: Fitment table (SimplePart/RevolutionParts)
            if not product_data['fitments']:
                fitment_table = soup.find('table', class_='fitment-table')
                if fitment_table:
                    tbody = fitment_table.find('tbody', class_='fitment-table-body')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            try:
                                cells = row.find_all('td')
                                if len(cells) >= 5:
                                    year = cells[0].get_text(strip=True) if len(cells) > 0 else ''
                                    make = cells[1].get_text(strip=True) if len(cells) > 1 else 'Volvo'
                                    model = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                                    trim = cells[3].get_text(strip=True) if len(cells) > 3 else ''
                                    engine = cells[4].get_text(strip=True) if len(cells) > 4 else ''
                                    
                                    # Handle multiple values
                                    trims = [t.strip() for t in trim.split(',')] if trim else ['']
                                    engines = [e.strip() for e in engine.split(',')] if engine else ['']
                                    
                                    for t in trims:
                                        for e in engines:
                                            product_data['fitments'].append({
                                                'year': year,
                                                'make': make,
                                                'model': model,
                                                'trim': t,
                                                'engine': e
                                            })
                            except:
                                continue
            
            # Default empty fitment if none found
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
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return None

