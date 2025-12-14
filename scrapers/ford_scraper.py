"""Scraper for parts.lakelandford.com (Ford parts)"""
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

class FordScraper(BaseScraperWithExtension):
    """Scraper for parts.lakelandford.com"""
    
    def __init__(self):
        super().__init__('ford', use_selenium=True)
        self.base_url = 'https://parts.lakelandford.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from parts.lakelandford.com"""
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
        """Search for wheels using site search - single page with 250 results, no pagination"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Search URL with numResults=250 to get all products on one page (no pagination)
            search_url = f"{self.base_url}/productSearch.aspx?ukey_make=0&modelYear=0&ukey_model=0&ukey_trimLevel=0&ukey_driveline=0&ukey_Category=0&numResults=250&sortOrder=Relevance&ukey_tag=0&isOnSale=0&isAccessory=0&isPerformance=0&showAllModels=1&searchTerm=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                self.driver.get(search_url)
                time.sleep(3)
            except Exception as e:
                self.logger.error(f"Error loading search page: {str(e)}")
                return product_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links to appear using Selenium
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/']"))
                )
                self.logger.info("Product links detected on page")
            except TimeoutException:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products (lazy loading)
            self._scroll_to_load_content()
            
            # Wait a bit more for any dynamic content
            time.sleep(3)
            
            # Try to find product links using Selenium first (more reliable for dynamic content)
            try:
                # Try multiple patterns for Ford product URLs
                selenium_selectors = [
                    "a[href*='/p/Ford__/']",
                    "a[href*='/p/Ford/']",
                    "a[href*='/p/']",
                ]
                
                selenium_links = []
                for selector in selenium_selectors:
                    try:
                        links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if links:
                            selenium_links = links
                            self.logger.info(f"Found {len(selenium_links)} product links via Selenium using selector: {selector}")
                            break
                    except:
                        continue
                
                for link_elem in selenium_links:
                    try:
                        href = link_elem.get_attribute('href')
                        if href:
                            # Normalize URL
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove query parameters and fragments
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            
                            # Only collect individual product pages (SimplePart pattern: /p/Ford__/Product-Name/ID/PartNumber.html)
                            if '/p/' in full_url and full_url.endswith('.html'):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    except Exception as e:
                        self.logger.debug(f"Error extracting link from Selenium element: {str(e)}")
                        continue
            except Exception as e:
                self.logger.debug(f"Error finding links via Selenium: {str(e)}")
            
            # Also try BeautifulSoup parsing as fallback
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Try multiple patterns to find product links (SimplePart platform)
            # Pattern 1: /p/Ford__/Product-Name/ID/PartNumber.html
            product_links = soup.find_all('a', href=re.compile(r'/p/Ford__/', re.I))
            
            # Pattern 2: /p/Ford/Product-Name/ID/PartNumber.html (alternative)
            if not product_links:
                product_links = soup.find_all('a', href=re.compile(r'/p/Ford/', re.I))
            
            # Pattern 3: Try looking in product containers/rows
            if not product_links:
                product_containers = soup.find_all(['div', 'li', 'tr'], class_=re.compile(r'product|item|part|row', re.I))
                for container in product_containers:
                    container_links = container.find_all('a', href=re.compile(r'/p/', re.I))
                    product_links.extend(container_links)
            
            # Pattern 4: Look for any link with /p/ pattern ending in .html
            if not product_links:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    if href and '/p/' in href.lower() and href.lower().endswith('.html'):
                        product_links.append(link)
            
            # Extract URLs from BeautifulSoup links
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
                    
                    # Only collect individual product pages
                    if '/p/' in full_url and full_url.endswith('.html'):
                        if full_url not in product_urls:
                            product_urls.append(full_url)
            
            # Pattern 5: Use JavaScript to find all links (most comprehensive fallback)
            if not product_urls:
                try:
                    js_links = self.driver.execute_script("""
                        var links = [];
                        var allLinks = document.querySelectorAll('a[href]');
                        for (var i = 0; i < allLinks.length; i++) {
                            var href = allLinks[i].href || allLinks[i].getAttribute('href');
                            if (href && href.toLowerCase().indexOf('/p/') !== -1 && href.toLowerCase().endsWith('.html')) {
                                links.push(href);
                            }
                        }
                        return links;
                    """)
                    self.logger.info(f"Found {len(js_links)} product links via JavaScript")
                    for js_link in js_links:
                        if js_link and js_link not in product_urls:
                            # Normalize URL
                            full_url = js_link if js_link.startswith('http') else f"{self.base_url}{js_link}"
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            if '/p/' in full_url and full_url.endswith('.html'):
                                product_urls.append(full_url)
                except Exception as js_error:
                    self.logger.debug(f"Error finding links via JavaScript: {str(js_error)}")
            
            self.logger.info(f"Found {len(product_urls)} unique product URLs on search page")
            
            # No pagination - all results are on a single page
            
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
            'div#WhatThisFitsTabComponent_TABPANEL',
            'div#fitments.tab-pane.active',
            'div#ctl00_Content_PageBody_ProductTabsLegacy_UpdatePanel_applications',
            'div[role="tabpanel"].active',
            'div.tab-pane.active',
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
            (By.CSS_SELECTOR, 'button.showMoreBtnLink'),
            (By.CSS_SELECTOR, 'button.btn-link.showMoreBtnLink'),
            (By.CSS_SELECTOR, 'a.showMoreBtnLink'),
            (By.CSS_SELECTOR, 'a.btn-link.showMoreBtnLink'),
            (By.XPATH, '//button[contains(text(), "Show More")]'),
            (By.XPATH, '//a[contains(text(), "Show More")]'),
            (By.XPATH, '//button[contains(@class, "showMore")]'),
            (By.XPATH, '//a[contains(@class, "showMore")]'),
            (By.ID, 'ctl00_Content_PageBody_ProductTabsLegacy_showAllApplications'),
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
            'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
            'div.whatThisFitsFitment',
            'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr',
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
                            'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
                            'div.whatThisFitsFitment',
                            'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr'
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
        """Scrape single product from parts.lakelandford.com"""
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
            # Extract title - try multiple selectors (priority: h1 > og:title > title tag)
            title_elem = soup.find('h1')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                # Try meta og:title
                og_title = soup.find('meta', property='og:title')
                if og_title:
                    product_data['title'] = og_title.get('content', '').strip()
            
            if not product_data['title'] or len(product_data['title']) < 3:
                # Try title tag as last resort
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # Clean up title: "2018 Ford F-150 Wheel 12345 | Parts Online"
                    if '|' in title_text:
                        title_text = title_text.split('|')[0].strip()
                    product_data['title'] = title_text
            
            if not product_data['title'] or len(product_data['title']) < 3:
                self.logger.warning(f"⚠️ No valid title found for {url}")
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - try multiple selectors
            sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                sku_elem = soup.find('h2', class_=re.compile(r'sku|part.*number', re.I))
            if not sku_elem:
                # Try to extract from title or URL if it contains part number
                # Example: "2018 Ford F-150 Wheel 12345"
                title_words = product_data['title'].split()
                for word in reversed(title_words):
                    if word.isdigit() and len(word) >= 6:  # Part numbers are usually 6+ digits
                        product_data['sku'] = word
                        product_data['pn'] = self.clean_sku(word)
                        break
            
            if sku_elem and not product_data['sku']:
                product_data['sku'] = sku_elem.get_text(strip=True)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    self.logger.info(f"⏭️ Skipping non-wheel product: {product_data['title']}")
                    return None
            except Exception as e:
                self.logger.warning(f"Error checking if wheel product: {str(e)}")
                return None
            
            # Extract sale price - try multiple selectors
            sale_price_elem = soup.find('strong', id='product_price')
            if not sale_price_elem:
                sale_price_elem = soup.find('strong', class_=re.compile(r'sale.*price', re.I))
            if not sale_price_elem:
                sale_price_elem = soup.find('span', id='product_price')
            if not sale_price_elem:
                sale_price_elem = soup.find('span', class_=re.compile(r'sale.*price|price', re.I))
            if not sale_price_elem:
                sale_price_elem = soup.find('div', class_=re.compile(r'sale.*price|price.*value', re.I))
            if not sale_price_elem:
                # Try meta tag
                price_meta = soup.find('meta', itemprop='price')
                if price_meta:
                    product_data['actual_price'] = self.extract_price(price_meta.get('content', ''))
            
            if sale_price_elem and not product_data['actual_price']:
                price_text = sale_price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
            
            # Extract MSRP - try multiple selectors
            msrp_elem = soup.find('span', id='product_price2')
            if not msrp_elem:
                msrp_elem = soup.find('span', class_=re.compile(r'list.*price', re.I))
            if not msrp_elem:
                msrp_elem = soup.find('div', class_=re.compile(r'msrp|list.*price', re.I))
            if not msrp_elem:
                # Try to find "compared" price (MSRP is often shown as comparison)
                msrp_elem = soup.find('span', class_=re.compile(r'compared|compare.*price', re.I))
            
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                product_data['msrp'] = self.extract_price(msrp_text)
            
            # Extract image URL - try multiple sources
            img_elem = soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I))
            if not img_elem:
                img_elem = soup.find('img', itemprop='image')
            if not img_elem:
                # Try to find any product-related image
                img_elem = soup.find('img', {'src': re.compile(r'product|part|wheel', re.I)})
            
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src') or img_elem.get('data-original')
                if img_url:
                    # Normalize image URL
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"{self.base_url}{img_url}"
                    else:
                        product_data['image_url'] = img_url
            
            # Fallback: try og:image meta tag
            if not product_data['image_url']:
                og_image = soup.find('meta', property='og:image')
                if og_image:
                    img_url = og_image.get('content', '')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description - try multiple sources
            # First try meta og:description (often contains structured description)
            desc_elem = soup.find('meta', property='og:description')
            if desc_elem:
                product_data['description'] = desc_elem.get('content', '').strip()
            
            # If not found, try HTML elements
            if not product_data['description']:
                desc_elem = soup.find('span', class_='description_body')
                if not desc_elem:
                    desc_elem = soup.find('li', class_='description')
                    if desc_elem:
                        desc_elem = desc_elem.find('span', class_='list-value')
                if not desc_elem:
                    desc_elem = soup.find('div', class_=re.compile(r'description|product.*description', re.I))
                if not desc_elem:
                    desc_elem = soup.find('p', class_=re.compile(r'description', re.I))
                
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True, separator=' ')
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    product_data['description'] = desc_text.strip()
            
            # Extract also_known_as (Other Names)
            also_known_elem = soup.find('li', class_='also_known_as')
            if also_known_elem:
                value_elem = also_known_elem.find('h2', class_='list-value')
                if not value_elem:
                    value_elem = also_known_elem.find('span', class_='list-value')
                if value_elem:
                    product_data['also_known_as'] = value_elem.get_text(strip=True)
            
            # Extract replaces
            replaces_elem = soup.find('li', class_='product-superseded-list')
            if not replaces_elem:
                replaces_elem = soup.find('li', class_=re.compile(r'superseded|replaces', re.I))
            if replaces_elem:
                value_elem = replaces_elem.find('h2', class_='list-value')
                if not value_elem:
                    value_elem = replaces_elem.find('span', class_='list-value')
                if value_elem:
                    product_data['replaces'] = value_elem.get_text(strip=True)
            
            # Extract fitment - Need to click "What This Fits" tab and "Show More" button to load all fitment data
            # Steps:
            # 1. Click the "What This Fits" tab
            # 2. Wait for tab panel to load
            # 3. Click "Show More" button
            # 4. Wait for all fitment data to load
            # 5. Extract fitment data from updated HTML
            
            fitment_rows_elements = []  # Initialize to store Selenium WebElement objects
            selenium_extraction_success = False
            
            if self.driver:
                try:
                    self.logger.info("🔍 Clicking 'What This Fits' tab to load fitment data...")
                    
                    # Step 1: Find and click the "What This Fits" tab
                    wait = WebDriverWait(self.driver, 10)
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
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab_element)
                            time.sleep(0.5)
                            tab_element.click()
                            tab_clicked = True
                            self.logger.info("✓ Clicked 'What This Fits' tab")
                            break
                        except Exception as e:
                            self.logger.debug(f"Attempt to click tab with {selector_type}={selector_value} failed: {e}")
                            continue
                    
                    if not tab_clicked:
                        self.logger.warning("⚠️ Could not find or click 'What This Fits' tab")
                    else:
                        # Step 2: WAIT for tab panel to be fully loaded
                        self.logger.info("🔍 Step 2: Waiting for tab panel to load completely...")
                        tab_panel_loaded = self._wait_for_tab_panel_loaded(timeout=30)
                        if tab_panel_loaded:
                            self.logger.info("✓ Tab panel loaded completely")
                        else:
                            self.logger.warning("⚠️ Tab panel may not have loaded completely, continuing anyway...")
                        
                        # Step 3: Click "Show More" button if present (with improved detection)
                        self.logger.info("🔍 Step 3: Looking for 'Show More' button...")
                        show_more_clicked = self._find_and_click_show_more(max_attempts=5, wait_between_attempts=2)
                        
                        # Step 4: WAIT for all fitment data to load completely
                        self.logger.info("🔍 Step 4: Waiting for all fitment data to load completely...")
                        fitment_loaded = self._wait_for_fitment_data_loaded(timeout=30, min_rows=1)
                        
                        if not fitment_loaded:
                            # Try one more time after additional wait
                            self.logger.info("🔍 Retrying fitment data load check after additional wait...")
                            time.sleep(5)
                            fitment_loaded = self._wait_for_fitment_data_loaded(timeout=20, min_rows=1)
                        
                        # Additional wait to ensure everything is stable
                        time.sleep(1)
                        
                        # Step 5: Extract fitment data directly using Selenium (more reliable for dynamic content)
                        # Use Selenium to find elements directly instead of BeautifulSoup
                        try:
                            # Find all fitment rows using Selenium - try multiple selectors
                            fitment_row_elements = []
                            
                            selectors_to_try = [
                                'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
                                'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr',
                                'div.whatThisFitsFitment',
                                'div[class*="whatThisFits"]',
                            ]
                            
                            for selector in selectors_to_try:
                                try:
                                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                    if elements:
                                        valid_elements = []
                                        for elem in elements:
                                            try:
                                                text = elem.text.strip()
                                                html = elem.get_attribute('outerHTML') or ''
                                                if text or (html and len(html) > 50):
                                                    valid_elements.append(elem)
                                            except:
                                                pass
                                        
                                        if valid_elements:
                                            fitment_row_elements = valid_elements
                                            self.logger.info(f"✓ Found {len(fitment_row_elements)} valid fitment rows via {selector}")
                                    break
                                except Exception as e:
                                    continue
                            
                            if not fitment_row_elements:
                                # Try finding any div.col-lg-12 that might contain fitment data
                                all_cols = self.driver.find_elements(By.CSS_SELECTOR, 'div.col-lg-12')
                                for col in all_cols:
                                    try:
                                        col_html = col.get_attribute('outerHTML') or ''
                                        if 'whatThisFits' in col_html.lower() or 'fitment' in col_html.lower():
                                            fitment_row_elements.append(col)
                                    except:
                                        continue
                            
                            if fitment_row_elements:
                                self.logger.info(f"✓ Found {len(fitment_row_elements)} fitment rows via Selenium")
                                fitment_rows_elements = fitment_row_elements
                                selenium_extraction_success = True
                            else:
                                self.logger.warning("⚠️ No fitment rows found via Selenium selectors")
                        
                        except Exception as selenium_error:
                            self.logger.warning(f"⚠️ Error extracting fitment via Selenium: {str(selenium_error)}")
                            selenium_extraction_success = False
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Error interacting with fitment tab: {str(e)}")
            
            # Extract fitment data from updated HTML (or fallback to initial HTML if Selenium failed)
            if selenium_extraction_success and fitment_rows_elements:
                self.logger.info(f"🔍 Processing {len(fitment_rows_elements)} fitment rows from Selenium WebElements...")
                for idx, row_element in enumerate(fitment_rows_elements):
                    try:
                        # Get outerHTML of the WebElement and parse with BeautifulSoup
                        row_html = row_element.get_attribute('outerHTML')
                        if not row_html:
                            continue
                        
                        row_soup = BeautifulSoup(row_html, 'lxml')
                        
                        # Find the vehicle description span
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
                        
                        # Parse vehicle_text
                        make = 'Ford'
                        parse_text = vehicle_text
                        if parse_text.startswith('Ford '):
                            parse_text = parse_text[5:].strip()
                        
                        words = parse_text.split()
                        model = words[0] if words else ''
                        
                        engine = ''
                        trim = ''
                        
                        # Attempt to extract engine and trim more robustly
                        engine_match = re.search(r'(\d+\.?\d*L\s*(?:V\d|I\d)?(?:\s*MILD HYBRID EV-GAS \(MHEV\))?(?:\s*EV-GAS \(MHEV\))?(?:\s*GAS)?(?:\s*ELECTRIC)?(?:\s*A/T|\s*M/T|\s*CVT|\s*AUTO|\s*MANUAL)?)', parse_text, re.IGNORECASE)
                        if engine_match:
                            engine = engine_match.group(1).strip()
                            # Remove engine part from parse_text to isolate trim
                            trim_start_index = parse_text.find(engine) + len(engine)
                            trim = parse_text[trim_start_index:].strip()
                            # Remove model from trim if it's still there
                            if trim.startswith(model):
                                trim = trim[len(model):].strip()
                        else:
                            # If no engine found, assume rest is trim after model
                            if len(words) > 1:
                                trim = ' '.join(words[1:]).strip()
                        
                        # Find years using Selenium directly from the row element (more reliable)
                        years = []
                        try:
                            years_div_element = row_element.find_element(By.CSS_SELECTOR, 'div.whatThisFitsYears')
                            year_links = years_div_element.find_elements(By.TAG_NAME, 'a')
                            
                            for link in year_links:
                                href = link.get_attribute('href') or ''
                                year_match = re.search(r'/p/Ford_(\d{4})', href)
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
                            # Fallback: try finding years div in BeautifulSoup
                            try:
                                years_div = row_soup.find('div', class_=lambda x: x and ('whatThisFitsYears' in str(x) if x else False))
                                if not years_div:
                                    years_div = row_soup.find('div', class_=re.compile(r'whatThisFitsYears', re.I))
                                
                                if years_div:
                                    year_links = years_div.find_all('a', href=True)
                                    for link in year_links:
                                        href = link.get('href', '')
                                        year_match = re.search(r'/p/Ford_(\d{4})', href)
                                        if year_match:
                                            years.append(year_match.group(1))
                                        else:
                                            link_text = link.get_text(strip=True)
                                            if link_text and link_text.isdigit() and len(link_text) == 4:
                                                years.append(link_text)
                                    if not years:
                                        years_text = years_div.get_text(strip=True)
                                        year_matches = re.findall(r'\b(\d{4})\b', years_text)
                                        years = [y for y in year_matches if 1900 <= int(y) <= 2100]
                            except Exception as fallback_error:
                                self.logger.debug(f"Error extracting years (both methods): {str(fallback_error)}")
                        
                        if years:
                            for year in years:
                                product_data['fitments'].append({
                                    'year': year,
                                    'make': make,
                                    'model': model,
                                    'trim': trim,
                                    'engine': engine
                                })
                            self.logger.info(f"🚗 Row {idx+1}: Found {len(years)} fitment(s): {model} ({', '.join(years)})")
                        else:
                            product_data['fitments'].append({
                                'year': '', 'make': make, 'model': model, 'trim': trim, 'engine': engine
                            })
                            self.logger.info(f"🚗 Row {idx+1}: Found 1 fitment (no year): {model}")
                    except Exception as row_parse_error:
                        self.logger.warning(f"⚠️ Error parsing fitment row {idx+1} via Selenium: {str(row_parse_error)}")
                        self.logger.debug(traceback.format_exc())
            else:
                self.logger.warning("⚠️ No fitment rows found for processing, even after dynamic interaction attempts.")
            
            # If no fitments found, still return the product with empty fitment
            if not product_data['fitments']:
                self.logger.warning(f"⚠️ No fitment data found for {product_data['title']}")
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

