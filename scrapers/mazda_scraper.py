"""Scraper for www.jimellismazdaparts.com (Mazda parts) - SimplePart platform"""
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

class MazdaScraper(BaseScraperWithExtension):
    """Scraper for www.jimellismazdaparts.com - Uses SimplePart platform"""
    
    def __init__(self):
        super().__init__('mazda', use_selenium=True)
        self.base_url = 'https://www.jimellismazdaparts.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.jimellismazdaparts.com"""
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            validated_urls = []
            for url in product_urls:
                # Product URLs: /products/Mazda/Product-Name/ID/PartNumber.html
                # Category URLs: /Mazda__/Category.html or /productSearch.aspx
                is_product = re.search(r'/products/Mazda/[^/]+/\d+/[^/]+\.html', url, re.I)
                is_category = re.search(r'/Mazda__/[^/]+\.html$|/productSearch\.aspx', url, re.I)
                
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
        """
        Search for wheels using site search - single page with 250 results, no pagination
        Product URLs pattern: /products/Mazda/Product-Name/ID/PartNumber.html
        Look for: <a href="/products/Mazda/..." class="btn btn-primary">View Product</a>
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Search URL with numResults=250 to get all products on one page (no pagination)
            search_url = f"{self.base_url}/productSearch.aspx?ukey_make=995&modelYear=0&ukey_model=0&ukey_trimLevel=0&ukey_driveline=0&ukey_Category=0&numResults=250&sortOrder=Relevance&ukey_tag=0&isOnSale=0&isAccessory=0&isPerformance=0&showAllModels=1&searchTerm=wheel"
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
            # Look for links with href containing /products/Mazda/
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/products/Mazda/']"))
                )
                self.logger.info("Product links detected on page")
            except TimeoutException:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products (lazy loading)
            self._scroll_to_load_content()
            
            # Wait a bit more for any dynamic content
            time.sleep(3)
            
            # Priority 1: Find "View Product" buttons with class="btn btn-primary" and href="/products/Mazda/..."
            try:
                # Look for links with class="btn btn-primary" and text "View Product"
                view_product_links = self.driver.find_elements(By.XPATH, "//a[@class='btn btn-primary' and contains(text(), 'View Product')]")
                self.logger.info(f"Found {len(view_product_links)} 'View Product' buttons")
                
                for link_elem in view_product_links:
                    try:
                        href = link_elem.get_attribute('href')
                        if href and '/products/Mazda/' in href:
                            # Normalize URL
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove query parameters and fragments
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            
                            # Validate pattern: /products/Mazda/Product-Name/ID/PartNumber.html
                            if re.search(r'/products/Mazda/[^/]+/\d+/[^/]+\.html$', full_url, re.I):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    except Exception as e:
                        self.logger.debug(f"Error extracting link from 'View Product' button: {str(e)}")
                        continue
            except Exception as e:
                self.logger.debug(f"Error finding 'View Product' buttons: {str(e)}")
            
            # Priority 2: Find all links with href containing /products/Mazda/ (broader search)
            try:
                selenium_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/products/Mazda/']")
                self.logger.info(f"Found {len(selenium_links)} product links via Selenium (all /products/Mazda/ links)")
                
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
                            
                            # Validate pattern: /products/Mazda/Product-Name/ID/PartNumber.html
                            if re.search(r'/products/Mazda/[^/]+/\d+/[^/]+\.html$', full_url, re.I):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    except Exception as e:
                        self.logger.debug(f"Error extracting link from Selenium element: {str(e)}")
                        continue
            except Exception as e:
                self.logger.debug(f"Error finding links via Selenium: {str(e)}")
            
            # Priority 3: Use JavaScript to find all /products/Mazda/ links (most comprehensive)
            if len(product_urls) < 200:  # If we haven't found enough, try JavaScript
                try:
                    js_links = self.driver.execute_script("""
                        var links = [];
                        var allLinks = document.querySelectorAll('a[href]');
                        for (var i = 0; i < allLinks.length; i++) {
                            var href = allLinks[i].href || allLinks[i].getAttribute('href');
                            if (href && href.toLowerCase().indexOf('/products/mazda/') !== -1 && href.toLowerCase().endsWith('.html')) {
                                links.push(href);
                            }
                        }
                        return links;
                    """)
                    self.logger.info(f"Found {len(js_links)} product links via JavaScript")
                    for js_link in js_links:
                        if js_link:
                            # Normalize URL
                            full_url = js_link if js_link.startswith('http') else f"{self.base_url}{js_link}"
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            # Validate pattern
                            if re.search(r'/products/Mazda/[^/]+/\d+/[^/]+\.html$', full_url, re.I):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                except Exception as e:
                    self.logger.debug(f"Error finding links via JavaScript: {str(e)}")
            
            # Priority 4: BeautifulSoup parsing as final fallback
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Look for links with href="/products/Mazda/..."
            product_links = soup.find_all('a', href=re.compile(r'/products/Mazda/', re.I))
            self.logger.info(f"Found {len(product_links)} product links via BeautifulSoup")
            
            # Extract and validate product URLs from BeautifulSoup results
            for link in product_links:
                href = link.get('href', '')
                if not href:
                    continue
                
                # Normalize URL
                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                
                # Remove query parameters and fragments
                if '?' in full_url:
                    full_url = full_url.split('?')[0]
                if '#' in full_url:
                    full_url = full_url.split('#')[0]
                full_url = full_url.rstrip('/')
                
                # Validate pattern: /products/Mazda/Product-Name/ID/PartNumber.html
                if re.search(r'/products/Mazda/[^/]+/\d+/[^/]+\.html$', full_url, re.I):
                    if full_url not in product_urls:
                        product_urls.append(full_url)
            
            # Remove duplicates and sort
            product_urls = sorted(list(set(product_urls)))
            self.logger.info(f"Extracted {len(product_urls)} unique product URLs from search page (expected: 250)")
            
            if len(product_urls) < 200:
                self.logger.warning(f"⚠️ Found only {len(product_urls)} product URLs, expected 250. May need to check selectors.")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _browse_tire_wheel_category(self):
        """Browse Tire and Wheel category with pagination"""
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            category_url = f"{self.base_url}/Mazda__/Tire-and-Wheel.html"
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
            
            # Wait for product links
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Mazda__/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products
            self._scroll_to_load_content()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract total pages if pagination exists
            total_pages = 1
            try:
                pagination_links = soup.find_all('a', href=re.compile(r'Tire-and-Wheel.*page|Page', re.I))
                max_page = 1
                
                for link in pagination_links:
                    href = link.get('href', '')
                    page_match = re.search(r'[Pp]age[=:]?(\d+)', href + ' ' + link.get_text(strip=True), re.I)
                    if page_match:
                        page_num = int(page_match.group(1))
                        if page_num > max_page:
                            max_page = page_num
                    
                    link_text = link.get_text(strip=True)
                    if link_text.isdigit():
                        page_num = int(link_text)
                        if page_num > max_page:
                            max_page = page_num
                
                total_pages = max_page
                if total_pages > 1:
                    self.logger.info(f"Found pagination: {total_pages} total pages for category")
            except Exception as e:
                self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
            
            # Extract products from all pages
            for page_num in range(1, total_pages + 1):
                try:
                    if page_num > 1:
                        pag_url = f"{category_url}?page={page_num}"
                        self.logger.info(f"Loading category page {page_num}/{total_pages}: {pag_url}")
                        
                        try:
                            self.page_load_timeout = 60
                            self.driver.set_page_load_timeout(60)
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                self.logger.warning(f"Category page {page_num} content too short, skipping")
                                continue
                            
                            try:
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/Mazda__/']"))
                                )
                            except:
                                pass
                            
                            self._scroll_to_load_content()
                            pag_html = self.driver.page_source
                            soup = BeautifulSoup(pag_html, 'lxml')
                        except Exception as e:
                            self.logger.warning(f"Error loading category page {page_num}: {str(e)}")
                            continue
                        finally:
                            try:
                                self.page_load_timeout = original_timeout
                                self.driver.set_page_load_timeout(original_timeout)
                            except:
                                pass
                    
                    # Extract product links from current page
                    product_links = soup.find_all('a', href=re.compile(r'/p/Mazda__/'))
                    page_count = 0
                    
                    for link in product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            full_url = full_url.rstrip('/')
                            
                            # Only collect individual product pages
                            if '/p/Mazda__/' in full_url and full_url.endswith('.html'):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                                    page_count += 1
                    
                    self.logger.info(f"Category page {page_num}/{total_pages}: Found {len(product_links)} product links, {page_count} new unique URLs (Total: {len(product_urls)})")
                    
                    if page_count == 0 and page_num > 1:
                        self.logger.info(f"No new products found on category page {page_num}, stopping pagination")
                        break
                    
                except Exception as e:
                    self.logger.error(f"Error processing category page {page_num}: {str(e)}")
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.info(f"Finished browsing category. Total unique URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error browsing category: {str(e)}")
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
        """Scrape single product from www.jimellismazdaparts.com with proper sequential event waiting"""
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
                    time.sleep(random.uniform(1.0, 2.0))
                    
                    if self.has_cloudflare_challenge():
                        cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                        if not cloudflare_bypassed:
                            retry_count += 1
                            if retry_count < max_retries:
                                time.sleep(random.uniform(10, 15))
                                continue
                            else:
                                return None
                    
                    # Wait for page to be fully loaded
                    time.sleep(random.uniform(2.0, 3.0))
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Check if page loaded correctly
                    title_elem = soup.find('span', class_='prodDescriptH2')
                    if not title_elem:
                        title_elem = soup.find('h1')
                    if not title_elem:
                        title_tag = soup.find('title')
                        if title_tag:
                            title_text = title_tag.get_text(strip=True)
                            if '|' in title_text:
                                title_text = title_text.split('|')[0].strip()
                        else:
                            title_text = ''
                    else:
                        title_text = title_elem.get_text(strip=True)
                    
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
        
        # Re-parse HTML after all interactions
        html = self.driver.page_source
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
            # Extract title - Structure: <span class="prodDescriptH2">Wheel</span>
            title_elem = soup.find('span', class_='prodDescriptH2')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                h1_elem = soup.find('h1')
                if h1_elem:
                    span_elem = h1_elem.find('span', class_='prodDescriptH2')
                    if span_elem:
                        product_data['title'] = span_elem.get_text(strip=True)
                    else:
                        product_data['title'] = h1_elem.get_text(strip=True)
            
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
            
            # Extract PN (Part Number) - from URL or page: 9965138500
            # URL pattern: /products/Mazda/Wheel/15384954/9965138500.html
            url_match = re.search(r'/products/Mazda/[^/]+/\d+/([^/]+)\.html', url)
            if url_match:
                product_data['pn'] = url_match.group(1)
            
            # Also try from page: <span itemprop="value" class="body-3 stock-code-text"><strong>9965138500</strong></span>
            if not product_data['pn']:
                pn_span = soup.find('span', {'itemprop': 'value', 'class': lambda x: x and 'stock-code-text' in ' '.join(x) if isinstance(x, list) else 'stock-code-text' in str(x)})
                if pn_span:
                    strong_elem = pn_span.find('strong')
                    if strong_elem:
                        product_data['pn'] = strong_elem.get_text(strip=True)
                    else:
                        product_data['pn'] = pn_span.get_text(strip=True)
            
            # Extract SKU - from supersession: "9965-13-8500; ; 9965-19-8500" -> "9965-13-8500"
            # Structure: <span class="body-3 alt-stock-code-text"><strong>9965-13-8500; ; 9965-19-8500</strong></span>
            sku_span = soup.find('span', class_=lambda x: x and 'alt-stock-code-text' in ' '.join(x) if isinstance(x, list) else 'alt-stock-code-text' in str(x))
            if sku_span:
                strong_elem = sku_span.find('strong')
                if strong_elem:
                    supersession_text = strong_elem.get_text(strip=True)
                    # Extract first part before semicolon
                    if ';' in supersession_text:
                        product_data['sku'] = supersession_text.split(';')[0].strip()
                    else:
                        product_data['sku'] = supersession_text.strip()
            
            # Extract Replaces - from supersession: "9965-13-8500; ; 9965-19-8500" -> "9965-19-8500"
            if sku_span:
                strong_elem = sku_span.find('strong')
                if strong_elem:
                    supersession_text = strong_elem.get_text(strip=True)
                    # Extract parts after first semicolon
                    parts = [p.strip() for p in supersession_text.split(';') if p.strip()]
                    if len(parts) > 1:
                        # Take the last non-empty part as replaces
                        for part in reversed(parts):
                            if part and part != product_data.get('sku', ''):
                                product_data['replaces'] = part
                                break
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract AC$ (actual price) - Structure: <span class="productPriceSpan money-3">$ 651.87</span>
            # Extract just the number: "651.87"
            price_span = soup.find('span', class_=lambda x: x and 'productPriceSpan' in ' '.join(x) if isinstance(x, list) else 'productPriceSpan' in str(x))
            if price_span:
                price_text = price_span.get_text(strip=True)
                # Remove $ and spaces, extract number
                price_value = self.extract_price(price_text)
                if price_value:
                    product_data['actual_price'] = price_value
            
            # Extract MSRP - Structure: MSRP: $ 804.94 -> "804.94"
            msrp_div = soup.find('div', class_=lambda x: x and 'msrpRow' in ' '.join(x) if isinstance(x, list) else 'msrpRow' in str(x))
            if msrp_div:
                msrp_text = msrp_div.get_text(strip=True)
                # Remove "MSRP: " prefix and extract number
                msrp_text = re.sub(r'^MSRP:\s*\$?\s*', '', msrp_text, flags=re.IGNORECASE)
                msrp_value = self.extract_price(msrp_text)
                if msrp_value:
                    product_data['msrp'] = msrp_value
            
            # Extract image - Structure: <img itemprop="image" src="https://images.simplepart.net/...">
            img_elem = soup.find('img', {'itemprop': 'image'})
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src')
                if img_url:
                    if img_url.startswith('//'):
                        product_data['image_url'] = f"https:{img_url}"
                    elif img_url.startswith('/'):
                        product_data['image_url'] = f"{self.base_url}{img_url}"
                    else:
                        product_data['image_url'] = img_url
            
            # Extract description - Structure: <p>Disc. <br>20" bright. A wheel / rim...</p>
            desc_div = soup.find('div', class_=lambda x: x and 'item-desc' in ' '.join(x) if isinstance(x, list) else 'item-desc' in str(x))
            if desc_div:
                desc_paragraphs = desc_div.find_all('p')
                desc_texts = []
                for p in desc_paragraphs:
                    p_text = p.get_text(strip=True, separator=' ')
                    if p_text and len(p_text) > 10:
                        desc_texts.append(p_text)
                if desc_texts:
                    product_data['description'] = ' '.join(desc_texts)
                    # Clean up description
                    product_data['description'] = re.sub(r'\s+', ' ', product_data['description']).strip()
            
            # Extract fitment - CRITICAL: Must click "What This Fits" tab, wait, click "Show More", wait, then extract
            fitment_rows_elements = []
            selenium_extraction_success = False
            
            if self.driver:
                try:
                    self.logger.info("🔍 Step 1: Clicking 'What This Fits' tab...")
                    
                    # Step 1: Find and click the "What This Fits" tab
                    wait = WebDriverWait(self.driver, 15)
                    tab_selectors = [
                        (By.ID, 'fitmentTab'),
                        (By.CSS_SELECTOR, 'a[href="#fitments"]'),
                        (By.CSS_SELECTOR, 'li#ctl00_Content_PageBody_ProductTabsLegacy_fitmentTabLI a'),
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
                            self.logger.debug(f"Tab selector {selector_value} failed: {e}")
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
                        
                        # Step 5: Extract fitment data from fully loaded page
                        self.logger.info("🔍 Step 5: Extracting fitment data...")
                        
                        try:
                            # Get updated HTML after all interactions
                            html = self.driver.page_source
                            soup = BeautifulSoup(html, 'lxml')
                            
                            # Find all fitment rows using Selenium (more reliable for dynamic content)
                            fitment_row_elements = []
                            
                            selectors_to_try = [
                                'div#ctl00_Content_PageBody_ProductTabsLegacy_div_applicationListContainer table tbody tr',
                                'div#WhatThisFitsTabComponent_TABPANEL div.col-lg-12',
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
                            
                            if fitment_row_elements:
                                self.logger.info(f"✓ Found {len(fitment_row_elements)} fitment rows")
                                fitment_rows_elements = fitment_row_elements
                                selenium_extraction_success = True
                            else:
                                self.logger.warning("⚠️ No fitment rows found")
                            
                        except Exception as e:
                            self.logger.warning(f"⚠️ Error in fitment extraction steps: {str(e)}")
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Error interacting with fitment tab: {str(e)}")
            
            # Extract fitment data from Selenium elements or fallback to HTML
            if selenium_extraction_success and fitment_rows_elements:
                self.logger.info(f"🔍 Processing {len(fitment_rows_elements)} fitment rows...")
                for idx, row_element in enumerate(fitment_rows_elements):
                    try:
                        row_html = row_element.get_attribute('outerHTML')
                        if not row_html:
                            continue
                        
                        row_soup = BeautifulSoup(row_html, 'lxml')
                        
                        # Handle table structure
                        if row_soup.name == 'tr' or row_soup.find('tr'):
                            tds = row_soup.find_all('td')
                            if len(tds) >= 1:
                                vehicle_text = tds[0].get_text(strip=True)
                                years = []
                                if len(tds) >= 2:
                                    years_cell = tds[1]
                                    year_links = years_cell.find_all('a', href=True)
                                    for link in year_links:
                                        href = link.get('href', '')
                                        year_match = re.search(r'/products/Mazda/(\d{4})', href)
                                        if year_match:
                                            years.append(year_match.group(1))
                                        else:
                                            link_text = link.get_text(strip=True)
                                            if link_text and link_text.isdigit() and len(link_text) == 4:
                                                years.append(link_text)
                                    if not years:
                                        years_text = years_cell.get_text(strip=True)
                                        year_matches = re.findall(r'\b(\d{4})\b', years_text)
                                        years = [y for y in year_matches if 1900 <= int(y) <= 2100]
                                
                                if vehicle_text:
                                    make = 'Mazda'
                                    parse_text = vehicle_text.replace('Mazda ', '').strip()
                                    words = parse_text.split()
                                    model = words[0] if words else ''
                                    
                                    # Extract engine and trim from vehicle description
                                    engine = ''
                                    trim = ''
                                    
                                    # Pattern 1: Extract engine (e.g., "2.0L", "3.7L V6", "2.5L I4", "2.0L SKYACTIV-G")
                                    # Also match with transmission (A/T, M/T, CVT, AUTO, MANUAL)
                                    engine_match = re.search(r'(\d+\.?\d*L\s*(?:V\d|I\d|SKYACTIV-[A-Z]+)?(?:\s*MILD HYBRID EV-GAS \(MHEV\))?(?:\s*EV-GAS \(MHEV\))?(?:\s*GAS)?(?:\s*ELECTRIC)?(?:\s*A/T|\s*M/T|\s*CVT|\s*AUTO|\s*MANUAL)?)', parse_text, re.IGNORECASE)
                                    if engine_match:
                                        engine = engine_match.group(1).strip()
                                        # Remove engine part from parse_text to isolate trim
                                        engine_start = parse_text.find(engine)
                                        engine_end = engine_start + len(engine)
                                        remaining_text = parse_text[engine_end:].strip()
                                        
                                        # Extract trim from remaining text
                                        # Trim is typically after transmission and drivetrain (AWD/FWD/RWD)
                                        # Common Mazda trim patterns: Touring, Grand Touring, Sport, Base, Signature, Carbon Edition, etc.
                                        
                                        # Pattern 1: Look for trim after AWD/FWD/RWD
                                        trim_match = re.search(r'(?:AWD|FWD|RWD|4WD)\s+([A-Za-z0-9\s]+?)(?:\s+(?:Sedan|SUV|Hatchback|Coupe|Convertible|Wagon|Truck|Crossover)|$)', remaining_text, re.I)
                                        if trim_match:
                                            potential_trim = trim_match.group(1).strip()
                                            # Clean up common words that aren't trim
                                            trim_words_to_remove = ['AWD', 'FWD', 'RWD', '4WD', 'A/T', 'M/T', 'CVT', 'AUTO', 'MANUAL']
                                            for word in trim_words_to_remove:
                                                potential_trim = re.sub(r'\b' + re.escape(word) + r'\b', '', potential_trim, flags=re.I).strip()
                                            if potential_trim:
                                                trim = potential_trim
                                        
                                        # Pattern 2: Look for common Mazda trim keywords anywhere in remaining text
                                        if not trim:
                                            mazda_trim_keywords = [
                                                'Touring', 'Grand Touring', 'GT', 'Sport', 'Base', 'Signature', 
                                                'Carbon Edition', 'Carbon', 'Club', 'Miata', 'MX-5', 'CX', 
                                                'Preferred', 'Premium', 'Luxury', 'Limited', 'Edition', 'SE', 'LE'
                                            ]
                                            for trim_keyword in mazda_trim_keywords:
                                                trim_pattern = r'\b' + re.escape(trim_keyword) + r'\b'
                                                if re.search(trim_pattern, remaining_text, re.I):
                                                    trim = trim_keyword
                                                    break
                                        
                                        # Pattern 3: If no specific trim found, take everything after engine/transmission/drivetrain
                                        if not trim:
                                            # Remove drivetrain and body type to get potential trim
                                            trim_candidate = re.sub(r'\s*(?:AWD|FWD|RWD|4WD)\s*', ' ', remaining_text, flags=re.I)
                                            trim_candidate = re.sub(r'\s*(?:Sedan|SUV|Hatchback|Coupe|Convertible|Wagon|Truck|Crossover)\s*$', '', trim_candidate, flags=re.I)
                                            trim_candidate = trim_candidate.strip()
                                            if trim_candidate and len(trim_candidate) > 0:
                                                trim = trim_candidate
                                    else:
                                        # No engine pattern found, try to extract trim from common patterns
                                        # Look for trim keywords in the entire parse_text
                                        mazda_trim_keywords = [
                                            'Touring', 'Grand Touring', 'GT', 'Sport', 'Base', 'Signature', 
                                            'Carbon Edition', 'Carbon', 'Club', 'Miata', 'MX-5', 'CX', 
                                            'Preferred', 'Premium', 'Luxury', 'Limited', 'Edition', 'SE', 'LE'
                                        ]
                                        for trim_keyword in mazda_trim_keywords:
                                            trim_pattern = r'\b' + re.escape(trim_keyword) + r'\b'
                                            if re.search(trim_pattern, parse_text, re.I):
                                                trim = trim_keyword
                                                break
                                        
                                        # If still no trim, and we have more than just model, use rest as trim
                                        if not trim and len(words) > 1:
                                            # Skip model and common drivetrain/body type words
                                            remaining_words = words[1:]
                                            filtered_words = []
                                            skip_words = ['AWD', 'FWD', 'RWD', '4WD', 'A/T', 'M/T', 'CVT', 'AUTO', 'MANUAL', 
                                                          'Sedan', 'SUV', 'Hatchback', 'Coupe', 'Convertible', 'Wagon', 'Truck', 'Crossover']
                                            for word in remaining_words:
                                                if word.upper() not in [w.upper() for w in skip_words]:
                                                    filtered_words.append(word)
                                            if filtered_words:
                                                trim = ' '.join(filtered_words).strip()
                                    
                                    if years:
                                        for year in years:
                                            product_data['fitments'].append({
                                                'year': year,
                                                'make': make,
                                                'model': model,
                                                'trim': trim,
                                                'engine': engine
                                            })
                                    else:
                                        product_data['fitments'].append({
                                            'year': '',
                                            'make': make,
                                            'model': model,
                                            'trim': trim,
                                            'engine': engine
                                        })
                    except Exception as row_error:
                        self.logger.debug(f"Error parsing fitment row {idx+1}: {str(row_error)}")
                        continue
            
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

