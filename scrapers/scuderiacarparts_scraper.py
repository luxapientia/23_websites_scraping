"""Scraper for scuderiacarparts.com"""
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

class ScuderiaCarPartsScraper(BaseScraper):
    """Scraper for scuderiacarparts.com"""
    
    def __init__(self):
        super().__init__('scuderiacarparts', use_selenium=True)
        self.base_url = 'https://www.scuderiacarparts.com'
        
    def get_product_urls(self):
        """
        Get all wheel product URLs from scuderiacarparts.com
        Uses search functionality with "Load more results" button handling
        """
        product_urls = []
        
        try:
            self.logger.info("Searching for wheel products on scuderiacarparts.com...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            product_urls = list(set(product_urls))  # Remove duplicates
            
            self.logger.info(f"Found {len(product_urls)} unique wheel product URLs")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """
        Search for wheels using site search - handles "Load more results" button
        Continuously clicks the button until all products are loaded, then extracts all product URLs
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Use the search URL provided by the user
            search_url = f"{self.base_url}/search/?stc=RM8&sac=N&q=wheel&params=eyJtYXNlcmF0aSI6bnVsbCwiYmVudGxleSI6bnVsbCwibGFuZHJvdmVyIjpudWxsLCJ0eXBlIjpbIk9yaWdpbmFsIFBhcnRzIiwiVHVuaW5nIFBhcnRzIl0sImFzdG9ubWFydGluIjpudWxsLCJhdWRpIjpudWxsLCJibXciOm51bGwsImZlcnJhcmkiOm51bGwsImhvbmRhIjpudWxsLCJsYW1ib3JnaGluaSI6bnVsbCwibWNsYXJlbiI6bnVsbCwibWVyY2VkZXMiOm51bGwsIm5pc3NhbiI6bnVsbCwicG9yc2NoZSI6bnVsbCwicm9sbHNyb3ljZSI6bnVsbCwidGVzbGEiOm51bGx9"
            self.logger.info(f"Loading search page: {search_url}")
            
            # Load the search page
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                
                self.driver.get(search_url)
                time.sleep(3)  # Wait for initial page load
                
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
            
            # Wait for search results to load - try multiple selectors
            self.logger.info("Waiting for search results to load...")
            try:
                # Try multiple selectors for product links
                product_link_selectors = [
                    (By.CSS_SELECTOR, "a[href*='/product/']"),
                    (By.CSS_SELECTOR, "a[href*='/item/']"),
                    (By.CSS_SELECTOR, "a[href*='/p/']"),
                    (By.CSS_SELECTOR, "a[href*='/products/']"),
                    (By.CSS_SELECTOR, "a[class*='product']"),
                    (By.CSS_SELECTOR, "div[class*='product'] a"),
                    (By.XPATH, "//a[contains(@href, '/product') or contains(@href, '/item') or contains(@href, '/p/')]"),
                ]
                
                found = False
                for by_type, selector in product_link_selectors:
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((by_type, selector))
                        )
                        self.logger.info(f"✓ Found product links using selector: {selector[:50]}")
                        found = True
                        break
                    except:
                        continue
                
                if not found:
                    self.logger.warning("Product links not found with standard selectors, continuing anyway...")
                    # Wait a bit more for page to fully load
                    time.sleep(3)
            except Exception as e:
                self.logger.warning(f"Error waiting for product links: {str(e)}, continuing anyway...")
                time.sleep(3)
            
            # Handle "Load more results" button - click repeatedly until all products are loaded
            self.logger.info("Handling 'Load more results' button...")
            load_more_clicked = 0
            max_load_more_clicks = 1000  # Safety limit
            consecutive_no_button = 0
            max_consecutive_no_button = 3  # Stop after 3 consecutive attempts with no button
            
            while load_more_clicked < max_load_more_clicks:
                try:
                    # Try multiple selectors for the "Load more results" button
                    load_more_selectors = [
                        "button:contains('Load more results')",
                        "button:contains('Load More')",
                        "a:contains('Load more results')",
                        "a:contains('Load More')",
                        ".load-more",
                        "#load-more",
                        "button.load-more",
                        "a.load-more",
                        "button[class*='load']",
                        "a[class*='load']",
                        "button[data-action='load-more']",
                        "a[data-action='load-more']",
                    ]
                    
                    load_more_button = None
                    
                    # First, try to find by text content (most reliable)
                    try:
                        # Use XPath to find button/link containing "Load more" text (case-insensitive)
                        load_more_button = self.driver.find_element(
                            By.XPATH, 
                            "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more')] | //a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more')]"
                        )
                    except NoSuchElementException:
                        # Try CSS selectors
                        for selector in load_more_selectors:
                            try:
                                if selector.startswith("button:") or selector.startswith("a:"):
                                    # These are jQuery-style selectors, skip for now
                                    continue
                                load_more_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                                if load_more_button:
                                    break
                            except NoSuchElementException:
                                continue
                    
                    if load_more_button:
                        # Check if button is visible and enabled
                        if load_more_button.is_displayed() and load_more_button.is_enabled():
                            # Scroll to button to ensure it's in view
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", load_more_button)
                            time.sleep(1)  # Wait for scroll
                            
                            # Click the button
                            try:
                                load_more_button.click()
                                load_more_clicked += 1
                                consecutive_no_button = 0  # Reset counter
                                self.logger.info(f"✓ Clicked 'Load more results' button (click #{load_more_clicked})")
                                
                                # Wait for new products to load
                                time.sleep(3)  # Wait for AJAX/content to load
                                
                                # Optional: Scroll down a bit to trigger lazy loading if any
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(1)
                                
                            except Exception as click_error:
                                self.logger.warning(f"Error clicking load more button: {str(click_error)}")
                                # Try JavaScript click as fallback
                                try:
                                    self.driver.execute_script("arguments[0].click();", load_more_button)
                                    load_more_clicked += 1
                                    consecutive_no_button = 0
                                    self.logger.info(f"✓ Clicked 'Load more results' button via JavaScript (click #{load_more_clicked})")
                                    time.sleep(3)
                                except Exception as js_click_error:
                                    self.logger.warning(f"JavaScript click also failed: {str(js_click_error)}")
                                    consecutive_no_button += 1
                                    if consecutive_no_button >= max_consecutive_no_button:
                                        self.logger.info("No more 'Load more results' button found or button not clickable")
                                    break
                        else:
                            # Button exists but not visible/enabled - likely all products loaded
                            self.logger.info("'Load more results' button found but not visible/enabled - all products likely loaded")
                            consecutive_no_button += 1
                            if consecutive_no_button >= max_consecutive_no_button:
                                break
                    else:
                        # No button found - all products likely loaded
                        consecutive_no_button += 1
                        if consecutive_no_button >= max_consecutive_no_button:
                            self.logger.info("No 'Load more results' button found - all products loaded")
                            break
                        else:
                            self.logger.debug(f"Button not found (attempt {consecutive_no_button}/{max_consecutive_no_button})")
                            time.sleep(1)  # Brief wait before checking again
                    
                except Exception as e:
                    self.logger.warning(f"Error while handling 'Load more results' button: {str(e)}")
                    consecutive_no_button += 1
                    if consecutive_no_button >= max_consecutive_no_button:
                            break
                    time.sleep(1)
                    
            self.logger.info(f"Finished clicking 'Load more results' button ({load_more_clicked} clicks)")
                    
            # Final scroll to ensure all lazy-loaded content is visible
            self.logger.info("Performing final scroll to load any lazy-loaded content...")
            try:
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                scroll_attempts = 0
                max_scrolls = 20
                no_change_count = 0
                
                while scroll_attempts < max_scrolls:
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
            except Exception as scroll_error:
                self.logger.warning(f"Error during final scroll: {str(scroll_error)}")
            
            # Extract all product URLs from the fully loaded page
            self.logger.info("Extracting product URLs from fully loaded page...")
            try:
                html = self.driver.page_source
            except Exception as page_source_error:
                self.logger.error(f"Error accessing page_source: {str(page_source_error)}")
                return product_urls
            
            soup = BeautifulSoup(html, 'lxml')
            
            # First, try to find product links using Selenium (more reliable for dynamic content)
            self.logger.info("Extracting product URLs using Selenium...")
            try:
                # Try multiple selectors to find product links
                selenium_selectors = [
                    "a[href*='/product/']",
                    "a[href*='/item/']",
                    "a[href*='/p/']",
                    "a[href*='/products/']",
                    "a[class*='product']",
                    "div[class*='product'] a",
                    "div[class*='item'] a",
                    "a[data-product-id]",
                    "a[data-item-id]",
                ]
                
                for selector in selenium_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            self.logger.info(f"Found {len(elements)} potential product links using selector: {selector}")
                            for elem in elements:
                                try:
                                    href = elem.get_attribute('href')
                                    if href:
                                        # Normalize URL
                                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                        
                                        # Remove fragment (#)
                                        if '#' in full_url:
                                            full_url = full_url.split('#')[0]
                                        
                                        # Remove query params that might be session-specific
                                        if '?' in full_url:
                                            base_url = full_url.split('?')[0]
                                            full_url = base_url
                                        
                                        # Normalize trailing slashes
                                        full_url = full_url.rstrip('/')
                                        
                                        # Only add if it's a product URL (not search, category, etc.)
                                        if full_url not in product_urls and '/product/' in full_url.lower():
                                            product_urls.append(full_url)
                                except Exception as e:
                                    self.logger.debug(f"Error extracting href from element: {str(e)}")
                                    continue
                    
                            if product_urls:
                                break  # Found products, no need to try other selectors
                    except Exception as e:
                        self.logger.debug(f"Error with selector {selector}: {str(e)}")
                        continue
            except Exception as e:
                self.logger.warning(f"Error extracting product URLs with Selenium: {str(e)}")
            
            # Fallback: Use BeautifulSoup to find product links
            if not product_urls:
                self.logger.info("No product URLs found with Selenium, trying BeautifulSoup...")
                # Find product links - try multiple patterns common on e-commerce sites
                product_link_patterns = [
                    re.compile(r'/product/'),
                    re.compile(r'/item/'),
                    re.compile(r'/p/'),
                    re.compile(r'/products/'),
                    re.compile(r'/catalog/'),
                ]
                
                all_links = soup.find_all('a', href=True)
                self.logger.info(f"Found {len(all_links)} total links in page, checking for product URLs...")
                
                for link in all_links:
                    href = link.get('href', '')
                    if not href:
                            continue
                    
                    # Check if this looks like a product URL
                    is_product_url = any(pattern.search(href) for pattern in product_link_patterns)
                    
                    if is_product_url:
                        # Normalize URL
                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                        
                        # Remove fragment (#)
                        if '#' in full_url:
                            full_url = full_url.split('#')[0]
                        
                        # Remove query params that might be session-specific
                        if '?' in full_url:
                            # Keep the base product URL, remove query params
                            base_url = full_url.split('?')[0]
                            full_url = base_url
                            
                        # Normalize trailing slashes
                        full_url = full_url.rstrip('/')
                        
                        # Exclude search URLs and other non-product pages
                        if '/search/' not in full_url.lower() and '/category/' not in full_url.lower():
                            if full_url not in product_urls:
                                product_urls.append(full_url)
                
                self.logger.info(f"Extracted {len(product_urls)} unique product URLs using BeautifulSoup")
            
            # Log sample URLs for debugging
            if product_urls:
                self.logger.info(f"Sample product URLs found: {product_urls[:3]}")
            else:
                self.logger.warning("⚠️ No product URLs found! Checking page content...")
                # Debug: log some sample links to see what's on the page
                try:
                    sample_links = soup.find_all('a', href=True, limit=10)
                    self.logger.info(f"Sample links found on page:")
                    for i, link in enumerate(sample_links[:5]):
                        href = link.get('href', '')[:100]
                        text = link.get_text(strip=True)[:50]
                        self.logger.info(f"  {i+1}. {href} - {text}")
                except:
                    pass
            
            self.logger.info(f"Total unique product URLs extracted: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    def scrape_product(self, url):
        """
        Scrape single product from ScuderiaCarParts with retry logic
        """
        import random
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        
        # Import TimeoutException - try to import it
        try:
            from selenium.common.exceptions import TimeoutException
        except ImportError:
            # If import fails, create a dummy class for type checking
            TimeoutException = type('TimeoutException', (Exception,), {})
        
        max_retries = 5
        retry_count = 0
        html = None
        
        while retry_count < max_retries:
            try:
                # Check health before proceeding
                if not self.check_health():
                    self.logger.error("Scraper health check failed, stopping")
                    return None
                
                self.logger.info(f"Loading product page (attempt {retry_count + 1}/{max_retries}): {url}")
                
                # Ensure driver is valid before loading page
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
                    self.driver.get(url)
                    
                    # Wait a bit before accessing page_source (more human-like)
                    time.sleep(random.uniform(0.5, 1.5))  # Added delay to avoid immediate page_source access
                    
                    # Quick Cloudflare check - only if on challenge URL or page is very small
                    current_url_check = self.driver.current_url.lower()
                    page_preview = self.driver.page_source[:6000]
                    
                    if ('challenges.cloudflare.com' in current_url_check or '/cdn-cgi/challenge' in current_url_check or len(page_preview) < 5000) and self.has_cloudflare_challenge():
                        self.logger.info("🛡️ Cloudflare challenge detected - waiting for bypass...")
                        cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                        if not cloudflare_bypassed:
                            # Quick final check - if page has content, continue anyway
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
                        time.sleep(1)  # Brief wait after bypass
                    
                    time.sleep(random.uniform(0.5, 1.0))  # Increased delay for more human-like timing
                    
                    # Simulate human behavior - scroll and wait (more realistic)
                    self.simulate_human_behavior()
                    time.sleep(random.uniform(0.5, 1.0))  # Increased delay for more human-like timing
                    
                    # Wait for page to fully load - realistic timing
                    time.sleep(random.uniform(1.5, 3.0))  # Increased to 1.5-3s for more human-like behavior
                    
                    # Wait for product title to load using WebDriverWait - realistic timeout
                    # Add delay before accessing page_source (more human-like)
                    time.sleep(random.uniform(0.5, 1.0))
                    html = self.driver.page_source  # Get HTML once for multiple checks
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Try multiple selectors for title extraction
                    title_text = ''
                    title_selectors = [
                        ("h1.product-title", By.CSS_SELECTOR),
                        ("h1", By.CSS_SELECTOR),
                        ("h2.product-title", By.CSS_SELECTOR),
                        ("[data-product-title]", By.CSS_SELECTOR),
                        (".product-name", By.CSS_SELECTOR),
                    ]
                    
                    # Try Selenium first (more reliable for dynamic content)
                    for selector, by_type in title_selectors:
                        try:
                            wait = WebDriverWait(self.driver, 5)  # Shorter timeout per selector
                            if by_type == By.CSS_SELECTOR:
                                title_element = wait.until(EC.presence_of_element_located((by_type, selector)))
                            else:
                                continue
                            title_text = title_element.text.strip()
                            if title_text and len(title_text) >= 3:
                                break
                        except:
                            continue
                    
                    # If Selenium failed, try BeautifulSoup
                    if not title_text or len(title_text) < 3:
                        title_elem = (soup.find('h1', class_='product-title') or 
                                     soup.find('h1') or 
                                     soup.find('h2', class_='product-title') or
                                     soup.find('div', class_='product-title') or
                                     soup.find('span', class_='product-title'))
                        title_text = title_elem.get_text(strip=True) if title_elem else ''
                        
                        # If still no title, try page title tag as last resort
                        if not title_text or len(title_text) < 3:
                            page_title_tag = soup.find('title')
                            if page_title_tag:
                                title_text = page_title_tag.get_text(strip=True)
                                # Clean up title tag (remove site name, etc.)
                                if '|' in title_text:
                                    title_text = title_text.split('|')[0].strip()
                                if '-' in title_text and len(title_text.split('-')) > 2:
                                    # Take the part before the last dash (usually product name)
                                    parts = title_text.split('-')
                                    title_text = '-'.join(parts[:-1]).strip()
                    
                    # REMOVED: Second Cloudflare check after page load
                    # If page already loaded successfully with content and title, there's no Cloudflare challenge
                    # This was causing false positives and unnecessary delays
                    
                    # Quick error check - only critical errors
                    current_url = self.driver.current_url.lower()
                    title_lower = title_text.lower() if title_text else ''
                    
                    # Check if redirected away from target domain (critical)
                    if 'scuderiacarparts.com' not in current_url and not current_url.startswith(('chrome-error://', 'about:')):
                        self.logger.warning(f"⚠️ Redirected away from target site: {current_url}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(10, 15)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    
                    # Check for critical error pages - improved detection to avoid false positives
                    # Only check for actual error codes in URL path/query, not in product part numbers
                    # Product URLs can contain "403" or "404" as part numbers (e.g., "40300d4025")
                    critical_error_patterns = [
                        'chrome-error://',
                        'err_connection',
                        'dns_probe',
                        '/404',  # Error code in path
                        '/403',  # Error code in path
                        '?404',  # Error code in query
                        '?403',  # Error code in query
                        'error=404',
                        'error=403',
                        'status=404',
                        'status=403',
                    ]
                    # Only check if error pattern appears as actual error indicator, not in product part numbers
                    has_critical_error = any(pattern in current_url for pattern in critical_error_patterns)
                    
                    # Additional check: if URL contains error codes but also has product path patterns, it's likely a product page, not an error
                    product_path_patterns = ['/product/', '/item/', '/p/', '/products/', '/catalog/']
                    if has_critical_error and any(pattern in current_url for pattern in product_path_patterns):
                        # This is likely a product page with part number containing error codes
                        has_critical_error = False
                    
                    # Check if page has substantial content (indicates it's not blocked)
                    page_content_length = len(html)
                    has_substantial_content = page_content_length > 8000  # At least 8KB of content
                    
                    # Check for product-specific elements to verify it's a product page
                    product_indicators = [
                        soup.find('span', class_=re.compile(r'sku|part.*number', re.I)),
                        soup.find('div', class_=re.compile(r'product.*price|price.*product', re.I)),
                        soup.find('div', class_=re.compile(r'product.*info|product.*details', re.I)),
                        soup.find('button', class_=re.compile(r'add.*cart|buy.*now', re.I)),
                    ]
                    has_product_elements = any(product_indicators)
                    
                    # Only treat as blocked if:
                    # 1. Has critical error URL (AND not a product page), OR
                    # 2. (Title is just domain OR empty) AND (page is small AND no product elements)
                    # IMPORTANT: If page has substantial content AND product elements, it's NOT blocked even if title is empty
                    title_is_domain = title_text.lower() in ['www.tascaparts.com', 'tascaparts.com', 'tascaparts', '']
                    # Only mark as blocked if BOTH conditions: small content AND no product elements
                    # If page has substantial content OR product elements, it's likely a valid page
                    is_likely_blocked = title_is_domain and (not has_substantial_content and not has_product_elements)
                    
                    # CRITICAL FIX: Don't mark as error if page has valid product content
                    # Even if critical_error is detected, if page has substantial content AND product elements AND valid title,
                    # it's a valid product page (part numbers can contain "403", "404", etc.)
                    if has_critical_error and has_substantial_content and has_product_elements and title_text and len(title_text) > 3:
                        # This is a valid product page, not an error page
                        has_critical_error = False
                        self.logger.debug(f"False positive error detection avoided: valid product page with part number containing error code")
                    
                    is_error_page = has_critical_error or is_likely_blocked
                    
                    if is_error_page:
                        error_type = "error page" if has_critical_error else "blocked"
                        self.logger.warning(f"⚠️ {error_type.capitalize()} on attempt {retry_count + 1}, title: '{title_text}', content: {page_content_length} bytes, has_product_elements: {has_product_elements}")
                        retry_count += 1
                        if retry_count < max_retries:
                            # Anti-blocking cooldown: wait significantly longer when blocked
                            # If blocked, wait much longer to avoid rate limiting
                            if error_type == "blocked":
                                # Blocked = wait 30-60 seconds (progressive, increased for anti-blocking)
                                base_wait = 30 + (retry_count * 10)  # 30s, 40s, 50s, 60s, 70s
                                wait_time = random.uniform(base_wait, base_wait + 15)
                                self.logger.warning(f"⚠️ BLOCKED - Extended cooldown: {wait_time:.1f} seconds before retry...")
                            else:
                                # Error page = wait 10-15 seconds
                                wait_time = random.uniform(10, 15)
                                self.logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                            
                            time.sleep(wait_time)
                            
                            # If blocked multiple times, add extra human-like behavior
                            if error_type == "blocked" and retry_count >= 2:
                                self.logger.info("Simulating extended human behavior after blocking...")
                                time.sleep(random.uniform(10, 20))  # Increased for anti-blocking
                                self.simulate_human_behavior()
                            
                            continue
                        else:
                            self.logger.error(f"❌ Failed after {max_retries} attempts - {error_type}")
                            return None
                    
                    # Success! Got real product page
                    html = self.driver.page_source
                    self.logger.info(f"✓ Page loaded successfully, title: {title_text[:50]}")
                    break
                    
                except TimeoutException as e:
                    # Page load timeout - wait longer for FULL content instead of accepting partial
                    self.logger.warning(f"⚠️ Page load timeout (30s) - waiting longer for FULL content...")
                    
                    # Increase timeout temporarily and wait for full content
                    original_timeout = self.page_load_timeout  # Use stored value
                    self.page_load_timeout = 60
                    self.driver.set_page_load_timeout(60)  # Increase to 60 seconds
                    
                    try:
                        # Wait for content to fully load (optimized for speed)
                        wait_for_content = 8  # Reduced from 15s to 8s for faster response
                        waited = 0
                        content_ready = False
                        
                        while waited < wait_for_content:
                            html = self.driver.page_source
                            current_url = self.driver.current_url.lower()
                            
                            # Check if we got an error page
                            if any(err in current_url for err in ['chrome-error://', 'err_', 'dns_probe']):
                                raise Exception(f"Connection error after timeout: {current_url}")
                            
                            # Require substantial content (not just partial) - reduced threshold
                            if html and len(html) > 8000:  # Reduced from 10KB to 8KB for faster detection
                                # Check for key product elements to ensure content is complete
                                soup = BeautifulSoup(html, 'lxml')
                                has_title = soup.find('h1', class_='product-title') or soup.find('h1')
                                has_body = soup.find('body')
                                body_text = has_body.get_text(strip=True) if has_body else ''
                                
                                # Content is ready if it has title or substantial body content (reduced thresholds)
                                if (has_title or len(body_text) > 500) and len(body_text) > 300:  # Reduced thresholds
                                    content_ready = True
                                    self.logger.info(f"✓ Full content loaded after additional {waited}s wait ({len(html)} chars)")
                                    break
                            
                            if waited % 2 == 0:  # Log every 2s instead of 3s
                                self.logger.info(f"Waiting for full content... ({waited}s/{wait_for_content}s)")
                            
                            time.sleep(0.5)  # Check every 0.5s instead of 1s for faster response
                            waited += 0.5
                        
                        if not content_ready:
                            raise Exception("Full content not loaded after extended wait")
                            
                    except Exception as e:
                        self.logger.warning(f"Could not get full content: {str(e)}")
                        # Restore timeout
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(5, 8)
                            self.logger.info(f"Retrying in {wait_time:.1f} seconds...")
                            time.sleep(wait_time)
                            continue
                        else:
                            self.logger.error(f"❌ Failed after {max_retries} timeout attempts")
                            return None
                    finally:
                        # Restore original timeout
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                except Exception as e:
                    # Connection errors, network errors, etc.
                    error_str = str(e).lower()
                    if any(err in error_str for err in ['connection', 'network', 'dns', 'err_', 'timeout']):
                        self.logger.warning(f"⚠️ Connection/network error on attempt {retry_count + 1}: {e}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(5, 8)  # Optimized: reduced from 12-18s
                            self.logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            self.logger.error(f"❌ Failed after {max_retries} attempts - connection error")
                            return None
                    else:
                        # Re-raise other exceptions
                        raise
                
            except TimeoutException as e:
                # Use error handler for timeout
                recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                
                if recovery['should_retry'] and retry_count < max_retries - 1:
                    wait_time = recovery['wait_time']
                    delay = random.uniform(wait_time[0], wait_time[1])
                    self.logger.warning(f"Timeout error, retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    retry_count += 1
                    continue
                else:
                    self.health_status['consecutive_failures'] += 1
                    self.health_status['last_failure_time'] = datetime.now()
                    self.logger.error(f"❌ Failed after {max_retries} timeout attempts")
                    return None
                    
            except Exception as e:
                # Log the actual exception first to see what's failing
                self.logger.error(f"❌ Exception occurred while loading product page: {type(e).__name__}: {str(e)}")
                self.logger.debug(f"Full traceback:\n{traceback.format_exc()}")
                
                # Comprehensive error handling using error handler
                recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                
                if not recovery['should_retry']:
                    error_msg = recovery.get('message', recovery.get('reason', 'Unknown error'))
                    self.logger.error(f"Unrecoverable error: {error_msg}")
                    self.health_status['consecutive_failures'] += 1
                    self.health_status['last_failure_time'] = datetime.now()
                    return None
                
                # Handle recovery actions
                
                if retry_count < max_retries - 1:
                    wait_time = recovery['wait_time']
                    delay = random.uniform(wait_time[0], wait_time[1])
                    error_msg = recovery.get('message', recovery.get('reason', 'Unknown error'))
                    self.logger.warning(f"{error_msg}, retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    retry_count += 1
                    continue
                else:
                    self.health_status['consecutive_failures'] += 1
                    self.health_status['last_failure_time'] = datetime.now()
                    return None
        
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Initialize product data structure
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
            # Extract title - try multiple ways with error handling
            try:
                title_elem = soup.find('h1', class_='product-title')
                if not title_elem:
                    title_elem = soup.find('h1')  # Try any h1
                if not title_elem:
                    # Last resort: try to find title in meta tags
                    title_elem = soup.find('meta', property='og:title')
                    if title_elem:
                        product_data['title'] = title_elem.get('content', '').strip()
                    else:
                        # Try page title
                        title_tag = soup.find('title')
                        if title_tag:
                            product_data['title'] = title_tag.get_text(strip=True)
                
                if title_elem and not product_data['title']:
                    product_data['title'] = title_elem.get_text(strip=True)
            except Exception as e:
                self.logger.warning(f"Error extracting title: {str(e)}")
                # Try to get title from page title as fallback
                try:
                    title_tag = soup.find('title')
                    if title_tag:
                        product_data['title'] = title_tag.get_text(strip=True)
                except:
                    pass
            
            # Check if title was found
            if not product_data['title'] or len(product_data['title']) < 3:
                self.logger.warning(f"⚠️ No valid title found for {url} - page may not have loaded correctly")
                # Check if we got blocked - only if page is very small AND no product elements
                if html and len(html) > 0:
                    html_lower = html.lower()
                    page_is_small = len(html) < 8000
                    # Check for product elements
                    soup_check = BeautifulSoup(html, 'lxml')
                    has_product_elements = any([
                        soup_check.find('span', class_=re.compile(r'sku|part.*number', re.I)),
                        soup_check.find('div', class_=re.compile(r'product.*price|price.*product', re.I)),
                        soup_check.find('button', class_=re.compile(r'add.*cart|buy.*now', re.I)),
                    ])
                    if 'scuderiacarparts.com' in html_lower[:500] and page_is_small and not has_product_elements:
                        self.logger.warning(f"Page appears to be blocked (minimal content: {len(html)} bytes, no product elements)")
                return None
            
            self.logger.info(f"📝 Found title: {product_data['title'][:60]}")
            
            # Extract SKU/Part Number with error handling - try multiple selectors
            try:
                sku_elem = (soup.find('span', class_=re.compile(r'sku|part.*number|part-number', re.I)) or
                           soup.find('div', class_=re.compile(r'sku|part.*number|part-number', re.I)) or
                           soup.find('span', class_='sku-display') or
                           soup.find('span', id=re.compile(r'sku|part.*number', re.I)) or
                           soup.find('div', id=re.compile(r'sku|part.*number', re.I)))
                if sku_elem:
                    product_data['sku'] = sku_elem.get_text(strip=True)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                else:
                    # Try to extract from meta tags or data attributes
                    sku_meta = soup.find('meta', property=re.compile(r'sku|part.*number', re.I))
                    if sku_meta:
                        product_data['sku'] = sku_meta.get('content', '').strip()
                    product_data['pn'] = self.clean_sku(product_data['sku'])
            except Exception as e:
                self.logger.debug(f"Error extracting SKU: {str(e)}")
            
            # Check if this is actually a wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                self.logger.info(f"🔍 Checking: '{product_data['title'][:60]}' -> {'✅ WHEEL' if is_wheel else '❌ SKIPPED'}")

                if not is_wheel:
                    self.logger.info(f"⏭️ Skipping non-wheel product: {product_data['title']}")
                    return None
            except Exception as e:
                self.logger.warning(f"Error checking if wheel product: {str(e)}")
                # If we can't determine, skip to be safe
                return None
            
            # Extract actual sale price with error handling - try multiple selectors
            try:
                sale_price_elem = (soup.find('span', class_=re.compile(r'sale.*price|price.*sale|current.*price', re.I)) or
                                 soup.find('div', class_=re.compile(r'sale.*price|price.*sale|current.*price', re.I)) or
                                 soup.find('strong', class_=re.compile(r'sale.*price|price.*sale', re.I)) or
                                 soup.find('span', class_='sale-price') or
                                 soup.find('span', class_='price') or
                                 soup.find('div', class_='price'))
                if sale_price_elem:
                    product_data['actual_price'] = self.extract_price(sale_price_elem.get_text(strip=True))
            except Exception as e:
                self.logger.debug(f"Error extracting sale price: {str(e)}")
            
            # Extract MSRP/List Price with error handling - try multiple selectors
            try:
                msrp_elem = (soup.find('span', class_=re.compile(r'list.*price|msrp|original.*price', re.I)) or
                           soup.find('div', class_=re.compile(r'list.*price|msrp|original.*price', re.I)) or
                           soup.find('span', class_='list-price') or
                           soup.find('span', class_='msrp') or
                           soup.find('del', class_=re.compile(r'price', re.I)))
                if msrp_elem:
                    product_data['msrp'] = self.extract_price(msrp_elem.get_text(strip=True))
            except Exception as e:
                self.logger.debug(f"Error extracting MSRP: {str(e)}")
            
            # Extract image URL with error handling - try multiple selectors
            try:
                img_elem = (soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I)) or
                          soup.find('img', id=re.compile(r'product.*image|main.*image', re.I)) or
                          soup.find('img', class_='product-main-image') or
                          soup.find('img', class_='product-image') or
                          soup.find('div', class_=re.compile(r'product.*image', re.I)).find('img') if soup.find('div', class_=re.compile(r'product.*image', re.I)) else None)
                if img_elem:
                    img_url = (img_elem.get('src') or 
                             img_elem.get('data-src') or 
                             img_elem.get('data-lazy-src') or
                             img_elem.get('data-original') or
                             img_elem.get('data-image'))
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            except Exception as e:
                self.logger.debug(f"Error extracting image URL: {str(e)}")
            
            # Extract basic info from main page first (before clicking tabs)
            # Title, price, SKU, image are usually visible on main page
            
            # Now click on "Specifications" tab to get detailed product information
            self.logger.info("Clicking 'Specifications' tab to extract product details...")
            try:
                # Try multiple selectors for the Specifications tab
                spec_tab_selectors = [
                    "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'specification')]",
                    "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'specification')]",
                    "//li[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'specification')]",
                    "button[data-tab='specifications']",
                    "a[data-tab='specifications']",
                    ".tab-specifications",
                    "#specifications-tab",
                ]
                
                spec_tab_clicked = False
                for selector in spec_tab_selectors:
                    try:
                        if selector.startswith("//"):
                            # XPath selector
                            tab_element = self.driver.find_element(By.XPATH, selector)
                        else:
                            # CSS selector
                            tab_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if tab_element and tab_element.is_displayed():
                            # Scroll to tab
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", tab_element)
                            time.sleep(0.5)
                            
                            # Click the tab
                            try:
                                tab_element.click()
                            except:
                                self.driver.execute_script("arguments[0].click();", tab_element)
                            
                            spec_tab_clicked = True
                            self.logger.info("✓ Clicked 'Specifications' tab")
                            time.sleep(2)  # Wait for tab content to load
                            break
                    except NoSuchElementException:
                        continue
                    except Exception as e:
                        self.logger.debug(f"Error clicking Specifications tab with selector {selector}: {str(e)}")
                        continue
                
                if spec_tab_clicked:
                    # Get updated HTML after clicking Specifications tab
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Extract description from Specifications tab
                    try:
                        desc_elem = (soup.find('div', class_=re.compile(r'description|product.*description', re.I)) or
                                   soup.find('span', class_=re.compile(r'description', re.I)) or
                                   soup.find('p', class_=re.compile(r'description', re.I)) or
                                   soup.find('div', id=re.compile(r'description', re.I)) or
                                   soup.find('section', class_=re.compile(r'description', re.I)) or
                                   soup.find('div', class_=re.compile(r'specification', re.I)))
                        if desc_elem:
                            product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
                    except Exception as e:
                        self.logger.debug(f"Error extracting description from Specifications tab: {str(e)}")
                    
                    # Extract other fields from Specifications tab
                    # Look for specification tables or lists
                    spec_tables = soup.find_all('table', class_=re.compile(r'specification', re.I))
                    spec_lists = soup.find_all('dl', class_=re.compile(r'specification', re.I))
                    spec_divs = soup.find_all('div', class_=re.compile(r'specification', re.I))
                    
                    # Extract from definition lists (dl/dt/dd structure)
                    for dl in spec_lists:
                        dts = dl.find_all('dt')
                        dds = dl.find_all('dd')
                        for i, dt in enumerate(dts):
                            label = dt.get_text(strip=True).lower()
                            value = dds[i].get_text(strip=True) if i < len(dds) else ''
                            
                            if 'also known' in label or 'other name' in label:
                                if not product_data['also_known_as']:
                                    product_data['also_known_as'] = value
                            elif 'position' in label or 'note' in label or 'footnote' in label:
                                if not product_data['positions']:
                                    product_data['positions'] = value
                            elif 'application' in label:
                                if not product_data['applications']:
                                    product_data['applications'] = value
                            elif 'replace' in label or 'supersede' in label:
                                if not product_data['replaces']:
                                    product_data['replaces'] = value
                    
                    # Extract from tables
                    for table in spec_tables:
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 2:
                                label = cells[0].get_text(strip=True).lower()
                                value = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                                
                                if 'also known' in label or 'other name' in label:
                                    if not product_data['also_known_as']:
                                        product_data['also_known_as'] = value
                                elif 'position' in label or 'note' in label:
                                    if not product_data['positions']:
                                        product_data['positions'] = value
                                elif 'application' in label:
                                    if not product_data['applications']:
                                        product_data['applications'] = value
                                elif 'replace' in label or 'supersede' in label:
                                    if not product_data['replaces']:
                                        product_data['replaces'] = value
                else:
                    self.logger.warning("Could not find or click 'Specifications' tab, extracting from main page...")
                    # Fallback: try to extract description from main page
                    try:
                        desc_elem = (soup.find('div', class_=re.compile(r'description|product.*description', re.I)) or
                                   soup.find('span', class_=re.compile(r'description', re.I)) or
                                   soup.find('p', class_=re.compile(r'description', re.I)))
                        if desc_elem:
                            product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
                    except Exception as e:
                        self.logger.debug(f"Error extracting description: {str(e)}")
            except Exception as e:
                self.logger.warning(f"Error handling Specifications tab: {str(e)}")
                # Fallback: try to extract description from current page if an error occurred before the 'else' block
                try:
                    desc_elem = (soup.find('div', class_=re.compile(r'description|product.*description', re.I)) or
                               soup.find('span', class_=re.compile(r'description', re.I)) or
                               soup.find('p', class_=re.compile(r'description', re.I)))
                    if desc_elem:
                        product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
                except:
                    pass
            
            # Additional fields (also_known_as, positions, applications, replaces) 
            # are now extracted from Specifications tab above
            # Fallback extraction if not found in Specifications tab:
            if not product_data.get('also_known_as'):
                try:
                    also_known_elem = (soup.find('li', class_=re.compile(r'also.*known|other.*name', re.I)) or
                                     soup.find('div', class_=re.compile(r'also.*known|other.*name', re.I)) or
                                     soup.find('span', class_=re.compile(r'also.*known|other.*name', re.I)))
                    if also_known_elem:
                        product_data['also_known_as'] = also_known_elem.get_text(strip=True)
                except:
                    pass
            
            if not product_data.get('positions'):
                try:
                    notes_elem = (soup.find('li', class_=re.compile(r'footnote|position|note', re.I)) or
                                soup.find('div', class_=re.compile(r'footnote|position|note', re.I)))
                    if notes_elem:
                        product_data['positions'] = notes_elem.get_text(strip=True)
                except:
                    pass
            
            # Extract fitment data from "Fitment Details" tab
            # This requires clicking the tab, then clicking each subcategory to load fitments
            self.logger.info("Clicking 'Fitment Details' tab to extract fitment information...")
            try:
                # Try multiple selectors for the Fitment Details tab
                fitment_tab_selectors = [
                    "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'fitment')]",
                    "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'fitment')]",
                    "//li[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'fitment')]",
                    "button[data-tab='fitment']",
                    "a[data-tab='fitment']",
                    ".tab-fitment",
                    "#fitment-tab",
                ]
                
                fitment_tab_clicked = False
                for selector in fitment_tab_selectors:
                    try:
                        if selector.startswith("//"):
                            tab_element = self.driver.find_element(By.XPATH, selector)
                        else:
                            tab_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if tab_element and tab_element.is_displayed():
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", tab_element)
                            time.sleep(0.5)
                            
                            try:
                                tab_element.click()
                            except:
                                self.driver.execute_script("arguments[0].click();", tab_element)
                            
                            fitment_tab_clicked = True
                            self.logger.info("✓ Clicked 'Fitment Details' tab")
                            time.sleep(2)  # Wait for tab content to load
                            break
                    except NoSuchElementException:
                        continue
                    except Exception as e:
                        self.logger.debug(f"Error clicking Fitment Details tab with selector {selector}: {str(e)}")
                        continue
                
                if fitment_tab_clicked:
                    # Now find all subcategory items on the left side and click each one
                    self.logger.info("Finding subcategory items in Fitment Details...")
                    
                    # Try multiple selectors for subcategory items (usually on the left side)
                    subcategory_selectors = [
                        "//div[contains(@class, 'fitment')]//li[contains(@class, 'category') or contains(@class, 'subcategory')]//a",
                        "//div[contains(@class, 'fitment')]//ul//li//a",
                        "//div[@id='fitment-details']//li//a",
                        ".fitment-categories li a",
                        ".fitment-list li a",
                        "[data-fitment-category]",
                    ]
                    
                    subcategory_elements = []
                    for selector in subcategory_selectors:
                        try:
                            if selector.startswith("//"):
                                elements = self.driver.find_elements(By.XPATH, selector)
                            else:
                                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            
                            if elements:
                                subcategory_elements = elements
                                self.logger.info(f"Found {len(elements)} subcategory items using selector: {selector[:50]}")
                                break
                        except Exception as e:
                            self.logger.debug(f"Error finding subcategories with selector {selector}: {str(e)}")
                            continue
                    
                    if not subcategory_elements:
                        # Fallback: try to find any clickable items in the fitment section
                        try:
                            # Look for any list items or links in the fitment area
                            fitment_container = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fitment') or contains(@id, 'fitment')]")
                            subcategory_elements = fitment_container.find_elements(By.TAG_NAME, "a")
                            if not subcategory_elements:
                                subcategory_elements = fitment_container.find_elements(By.TAG_NAME, "li")
                        except:
                            pass
                    
                    if subcategory_elements:
                        self.logger.info(f"Found {len(subcategory_elements)} subcategory items to process")
                        
                        # Click each subcategory and extract fitments
                        for idx, subcategory in enumerate(subcategory_elements):
                            try:
                                # Scroll to subcategory
                                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", subcategory)
                                time.sleep(0.3)
                                
                                # Get subcategory text for logging
                                subcategory_text = subcategory.text.strip()[:50] if subcategory.text else f"Item {idx+1}"
                                
                                # Click the subcategory
                                try:
                                    subcategory.click()
                                except:
                                    self.driver.execute_script("arguments[0].click();", subcategory)
                                
                                self.logger.debug(f"Clicked subcategory: {subcategory_text}")
                                time.sleep(1.5)  # Wait for fitments to load on the right side
                                
                                # Now extract fitments from the right side
                                html_after_click = self.driver.page_source
                                soup_after_click = BeautifulSoup(html_after_click, 'lxml')
                                
                                # Look for fitment tables or lists on the right side
                                fitment_tables = soup_after_click.find_all('table', class_=re.compile(r'fitment|vehicle|compatibility', re.I))
                                fitment_lists = soup_after_click.find_all('ul', class_=re.compile(r'fitment|vehicle|compatibility', re.I))
                                
                                # Extract from tables
                                for table in fitment_tables:
                                    rows = table.find_all('tr')
                                    for row in rows[1:]:  # Skip header row
                                        cells = row.find_all(['td', 'th'])
                                        if len(cells) >= 2:
                                            try:
                                                # Try to parse fitment data from table cells
                                                # Common formats: Year | Make | Model | Trim | Engine
                                                year = cells[0].get_text(strip=True) if len(cells) > 0 else ''
                                                make = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                                                model = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                                                trim = cells[3].get_text(strip=True) if len(cells) > 3 else ''
                                                engine = cells[4].get_text(strip=True) if len(cells) > 4 else ''
                                                
                                                # Only add if we have at least year, make, and model
                                                if year and make and model:
                                                    fitment_entry = {
                                                        'year': year,
                                                        'make': make,
                                                        'model': model,
                                                        'trim': trim,
                                                        'engine': engine
                                                    }
                                                    # Check if this fitment is already added
                                                    if fitment_entry not in product_data['fitments']:
                                                        product_data['fitments'].append(fitment_entry)
                                            except Exception as e:
                                                self.logger.debug(f"Error parsing fitment row: {str(e)}")
                                                continue
                                
                                # Extract from lists (if format is different)
                                for ul in fitment_lists:
                                    items = ul.find_all('li')
                                    for item in items:
                                        text = item.get_text(strip=True)
                                        # Try to parse fitment from text (e.g., "2020 Land Rover Range Rover")
                                        # This is a fallback if table structure is not available
                                        if text and len(text) > 5:
                                            # Try to extract year, make, model from text
                                            # This is a simple parser - may need adjustment based on actual format
                                            parts = text.split()
                                            if len(parts) >= 3:
                                                try:
                                                    year = parts[0] if parts[0].isdigit() else ''
                                                    make = parts[1] if len(parts) > 1 else ''
                                                    model = ' '.join(parts[2:]) if len(parts) > 2 else ''
                                                    
                                                    if year and make and model:
                                                        fitment_entry = {
                                                            'year': year,
                                                            'make': make,
                                                            'model': model,
                                                            'trim': '',
                                                            'engine': ''
                                                        }
                                                        if fitment_entry not in product_data['fitments']:
                                                            product_data['fitments'].append(fitment_entry)
                                                except:
                                                    pass
                                
                            except Exception as e:
                                self.logger.warning(f"Error processing subcategory {idx+1}: {str(e)}")
                                continue
                        
                        if product_data['fitments']:
                            self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from Fitment Details tab")
                    else:
                        self.logger.warning("No subcategory items found in Fitment Details tab")
                        # Fallback: try to extract fitments directly from the tab without clicking subcategories
                        html = self.driver.page_source
                        soup = BeautifulSoup(html, 'lxml')
                        fitment_tables = soup.find_all('table', class_=re.compile(r'fitment|vehicle|compatibility', re.I))
                        for table in fitment_tables:
                            rows = table.find_all('tr')
                            for row in rows[1:]:
                                cells = row.find_all(['td', 'th'])
                                if len(cells) >= 3:
                                    try:
                                        product_data['fitments'].append({
                                            'year': cells[0].get_text(strip=True) if len(cells) > 0 else '',
                                            'make': cells[1].get_text(strip=True) if len(cells) > 1 else '',
                                            'model': cells[2].get_text(strip=True) if len(cells) > 2 else '',
                                            'trim': cells[3].get_text(strip=True) if len(cells) > 3 else '',
                                            'engine': cells[4].get_text(strip=True) if len(cells) > 4 else ''
                                        })
                                    except:
                                        continue
                else:
                    self.logger.warning("Could not find or click 'Fitment Details' tab, trying to extract fitments from main page...")
                    # Fallback: try JSON or HTML extraction from main page
                    try:
                        script_elem = soup.find('script', id='product_data')
                        if not script_elem:
                            script_tags = soup.find_all('script', type='application/json')
                            for tag in script_tags:
                                if tag.string and ('fitment' in tag.string.lower() or 'vehicle' in tag.string.lower()):
                                    script_elem = tag
                                    break
                    
                        if script_elem and script_elem.string:
                            try:
                                product_json = json.loads(script_elem.string)
                                fitments = product_json.get('fitment', []) or product_json.get('vehicles', [])
                                for fitment in fitments:
                                    try:
                                        year = str(fitment.get('year', ''))
                                        make = fitment.get('make', '')
                                        model = fitment.get('model', '')
                                        trims = fitment.get('trims', []) or ['']
                                        engines = fitment.get('engines', []) or ['']
                                        
                                        for trim in trims:
                                            for engine in engines:
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
                    except:
                        pass
            except Exception as e:
                self.logger.warning(f"Error extracting fitments from Fitment Details tab: {str(e)}")
                import traceback
                self.logger.debug(f"Traceback: {traceback.format_exc()}")
            
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
            
            self.logger.info(f"✅ Successfully scraped: {product_data['title']}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"❌ Error scraping product {url}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None