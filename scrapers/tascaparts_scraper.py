"""Scraper for tascaparts.com (GM parts)"""
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
from selenium.common.exceptions import TimeoutException  # ← ADD THIS LINE!

class TascaPartsScraper(BaseScraper):
    """Scraper for tascaparts.com"""
    
    def __init__(self):
        super().__init__('tascaparts', use_selenium=True)
        self.base_url = 'https://www.tascaparts.com'
        
    def get_product_urls(self):
        """
        Get all wheel product URLs from tascaparts.com
        Use search functionality as PRIMARY method (not fallback)
        """
        product_urls = []
        
        try:
            # PRIMARY METHOD: Use search (returns 64,795+ results)
            self.logger.info("Using search as PRIMARY method to find wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            product_urls = list(set(product_urls))  # Remove duplicates
            
            self.logger.info(f"Found {len(product_urls)} wheel products via search")
            
            # FALLBACK: If search didn't work, try category page
            if len(product_urls) < 10:
                self.logger.warning("Search returned few results, trying category page as fallback...")
                category_url = f"{self.base_url}/c/wheelstiresparts"
                self.logger.info(f"Fetching category page: {category_url}")
                
                html = self.get_page(category_url, use_selenium=True, wait_time=1)
                if html:
                    soup = BeautifulSoup(html, 'lxml')
                    product_links = soup.find_all('a', href=re.compile(r'/oem-parts/'))
                    
                    for link in product_links:
                        href = link.get('href', '')
                        if href and href not in product_urls:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            link_text = link.get_text(strip=True).lower()
                            if 'wheel' in link_text or 'rim' in link_text:
                                product_urls.append(full_url)
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """
        Search for wheels using site search - handles pagination and dynamic content
        Extracts ALL product URLs from search results (not just those with 'wheel' in URL)
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            search_url = f"{self.base_url}/search?search_str=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            # Increase page load timeout for search page (it's large with 64K+ results)
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60  # Increase to 60 seconds for search page
                self.driver.set_page_load_timeout(60)
                
                # Use get_page() instead of direct driver.get() for better error handling
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
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            
            # Wait for product links to appear (with shorter timeout)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load more products (if lazy loading) - with timeout protection
            self.logger.info("Scrolling to load all products on current page...")
            try:
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                scroll_attempts = 0
                max_scrolls = 50  # Increased limit for pages with many products
                no_change_count = 0
                
                while scroll_attempts < max_scrolls:
                    try:
                        # Scroll down
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1.5)  # Wait for content to load
                        
                        # Check if new content loaded (with timeout protection)
                        try:
                            new_height = self.driver.execute_script("return document.body.scrollHeight")
                        except Exception as scroll_error:
                            self.logger.warning(f"Error checking scroll height: {str(scroll_error)}")
                            break
                        
                        if new_height == last_height:
                            no_change_count += 1
                            if no_change_count >= 3:  # No change for 3 consecutive scrolls
                                break  # No more content to load
                        else:
                            no_change_count = 0  # Reset counter if content changed
                        last_height = new_height
                        scroll_attempts += 1
                    except Exception as scroll_error:
                        self.logger.warning(f"Error during scrolling: {str(scroll_error)}")
                        break
            except Exception as scroll_error:
                self.logger.warning(f"Error initializing scroll: {str(scroll_error)}")
            
            # Get page source after scrolling - with timeout protection
            try:
                html = self.driver.page_source
            except Exception as page_source_error:
                self.logger.error(f"Error accessing page_source: {str(page_source_error)}")
                # Try to get HTML via get_page() as fallback
                html = self.get_page(search_url, use_selenium=True, wait_time=1)
                if not html:
                    self.logger.error("Could not retrieve page source")
                    return product_urls
            
            soup = BeautifulSoup(html, 'lxml')
            
            # Find ALL product links (not just those with 'wheel' in URL)
            # The is_wheel_product() method will filter later
            product_links = soup.find_all('a', href=re.compile(r'/oem-parts/'))
            
            for link in product_links:
                href = link.get('href', '')
                if href:
                    # Normalize URL: extract base product URL (remove query params that change per page)
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    
                    # Remove fragment (#)
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    
                    # IMPORTANT: Extract only the base product URL, remove page-specific query params
                    # This ensures we get unique products across pages
                    if '/oem-parts/' in full_url:
                        # Extract just the product path, remove all query params
                        if '?' in full_url:
                            full_url = full_url.split('?')[0]
                    
                    # Normalize trailing slashes
                    full_url = full_url.rstrip('/')
                    
                    if full_url not in product_urls:
                        product_urls.append(full_url)
            
            self.logger.info(f"Found {len(product_links)} product links on page 1, {len(product_urls)} unique URLs")
            
            # Handle pagination - iterate through all pages
            # Strategy: Use direct URL construction since we know the search URL pattern
            page_num = 2
            max_pages = 2000  # Safety limit (64,795 results / ~50 per page = ~1,300 pages)
            consecutive_empty_pages = 0
            max_consecutive_empty = 4  # Stop after 4 consecutive pages with no new products
            
            while page_num <= max_pages:
                try:
                    self.logger.info(f"Loading page {page_num}...")
                    
                    # Try multiple pagination URL patterns
                    pagination_urls = [
                        f"{self.base_url}/search?search_str=wheel&page={page_num}",
                        f"{self.base_url}/search?search_str=wheel&p={page_num}",
                        f"{self.base_url}/search?search_str=wheel&pageNumber={page_num}",
                        f"{self.base_url}/search?q=wheel&page={page_num}",
                        f"{self.base_url}/search/wheel?page={page_num}",
                    ]
                    
                    page_loaded = False
                    pag_url_used = None
                    
                    for pag_url in pagination_urls:
                        try:
                            self.logger.debug(f"Trying pagination URL: {pag_url}")
                            
                            # Increase timeout for pagination pages
                            original_pag_timeout = self.page_load_timeout
                            try:
                                self.page_load_timeout = 60
                                self.driver.set_page_load_timeout(60)
                                
                                # Load the page directly
                                self.driver.get(pag_url)
                                time.sleep(2)  # Wait for page to load
                                
                                # Wait for product links to appear
                                try:
                                    WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/']"))
                                    )
                                except:
                                    self.logger.debug(f"Product links not found immediately on {pag_url}, continuing...")
                                
                                # Check if page loaded successfully (has product links)
                                page_links_check = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/oem-parts/']")
                                if len(page_links_check) > 0:
                                    # Page loaded successfully
                                    page_loaded = True
                                    pag_url_used = pag_url
                                    self.logger.info(f"✓ Successfully loaded page {page_num} using URL: {pag_url}")
                                    break
                            except Exception as pag_error:
                                error_str = str(pag_error).lower()
                                if 'timeout' in error_str:
                                    self.logger.debug(f"Timeout loading {pag_url}, trying next pattern...")
                                continue
                            finally:
                                # Restore timeout
                                try:
                                    self.page_load_timeout = original_pag_timeout
                                    self.driver.set_page_load_timeout(original_pag_timeout)
                                except:
                                    pass
                        except Exception as pag_error:
                            continue
                    
                    if not page_loaded:
                        self.logger.warning(f"Could not load page {page_num} with any URL pattern")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages failed to load")
                            break
                        page_num += 1
                        continue
                    
                    # Don't reset consecutive_empty_pages here - only reset when we find new products
                    # This ensures we properly track consecutive pages with no new products
                    
                    # Scroll to load all products on this page
                    try:
                        last_height = self.driver.execute_script("return document.body.scrollHeight")
                        scroll_attempts = 0
                        no_change_count = 0
                        
                        while scroll_attempts < 30:  # Limit per page
                            try:
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(1)
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
                                self.logger.warning(f"Error during pagination scroll: {str(scroll_error)}")
                                break
                    except Exception as scroll_init_error:
                        self.logger.warning(f"Error initializing pagination scroll: {str(scroll_init_error)}")
                    
                    # Extract product links from this page - with timeout protection
                    try:
                        html = self.driver.page_source
                    except Exception as page_source_error:
                        self.logger.warning(f"Error accessing page_source on page {page_num}: {str(page_source_error)}")
                        # Try to get HTML via get_page() as fallback
                        html = self.get_page(pag_url_used, use_selenium=True, wait_time=1)
                        if not html:
                            self.logger.warning(f"Could not retrieve page source for page {page_num}, skipping")
                            page_num += 1
                            continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    page_links = soup.find_all('a', href=re.compile(r'/oem-parts/'))
                    
                    page_urls_count = 0
                    for link in page_links:
                        href = link.get('href', '')
                        if href:
                            # Normalize URL: extract base product URL (remove query params that change per page)
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove fragment (#)
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            
                            # IMPORTANT: Extract only the base product URL, remove page-specific query params
                            # This ensures we get unique products across pages
                            if '/oem-parts/' in full_url:
                                # Extract just the product path, remove all query params
                                if '?' in full_url:
                                    full_url = full_url.split('?')[0]
                            
                            # Normalize trailing slashes
                            full_url = full_url.rstrip('/')
                            
                            if full_url not in product_urls:
                                product_urls.append(full_url)
                                page_urls_count += 1
                    
                    self.logger.info(f"Page {page_num}: Found {len(page_links)} product links, {page_urls_count} new unique URLs (Total: {len(product_urls)})")
                    
                    # If no new products found, increment empty counter
                    if page_urls_count == 0:
                        consecutive_empty_pages += 1
                        self.logger.warning(f"No new products on page {page_num} (consecutive empty: {consecutive_empty_pages})")
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages with no new products")
                            break
                    else:
                        consecutive_empty_pages = 0  # Reset counter if we found new products
                    
                    page_num += 1
                    
                    # Add delay between pages to avoid being blocked
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        self.logger.warning(f"Stopping pagination due to errors")
                        break
                    page_num += 1
                    continue
            
            self.logger.info(f"Pagination complete. Total unique product URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    def scrape_product(self, url):
        """
        Scrape single product from TascaParts with retry logic
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
                    if 'tascaparts.com' not in current_url and not current_url.startswith(('chrome-error://', 'about:')):
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
                    
                    # Additional check: if URL contains error codes but also has '/oem-parts/', it's likely a product page, not an error
                    if has_critical_error and '/oem-parts/' in current_url:
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
                    if 'tascaparts.com' in html_lower[:500] and page_is_small and not has_product_elements:
                        self.logger.warning(f"Page appears to be blocked (minimal content: {len(html)} bytes, no product elements)")
                return None
            
            self.logger.info(f"📝 Found title: {product_data['title'][:60]}")
            
            # Extract SKU with error handling
            try:
                sku_elem = soup.find('span', class_='sku-display')
                if sku_elem:
                    product_data['sku'] = sku_elem.get_text(strip=True)
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
            
            # Extract actual sale price with error handling
            try:
                sale_price_elem = soup.find('strong', class_='sale-price-value')
                if not sale_price_elem:
                    # Try alternative selectors
                    sale_price_elem = soup.find('span', class_='sale-price') or soup.find('div', class_='sale-price')
                if sale_price_elem:
                    product_data['actual_price'] = self.extract_price(sale_price_elem.get_text(strip=True))
            except Exception as e:
                self.logger.debug(f"Error extracting sale price: {str(e)}")
            
            # Extract MSRP with error handling
            try:
                msrp_elem = soup.find('span', class_='list-price-value')
                if not msrp_elem:
                    # Try alternative selectors
                    msrp_elem = soup.find('span', class_='list-price') or soup.find('div', class_='msrp')
                if msrp_elem:
                    product_data['msrp'] = self.extract_price(msrp_elem.get_text(strip=True))
            except Exception as e:
                self.logger.debug(f"Error extracting MSRP: {str(e)}")
            
            # Extract image URL with error handling
            try:
                img_elem = soup.find('img', class_='product-main-image')
                if not img_elem:
                    # Try alternative selectors
                    img_elem = soup.find('img', class_='product-image') or soup.find('img', id='product-image')
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src')
                    if img_url:
                        product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            except Exception as e:
                self.logger.debug(f"Error extracting image URL: {str(e)}")
            
            # Extract description with error handling
            try:
                desc_elem = soup.find('span', class_='description_body')
                if not desc_elem:
                    # Try alternative selectors
                    desc_elem = soup.find('div', class_='description') or soup.find('p', class_='product-description')
                if desc_elem:
                    product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
            except Exception as e:
                self.logger.debug(f"Error extracting description: {str(e)}")
            
            # Extract also_known_as (Other Names) with error handling
            try:
                also_known_elem = soup.find('li', class_='also_known_as')
                if also_known_elem:
                    value_elem = also_known_elem.find('h2', class_='list-value')
                    if value_elem:
                        product_data['also_known_as'] = value_elem.get_text(strip=True)
            except Exception as e:
                self.logger.debug(f"Error extracting also_known_as: {str(e)}")
            
            # Extract footnotes/notes (positions) with error handling
            try:
                notes_elem = soup.find('li', class_='footnotes')
                if notes_elem:
                    value_elem = notes_elem.find('span', class_='list-value')
                    if value_elem:
                        product_data['positions'] = value_elem.get_text(strip=True)
            except Exception as e:
                self.logger.debug(f"Error extracting positions: {str(e)}")
            
            # Extract fitment data from JSON with comprehensive error handling
            try:
                script_elem = soup.find('script', id='product_data')
                if not script_elem:
                    # Try alternative script tags
                    script_tags = soup.find_all('script', type='application/json')
                    for tag in script_tags:
                        if 'fitment' in tag.string.lower() if tag.string else '':
                            script_elem = tag
                            break
                
                if script_elem and script_elem.string:
                    try:
                        product_json = json.loads(script_elem.string)
                        fitments = product_json.get('fitment', [])
                        
                        for fitment in fitments:
                            try:
                                year = str(fitment.get('year', ''))
                                make = fitment.get('make', '')
                                model = fitment.get('model', '')
                                trims = fitment.get('trims', [])
                                engines = fitment.get('engines', [])
                                
                                # Create a row for each trim/engine combination
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
                                            'trim': trim,
                                            'engine': engine
                                        })
                            except Exception as fitment_error:
                                self.logger.debug(f"Error processing fitment: {str(fitment_error)}")
                                continue
                        
                        self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations")
                        
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Error parsing JSON: {str(e)}")
                    except Exception as e:
                        self.logger.warning(f"Error extracting fitments: {str(e)}")
            except Exception as e:
                self.logger.debug(f"Error finding fitment script: {str(e)}")
            
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