"""Scraper for parts.moparonlineparts.com (Mopar parts)"""
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
from selenium.common.exceptions import TimeoutException


class MoparOnlinePartsScraper(BaseScraper):
    """Scraper for parts.moparonlineparts.com"""
    
    def __init__(self):
        super().__init__('moparonlineparts', use_selenium=True)
        self.base_url = 'https://parts.moparonlineparts.com'
        
    def get_product_urls(self):
        """
        Get all wheel product URLs from moparonlineparts.com
        Comprehensive discovery strategy:
        1. Search for wheels
        2. Browse wheel category pages
        3. Extract individual product URLs from category/listing pages
        """
        product_urls = []
        
        try:
            # Method 1: Search for wheels
            self.logger.info("Method 1: Searching for wheel products...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            self.logger.info(f"Found {len(search_urls)} URLs via search")
            
            # Method 2: Browse wheel category pages
            self.logger.info("Method 2: Browsing wheel category pages...")
            category_urls = self._browse_wheel_categories()
            product_urls.extend(category_urls)
            self.logger.info(f"Found {len(category_urls)} URLs via category browsing")
            
            # Remove duplicates
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # CRITICAL: Filter out category/listing pages - only keep individual product pages
            validated_urls = []
            filtered_out = []
            
            for url in product_urls:
                # Individual product URLs should match these patterns:
                # - /oem-parts/... (individual product)
                # - /parts/... (individual product)
                # Should NOT be:
                # - /wheels (category page)
                # - /spare-wheel (category page)
                # - /search (search results page)
                # - /v-... (vehicle selection pages)
                
                is_product_page = False
                is_category_page = False
                
                # Check if it's a category/listing page (should be filtered out)
                category_patterns = [
                    r'/wheels$',
                    r'/wheels\?',
                    r'/spare-wheel$',
                    r'/spare-wheel\?',
                    r'/search',
                    r'/v-',  # Vehicle selection pages
                    r'/v$',  # Vehicle search page
                ]
                
                for pattern in category_patterns:
                    if re.search(pattern, url, re.I):
                        is_category_page = True
                        break
                
                # Check if it's an individual product page
                product_patterns = [
                    r'/oem-parts/',
                    r'/parts/[^/]+/[^/]+',  # /parts/category/product-name
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
            
            search_url = f"{self.base_url}/search?search_str=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            # Increase page load timeout for search page (to allow Cloudflare to complete)
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
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/'], a[href*='/parts/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load lazy-loaded content
            self._scroll_to_load_content()
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Find product links
            product_links = soup.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+'))
            
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
                        f"{self.base_url}/search?search_str=wheel&page={page_num}",
                        f"{self.base_url}/search?search_str=wheel&p={page_num}",
                        f"{self.base_url}/search?q=wheel&page={page_num}",
                    ]
                    
                    page_loaded = False
                    pag_url_used = None
                    
                    for pag_url in pagination_urls:
                        try:
                            # Increase timeout for pagination pages (to allow Cloudflare to complete)
                            original_pag_timeout = self.page_load_timeout
                            try:
                                self.page_load_timeout = 60
                                self.driver.set_page_load_timeout(60)
                                
                                html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                                if html and len(html) > 5000:
                                    # Check if page has product links
                                    soup_check = BeautifulSoup(html, 'lxml')
                                    page_links = soup_check.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+'))
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
                                # Restore timeout
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
                    page_links = soup.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+'))
                    
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
    
    def _browse_wheel_categories(self):
        """Browse wheel-related category pages and extract product URLs"""
        product_urls = []
        
        try:
            # Wheel-related category URLs
            category_urls = [
                f"{self.base_url}/wheels",
                f"{self.base_url}/spare-wheel",
                f"{self.base_url}/wheel-lug-nut",
            ]
            
            for category_url in category_urls:
                try:
                    self.logger.info(f"Browsing category: {category_url}")
                    
                    # Increase page load timeout for category pages (to allow Cloudflare to complete)
                    original_timeout = self.page_load_timeout
                    try:
                        self.page_load_timeout = 60  # Increase to 60 seconds for category page
                        self.driver.set_page_load_timeout(60)
                        
                        html = self.get_page(category_url, use_selenium=True, wait_time=2)
                        if not html:
                            continue
                    except Exception as e:
                        self.logger.error(f"Error loading category page: {str(e)}")
                        continue
                    finally:
                        # Restore original timeout
                        try:
                            self.page_load_timeout = original_timeout
                            self.driver.set_page_load_timeout(original_timeout)
                        except:
                            pass
                    
                    # Wait for products to load
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/'], a[href*='/parts/']"))
                        )
                    except:
                        pass
                    
                    # Scroll to load content
                    self._scroll_to_load_content()
                    
                    # Get updated HTML
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Extract product links
                    product_links = soup.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+'))
                    
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
                                    # Increase timeout for category pagination pages
                                    original_pag_timeout = self.page_load_timeout
                                    try:
                                        self.page_load_timeout = 60
                                        self.driver.set_page_load_timeout(60)
                                        
                                        html = self.get_page(pag_url, use_selenium=True, wait_time=1)
                                        if html and len(html) > 5000:
                                            soup_check = BeautifulSoup(html, 'lxml')
                                            if soup_check.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+')):
                                                page_loaded = True
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
                            page_links = soup.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+'))
                            
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
                    self.logger.error(f"Error browsing category {category_url}: {str(e)}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error browsing wheel categories: {str(e)}")
        
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
        Scrape single product from MoparOnlineParts
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
                
                # Store original timeout at the start (before any try blocks)
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
                
                # Load the page with timeout protection (matching tascaparts_scraper.py approach)
                # Increase timeout to allow Cloudflare to complete
                try:
                    self.page_load_timeout = 60  # Increase to 60 seconds for product pages
                    self.driver.set_page_load_timeout(60)
                    
                    self.driver.get(url)
                    
                    # Wait a bit before accessing page_source (more human-like)
                    time.sleep(random.uniform(0.5, 1.5))  # Added delay to avoid immediate page_source access
                    
                    # Quick Cloudflare check - only if on challenge URL or page is very small
                    current_url_check = self.driver.current_url.lower()
                    page_preview = self.driver.page_source[:6000]
                    
                    # undetected_chromedriver handles Cloudflare automatically
                    # Wait a bit longer to let it complete the challenge
                    time.sleep(random.uniform(2, 4))  # Initial wait for page to start loading
                    
                    # Check if we're on a Cloudflare challenge page
                    current_url_after_wait = self.driver.current_url.lower()
                    page_preview_after_wait = self.driver.page_source[:10000] if len(self.driver.page_source) > 10000 else self.driver.page_source
                    
                    # Only check for Cloudflare if we're definitely on a challenge page
                    is_challenge_page = (
                        'challenges.cloudflare.com' in current_url_after_wait or 
                        '/cdn-cgi/challenge' in current_url_after_wait or
                        (len(page_preview_after_wait) < 5000 and self.has_cloudflare_challenge())
                    )
                    
                    if is_challenge_page:
                        self.logger.info("🛡️ Cloudflare challenge detected - waiting for undetected_chromedriver to handle it...")
                        
                        # undetected_chromedriver should handle this automatically, but we need to wait
                        # Give it plenty of time (up to 60 seconds total)
                        max_wait_time = 60
                        waited = 0
                        check_interval = 3
                        
                        while waited < max_wait_time:
                            time.sleep(check_interval)
                            waited += check_interval
                            
                            # Check current state
                            current_url_check = self.driver.current_url.lower()
                            page_source_check = self.driver.page_source
                            page_preview_check = page_source_check[:10000].lower() if len(page_source_check) > 10000 else page_source_check.lower()
                            
                            # Check if challenge is gone
                            still_on_challenge = (
                                'challenges.cloudflare.com' in current_url_check or
                                '/cdn-cgi/challenge' in current_url_check or
                                "verifying you are human" in page_preview_check or
                                "review the security of your connection" in page_preview_check or
                                "verifying..." in page_preview_check or
                                (len(page_source_check) < 5000 and self.has_cloudflare_challenge())
                            )
                            
                            if not still_on_challenge and len(page_source_check) > 8000:
                                # Challenge passed!
                                self.logger.info(f"✅ Cloudflare bypassed by undetected_chromedriver! (waited {waited}s)")
                                time.sleep(random.uniform(1, 2))  # Brief stabilization
                                break
                            
                            if waited % 10 == 0:  # Log every 10 seconds
                                self.logger.info(f"⏳ Still waiting for Cloudflare bypass... ({waited}s/{max_wait_time}s)")
                        
                        # Final check - if still on challenge, use manual bypass
                        final_url = self.driver.current_url.lower()
                        final_page = self.driver.page_source
                        still_challenged = (
                            'challenges.cloudflare.com' in final_url or
                            '/cdn-cgi/challenge' in final_url or
                            (len(final_page) < 5000 and self.has_cloudflare_challenge())
                        )
                        
                        if still_challenged:
                            self.logger.warning("⚠️ Cloudflare still present after wait - using manual bypass...")
                            cloudflare_bypassed = self.wait_for_cloudflare(timeout=60, target_url=url, max_retries=2)
                            if not cloudflare_bypassed:
                                retry_count += 1
                                if retry_count < max_retries:
                                    wait_time = random.uniform(10, 15)
                                    self.logger.warning(f"Retrying page load in {wait_time:.1f}s...")
                                    time.sleep(wait_time)
                                    continue
                                else:
                                    return None
                        else:
                            self.logger.info("✓ Cloudflare bypassed successfully!")
                    else:
                        # Not on challenge page, continue normally
                        pass
                    
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
                    
                    # Check if this is actually a product page (not a category/listing page)
                    # Category pages typically have multiple product listings, not a single product
                    product_indicators = [
                        soup.find('h1', class_=re.compile(r'product.*title|product.*name', re.I)),
                        soup.find('div', class_=re.compile(r'product.*detail|product.*info', re.I)),
                        soup.find('span', class_=re.compile(r'part.*number|sku', re.I)),
                    ]
                    
                    # Check if it's a category/listing page (has multiple product cards)
                    category_indicators = [
                        soup.find('div', class_=re.compile(r'catalog.*product|catalog.*listing', re.I)),
                        soup.find_all('div', class_=re.compile(r'product.*card|product.*item', re.I)),
                    ]
                    
                    has_product_elements = any(product_indicators)
                    has_category_elements = len(category_indicators[1]) > 1 if category_indicators[1] else False
                    
                    # If it looks like a category page, skip it
                    if has_category_elements and not has_product_elements:
                        self.logger.warning(f"⚠️ URL appears to be a category/listing page, not a product page: {url}")
                        return None
                    
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
                    if 'moparonlineparts.com' not in current_url and not current_url.startswith(('chrome-error://', 'about:')):
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
                    # Restore timeout
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                    break
                    
                except TimeoutException as e:
                    self.logger.warning(f"⚠️ Page load timeout - waiting longer...")
                    # Timeout already increased above, keep it at 60
                    
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
                            # Restore timeout before returning
                            self.page_load_timeout = original_timeout
                            self.driver.set_page_load_timeout(original_timeout)
                            return None
                    finally:
                        # Always restore timeout
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                        
                except Exception as e:
                    error_str = str(e).lower()
                    if any(err in error_str for err in ['connection', 'network', 'dns', 'err_', 'timeout']):
                        self.logger.warning(f"⚠️ Connection/network error: {e}")
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = random.uniform(5, 8)
                            time.sleep(wait_time)
                            continue
                        else:
                            return None
                    else:
                        # Restore timeout before re-raising
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                        raise
                        
            except Exception as e:
                # Restore timeout in case of any exception
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
                        product_data['title'] = title_tag.get_text(strip=True)
            
            if title_elem and not product_data['title']:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                self.logger.warning(f"⚠️ No valid title found for {url}")
                return None
            
            self.logger.info(f"📝 Found title: {product_data['title'][:60]}")
            
            # Extract SKU/Part Number - Method 1: From span with class "sku-display"
            sku_elem = soup.find('span', class_='sku-display')
            if sku_elem:
                product_data['sku'] = sku_elem.get_text(strip=True)
                product_data['pn'] = self.clean_sku(product_data['sku'])
                self.logger.info(f"📦 Found SKU from sku-display: {product_data['sku']}")
            
            # Method 2: From h2 with class "list-value sku-display"
            if not product_data['sku']:
                sku_elem = soup.find('h2', class_=re.compile(r'list-value.*sku|sku.*list-value', re.I))
                if sku_elem:
                    product_data['sku'] = sku_elem.get_text(strip=True)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                    self.logger.info(f"📦 Found SKU from h2: {product_data['sku']}")
            
            # Method 3: From product_data JSON script tag
            if not product_data['sku']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        if 'sku' in product_json:
                            product_data['sku'] = str(product_json['sku']).strip()
                            product_data['pn'] = self.clean_sku(product_data['sku'])
                            self.logger.info(f"📦 Found SKU from product_data JSON: {product_data['sku']}")
                    except json.JSONDecodeError as e:
                        self.logger.debug(f"Error parsing product_data JSON: {str(e)}")
            
            # Method 4: Search for "Part Number:" or "SKU:" text
            if not product_data['sku']:
                page_text = soup.get_text()
                part_num_match = re.search(r'(?:Part\s*Number|SKU)[:\s]+([A-Z0-9]{5,})', page_text, re.I)
                if part_num_match:
                    product_data['sku'] = part_num_match.group(1).strip()
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                    self.logger.info(f"📦 Found SKU from page text: {product_data['sku']}")
            
            # Method 5: LAST RESORT - Extract from URL (e.g., /oem-parts/mopar-wheel-82213447)
            if not product_data['sku']:
                url_match = re.search(r'/(?:oem-parts|parts)/[^/]+-(\d+)', url)
                if url_match:
                    product_data['sku'] = url_match.group(1).strip()
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                    self.logger.info(f"📦 Found SKU from URL (last resort): {product_data['sku']}")
            
            # Method 6: LAST RESORT - Extract from title if part number still not found
            if not product_data['sku'] and product_data['title']:
                # Titles like "Wheel - Mopar (82213447)" contain part number
                title_match = re.search(r'\((\d{5,})\)', product_data['title'])
                if title_match:
                    product_data['sku'] = title_match.group(1)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                    self.logger.info(f"📦 Found SKU from title (last resort): {product_data['sku']}")
            
            # Check if this is a wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                self.logger.info(f"🔍 Checking: '{product_data['title'][:60]}' -> {'✅ WHEEL' if is_wheel else '❌ SKIPPED'}")
                
                if not is_wheel:
                    self.logger.info(f"⏭️ Skipping non-wheel product: {product_data['title']}")
                    return None
            except Exception as e:
                self.logger.warning(f"Error checking if wheel product: {str(e)}")
                return None
            
            # Extract price - Method 1: From sale-price-value
            price_elem = soup.find('strong', id='product_price', class_='sale-price-value')
            if not price_elem:
                price_elem = soup.find('strong', class_=re.compile(r'sale-price-value|sale-price-amount', re.I))
            if not price_elem:
                price_elem = soup.find('span', class_=re.compile(r'sale-price-value', re.I))
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                product_data['actual_price'] = self.extract_price(price_text)
                if product_data['actual_price']:
                    self.logger.info(f"💰 Found price: {product_data['actual_price']}")
            
            # Method 2: From product_data JSON
            if not product_data['actual_price']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        if 'price' in product_json:
                            product_data['actual_price'] = str(product_json['price']).strip()
                            self.logger.info(f"💰 Found price from JSON: {product_data['actual_price']}")
                    except:
                        pass
            
            # Extract MSRP - Method 1: From list-price-value
            msrp_elem = soup.find('span', class_=re.compile(r'list-price-value', re.I))
            if msrp_elem:
                msrp_text = msrp_elem.get_text(strip=True)
                product_data['msrp'] = self.extract_price(msrp_text)
                if product_data['msrp']:
                    self.logger.info(f"💰 Found MSRP: {product_data['msrp']}")
            
            # Method 2: From product_data JSON
            if not product_data['msrp']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        if 'msrp' in product_json:
                            product_data['msrp'] = str(product_json['msrp']).strip()
                            self.logger.info(f"💰 Found MSRP from JSON: {product_data['msrp']}")
                    except:
                        pass
            
            # Extract image URL - Method 1: From product-main-image
            img_elem = soup.find('img', class_='product-main-image')
            if not img_elem:
                img_elem = soup.find('img', itemprop='image')
            if img_elem:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy-src')
                if img_url:
                    product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
                    if not product_data['image_url'].startswith('http'):
                        product_data['image_url'] = f"https:{product_data['image_url']}"
            
            # Method 2: From product_data JSON (first image)
            if not product_data['image_url']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        if 'images' in product_json and len(product_json['images']) > 0:
                            first_image = product_json['images'][0]
                            if 'main' in first_image and 'url' in first_image['main']:
                                img_url = first_image['main']['url']
                                product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
                    except:
                        pass
            
            # Extract description - Method 1: From description_body
            desc_elem = soup.find('span', class_='description_body')
            if not desc_elem:
                desc_elem = soup.find('li', class_='description')
            if desc_elem:
                # Get text and clean up HTML tags
                desc_text = desc_elem.get_text(strip=True, separator=' ')
                # Remove extra whitespace
                desc_text = re.sub(r'\s+', ' ', desc_text)
                product_data['description'] = desc_text.strip()
            
            # Method 2: From product_data JSON
            if not product_data['description']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        if 'description' in product_json:
                            desc_text = product_json['description']
                            # Remove HTML tags if present
                            desc_text = re.sub(r'<[^>]+>', ' ', desc_text)
                            desc_text = re.sub(r'\s+', ' ', desc_text)
                            product_data['description'] = desc_text.strip()
                    except:
                        pass
            
            # Extract also_known_as (Other Names)
            also_known_elem = soup.find('li', class_='also_known_as')
            if also_known_elem:
                value_elem = also_known_elem.find('h2', class_='list-value')
                if not value_elem:
                    value_elem = also_known_elem.find('span', class_='list-value')
                if value_elem:
                    product_data['also_known_as'] = value_elem.get_text(strip=True)
            
            # Method 2: From product_data JSON
            if not product_data['also_known_as']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        if 'also_known_as' in product_json:
                            product_data['also_known_as'] = str(product_json['also_known_as']).strip()
                    except:
                        pass
            
            # Extract replaces (Replaces/Supersedes) - Method 1: From li element
            replaces_elem = soup.find('li', class_=re.compile(r'replaces|supersedes', re.I))
            if not replaces_elem:
                # Try finding by text content
                all_lis = soup.find_all('li')
                for li in all_lis:
                    li_text = li.get_text(strip=True).lower()
                    if 'replace' in li_text or 'supersede' in li_text:
                        # Check if it has a label/value structure
                        label_elem = li.find('h2', class_='list-label') or li.find('span', class_='list-label')
                        if label_elem:
                            label_text = label_elem.get_text(strip=True).lower()
                            if 'replace' in label_text or 'supersede' in label_text:
                                replaces_elem = li
                                break
            
            if replaces_elem:
                value_elem = replaces_elem.find('h2', class_='list-value')
                if not value_elem:
                    value_elem = replaces_elem.find('span', class_='list-value')
                if not value_elem:
                    # Try to get text directly, excluding label
                    label_elem = replaces_elem.find('h2', class_='list-label') or replaces_elem.find('span', class_='list-label')
                    if label_elem:
                        # Get all text and remove label
                        all_text = replaces_elem.get_text(strip=True)
                        label_text = label_elem.get_text(strip=True)
                        if label_text in all_text:
                            value_elem_text = all_text.replace(label_text, '', 1).strip()
                            if value_elem_text:
                                product_data['replaces'] = value_elem_text
                if value_elem and not product_data['replaces']:
                    product_data['replaces'] = value_elem.get_text(strip=True)
            
            # Method 2: From definition lists (dl/dt/dd structure)
            if not product_data['replaces']:
                spec_lists = soup.find_all('dl', class_=re.compile(r'specification|product.*spec', re.I))
                for dl in spec_lists:
                    dts = dl.find_all('dt')
                    dds = dl.find_all('dd')
                    for i, dt in enumerate(dts):
                        label = dt.get_text(strip=True).lower()
                        value = dds[i].get_text(strip=True) if i < len(dds) else ''
                        
                        if ('replace' in label or 'supersede' in label) and value:
                            product_data['replaces'] = value
                            break
                    if product_data['replaces']:
                        break
            
            # Method 3: From product_data JSON
            if not product_data['replaces']:
                product_data_script = soup.find('script', id='product_data', type='application/json')
                if product_data_script and product_data_script.string:
                    try:
                        product_json = json.loads(product_data_script.string)
                        # Try various possible keys
                        if 'replaces' in product_json:
                            product_data['replaces'] = str(product_json['replaces']).strip()
                        elif 'supersedes' in product_json:
                            product_data['replaces'] = str(product_json['supersedes']).strip()
                        elif 'replaced_by' in product_json:
                            product_data['replaces'] = str(product_json['replaced_by']).strip()
                    except:
                        pass
            
            # Method 4: Search in page text for "Replaces:" or "Supersedes:"
            if not product_data['replaces']:
                page_text = soup.get_text()
                # Look for patterns like "Replaces: ABC123" or "Supersedes: XYZ789"
                replaces_patterns = [
                    r'(?:replaces|supersedes)[:\s]+([A-Z0-9\s,]+)',
                    r'(?:replaces|supersedes)[:\s]+([A-Z0-9\-]+(?:\s*,\s*[A-Z0-9\-]+)*)',
                ]
                for pattern in replaces_patterns:
                    match = re.search(pattern, page_text, re.I)
                    if match:
                        product_data['replaces'] = match.group(1).strip()
                        break
            
            if product_data['replaces']:
                self.logger.info(f"🔄 Found replaces: {product_data['replaces']}")
            
            # Extract fitment data from product_data JSON script tag
            product_data_script = soup.find('script', id='product_data', type='application/json')
            if product_data_script and product_data_script.string:
                try:
                    product_json = json.loads(product_data_script.string)
                    fitments = product_json.get('fitment', [])
                    
                    if fitments:
                        self.logger.info(f"🔍 Found {len(fitments)} fitment entries in JSON")
                        
                        for fitment_entry in fitments:
                            try:
                                year = str(fitment_entry.get('year', '')).strip()
                                make = str(fitment_entry.get('make', '')).strip()
                                model = str(fitment_entry.get('model', '')).strip()
                                
                                # Get trims and engines (can be arrays or strings)
                                trims = fitment_entry.get('trims', [])
                                engines = fitment_entry.get('engines', [])
                                
                                # Convert to lists if they're strings
                                if isinstance(trims, str):
                                    # Split by comma if it's a comma-separated string
                                    trims = [t.strip() for t in trims.split(',') if t.strip()]
                                elif not isinstance(trims, list):
                                    trims = []
                                
                                if isinstance(engines, str):
                                    engines = [e.strip() for e in engines.split(',') if e.strip()]
                                elif not isinstance(engines, list):
                                    engines = []
                                
                                # If no trims, use empty string
                                if not trims:
                                    trims = ['']
                                
                                # If no engines, use empty string
                                if not engines:
                                    engines = ['']
                                
                                # Create all combinations of trim × engine
                                for trim in trims:
                                    for engine in engines:
                                        product_data['fitments'].append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': str(trim).strip() if trim else '',
                                            'engine': str(engine).strip() if engine else ''
                                        })
                                
                                self.logger.debug(f"  Created {len(trims) * len(engines)} fitment combinations for {year} {make} {model}")
                                
                            except Exception as fitment_error:
                                self.logger.warning(f"Error processing fitment entry: {str(fitment_error)}")
                                continue
                        
                        self.logger.info(f"✅ Extracted {len(product_data['fitments'])} total fitment combinations")
                        
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Error parsing product_data JSON: {str(e)}")
                except Exception as e:
                    self.logger.warning(f"Error extracting fitments from JSON: {str(e)}")
            
            # Fallback: Try to extract from fitment table if JSON not available
            if not product_data['fitments']:
                fitment_table = soup.find('table', class_='fitment-table')
                if fitment_table:
                    tbody = fitment_table.find('tbody', class_='fitment-table-body')
                    if tbody:
                        rows = tbody.find_all('tr', class_='fitment-row')
                        for row in rows:
                            try:
                                year_cell = row.find('td', class_='fitment-year')
                                make_cell = row.find('td', class_='fitment-make')
                                model_cell = row.find('td', class_='fitment-model')
                                trim_cell = row.find('td', class_='fitment-trim')
                                engine_cell = row.find('td', class_='fitment-engine')
                                
                                year = year_cell.get_text(strip=True) if year_cell else ''
                                make = make_cell.get_text(strip=True) if make_cell else ''
                                model = model_cell.get_text(strip=True) if model_cell else ''
                                
                                # Parse trims (comma-separated)
                                trim_text = trim_cell.get_text(strip=True) if trim_cell else ''
                                trims = [t.strip() for t in trim_text.split(',') if t.strip()] if trim_text else ['']
                                
                                # Parse engines (comma-separated)
                                engine_text = engine_cell.get_text(strip=True) if engine_cell else ''
                                engines = [e.strip() for e in engine_text.split(',') if e.strip()] if engine_text else ['']
                                
                                # Create all combinations
                                for trim in trims:
                                    for engine in engines:
                                        product_data['fitments'].append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': trim,
                                            'engine': engine
                                        })
                                
                            except Exception as row_error:
                                self.logger.debug(f"Error processing fitment table row: {str(row_error)}")
                                continue
                        
                        if product_data['fitments']:
                            self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from table")
            
            # If no fitments found, add empty fitment
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
