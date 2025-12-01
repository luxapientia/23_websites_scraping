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
            
            # Load the search page with retry logic for timeout handling
            original_timeout = self.page_load_timeout
            max_retries = 3
            retry_count = 0
            page_loaded = False
            
            while retry_count < max_retries and not page_loaded:
                try:
                    self.page_load_timeout = 90  # Increased to 90 seconds
                    self.driver.set_page_load_timeout(90)
                    
                    self.logger.info(f"Attempting to load search page (attempt {retry_count + 1}/{max_retries})...")
                    self.driver.get(search_url)
                    time.sleep(3)  # Wait for initial page load
                    page_loaded = True
                    self.logger.info("✓ Search page loaded successfully")
                    
                except Exception as e:
                    error_str = str(e).lower()
                    retry_count += 1
                    
                    # Check if it's a timeout error
                    if 'timeout' in error_str or 'timed out' in error_str:
                        self.logger.warning(f"⚠️ Page load timeout on attempt {retry_count}/{max_retries}: {str(e)[:100]}")
                        
                        # Try to get page source anyway - sometimes page loads partially
                        try:
                            page_source = self.driver.page_source
                            if page_source and len(page_source) > 5000:
                                self.logger.info("✓ Page partially loaded, continuing with available content...")
                                page_loaded = True
                                break
                        except:
                            pass
                        
                        if retry_count < max_retries:
                            wait_time = 5 + (retry_count * 2)  # Progressive wait: 5s, 7s, 9s
                            self.logger.info(f"Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                        else:
                            self.logger.error("❌ Failed to load search page after all retries")
                            # Try one last time to get page source even if timeout
                            try:
                                page_source = self.driver.page_source
                                if page_source and len(page_source) > 1000:
                                    self.logger.warning("⚠️ Using partial page content despite timeout...")
                                    page_loaded = True
                            except:
                                pass
                    else:
                        # Other errors - log and retry
                        self.logger.warning(f"⚠️ Error loading search page on attempt {retry_count}/{max_retries}: {str(e)[:100]}")
                        if retry_count < max_retries:
                            wait_time = 3 + retry_count
                            self.logger.info(f"Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                        else:
                            self.logger.error("❌ Failed to load search page after all retries")
                finally:
                    # Restore original timeout
                    try:
                        self.page_load_timeout = original_timeout
                        self.driver.set_page_load_timeout(original_timeout)
                    except:
                        pass
            
            if not page_loaded:
                self.logger.error("❌ Could not load search page, returning empty product list")
                return product_urls
            
            # Wait for search results to load - wait for product containers (NOT URL-based selectors)
            self.logger.info("Waiting for search results to load...")
            try:
                # Wait for product containers to appear (NOT URL-based selectors like a[href*='/product/'])
                product_container_selectors = [
                    (By.CSS_SELECTOR, "div[class*='product']"),
                    (By.CSS_SELECTOR, "div[class*='item']"),
                    (By.CSS_SELECTOR, "div[class*='catalog']"),
                    (By.CSS_SELECTOR, "[data-product-id]"),
                    (By.CSS_SELECTOR, "[data-item-id]"),
                    (By.CSS_SELECTOR, ".product-card"),
                    (By.CSS_SELECTOR, ".product-item"),
                    (By.CSS_SELECTOR, ".catalog-item"),
                ]
                
                found = False
                for by_type, selector in product_container_selectors:
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((by_type, selector))
                        )
                        self.logger.info(f"✓ Found product containers using selector: {selector[:50]}")
                        found = True
                        break
                    except:
                        continue
                
                if not found:
                    self.logger.warning("Product containers not found with standard selectors, continuing anyway...")
                    # Wait a bit more for page to fully load
                    time.sleep(3)
            except Exception as e:
                self.logger.warning(f"Error waiting for product containers: {str(e)}, continuing anyway...")
                time.sleep(3)
            
            # Helper function to wait for loading overlay to disappear
            def wait_for_loading_overlay_to_disappear(timeout=10):
                """Wait for loading overlay to disappear"""
                try:
                    overlay_selectors = [
                        ".loadingoverlay",
                        "div.loadingoverlay",
                        "[class*='loading']",
                        "[class*='overlay']",
                        "div[style*='loading']",
                    ]
                    
                    start_time = time.time()
                    while time.time() - start_time < timeout:
                        overlay_found = False
                        for selector in overlay_selectors:
                            try:
                                overlays = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                for overlay in overlays:
                                    try:
                                        # Check if overlay is visible (display: flex or block, opacity > 0)
                                        style = overlay.get_attribute('style') or ''
                                        is_visible = overlay.is_displayed()
                                        
                                        # Check if overlay is actually visible (not hidden)
                                        if is_visible and ('display: none' not in style.lower()):
                                            overlay_found = True
                                            break
                                    except:
                                        continue
                                if overlay_found:
                                    break
                            except:
                                continue
                        
                        if not overlay_found:
                            # No visible overlay found
                            return True
                        
                        time.sleep(0.3)  # Check every 300ms
                    
                    return False  # Timeout
                except:
                    return True  # Assume overlay is gone if we can't check
            
            # Handle "Load more results" button - click twice then proceed to next step
            self.logger.info("Handling 'Load more results' button (will click twice then proceed)...")
            load_more_clicked = 0
            max_load_more_clicks = 2  # Click only twice as requested
            consecutive_no_button = 0
            max_consecutive_no_button = 3  # Stop after 3 consecutive attempts with no button
            
            while load_more_clicked < max_load_more_clicks:
                try:
                    # CRITICAL: Wait for loading overlay to disappear before trying to click
                    wait_for_loading_overlay_to_disappear(timeout=5)
                    
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
                        "#load_more_results",  # Based on error message: id="load_more_results"
                        "a#load_more_results",
                    ]
                    
                    load_more_button = None
                    
                    # First, try to find by ID (most specific - from error message)
                    try:
                        load_more_button = self.driver.find_element(By.ID, "load_more_results")
                    except NoSuchElementException:
                        pass
                    
                    # If not found by ID, try by text content
                    if not load_more_button:
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
                            time.sleep(0.5)  # Wait for scroll
                            
                            # CRITICAL: Wait for loading overlay to disappear before clicking
                            wait_for_loading_overlay_to_disappear(timeout=5)
                            
                            # Click the button
                            try:
                                load_more_button.click()
                                load_more_clicked += 1
                                consecutive_no_button = 0  # Reset counter
                                self.logger.info(f"✓ Clicked 'Load more results' button (click #{load_more_clicked}/{max_load_more_clicks})")
                                
                                # Wait for loading overlay to appear and then disappear (content is loading)
                                time.sleep(1)  # Brief wait for overlay to appear
                                wait_for_loading_overlay_to_disappear(timeout=10)  # Wait for content to load
                                
                                # Wait for new product containers to appear in DOM (NOT URL-based selectors)
                                try:
                                    WebDriverWait(self.driver, 5).until(
                                        lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[class*='product'], div[class*='item'], [data-product-id], .product-card, .product-item")) > 0
                                    )
                                except:
                                    pass  # Products might already be there
                                
                                # Additional wait for new products to fully render
                                time.sleep(2)
                                
                                # Optional: Scroll down a bit to trigger lazy loading if any
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(1)
                                
                                # Check if we've reached the limit - if so, break to proceed to next step
                                if load_more_clicked >= max_load_more_clicks:
                                    self.logger.info(f"✓ Reached click limit ({max_load_more_clicks}), proceeding to extract product URLs...")
                                    break
                                
                            except Exception as click_error:
                                error_str = str(click_error).lower()
                                if 'click intercepted' in error_str or 'loadingoverlay' in error_str:
                                    # Loading overlay is blocking - wait for it to disappear
                                    self.logger.debug("Loading overlay blocking click, waiting for it to disappear...")
                                    wait_for_loading_overlay_to_disappear(timeout=10)
                                    # Try again after overlay disappears
                                    try:
                                        load_more_button.click()
                                        load_more_clicked += 1
                                        consecutive_no_button = 0
                                        self.logger.info(f"✓ Clicked 'Load more results' button after waiting for overlay (click #{load_more_clicked}/{max_load_more_clicks})")
                                        time.sleep(1)
                                        wait_for_loading_overlay_to_disappear(timeout=10)
                                        # Wait for new product containers (NOT URL-based selectors)
                                        try:
                                            WebDriverWait(self.driver, 5).until(
                                                lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[class*='product'], div[class*='item'], [data-product-id], .product-card, .product-item")) > 0
                                            )
                                        except:
                                            pass
                                        time.sleep(2)
                                        
                                        # Check if we've reached the limit
                                        if load_more_clicked >= max_load_more_clicks:
                                            self.logger.info(f"✓ Reached click limit ({max_load_more_clicks}), proceeding to extract product URLs...")
                                            break
                                    except:
                                        # If still fails, try JavaScript click
                                        try:
                                            self.driver.execute_script("arguments[0].click();", load_more_button)
                                            load_more_clicked += 1
                                            consecutive_no_button = 0
                                            self.logger.info(f"✓ Clicked 'Load more results' button via JavaScript (click #{load_more_clicked}/{max_load_more_clicks})")
                                            time.sleep(1)
                                            wait_for_loading_overlay_to_disappear(timeout=10)
                                            # Wait for new product containers (NOT URL-based selectors)
                                            try:
                                                WebDriverWait(self.driver, 5).until(
                                                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[class*='product'], div[class*='item'], [data-product-id], .product-card, .product-item")) > 0
                                                )
                                            except:
                                                pass
                                            time.sleep(2)
                                            
                                            # Check if we've reached the limit
                                            if load_more_clicked >= max_load_more_clicks:
                                                self.logger.info(f"✓ Reached click limit ({max_load_more_clicks}), proceeding to extract product URLs...")
                                                break
                                        except Exception as js_click_error:
                                            self.logger.warning(f"JavaScript click also failed: {str(js_click_error)}")
                                            consecutive_no_button += 1
                                            if consecutive_no_button >= max_consecutive_no_button:
                                                self.logger.info("No more 'Load more results' button found or button not clickable")
                                            break
                                else:
                                    self.logger.warning(f"Error clicking load more button: {str(click_error)}")
                                    # Try JavaScript click as fallback
                                    try:
                                        self.driver.execute_script("arguments[0].click();", load_more_button)
                                        load_more_clicked += 1
                                        consecutive_no_button = 0
                                        self.logger.info(f"✓ Clicked 'Load more results' button via JavaScript (click #{load_more_clicked}/{max_load_more_clicks})")
                                        time.sleep(1)
                                        wait_for_loading_overlay_to_disappear(timeout=10)
                                        time.sleep(2)
                                        
                                        # Check if we've reached the limit
                                        if load_more_clicked >= max_load_more_clicks:
                                            self.logger.info(f"✓ Reached click limit ({max_load_more_clicks}), proceeding to extract product URLs...")
                                            break
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
                    
            self.logger.info(f"Finished clicking 'Load more results' button ({load_more_clicked}/{max_load_more_clicks} clicks) - proceeding to extract product URLs...")
                    
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
            
            # Extract ALL product URLs from the fully loaded page (not filtered by pattern)
            self.logger.info("Extracting ALL product URLs from fully loaded page...")
            
            # Wait for loading overlay to disappear before extracting
            wait_for_loading_overlay_to_disappear(timeout=5)
            time.sleep(2)  # Additional wait for content to stabilize
            
            try:
                html = self.driver.page_source
            except Exception as page_source_error:
                self.logger.error(f"Error accessing page_source: {str(page_source_error)}")
                return product_urls
            
            soup = BeautifulSoup(html, 'lxml')
            
            # NEW APPROACH: Find product items by their titles first, then extract URLs only for wheel products
            # Step 1: Find all product containers/items on the search results page
            self.logger.info("Step 1: Finding product items on search results page...")
            self.logger.info("Extracting titles from product items and filtering by WHEEL_KEYWORDS and EXCLUDE_KEYWORDS...")
            
            product_containers_found = False
            
            try:
                # Try multiple selectors to find product containers/cards on the search results page
                # Based on HTML structure: div.searchresultbox contains the product info
                product_container_selectors = [
                    "div.searchresultbox",  # Primary selector based on actual HTML structure
                    "div[class*='searchresult']",
                    "div[class*='product']",
                    "div[class*='item']",
                    "div[class*='catalog']",
                    "[data-product-id]",
                    "[data-item-id]",
                    ".product-card",
                    ".product-item",
                    ".catalog-item",
                    "article[class*='product']",
                    "li[class*='product']",
                ]
                
                for container_selector in product_container_selectors:
                    try:
                        containers = self.driver.find_elements(By.CSS_SELECTOR, container_selector)
                        if containers and len(containers) > 0:
                            self.logger.info(f"Found {len(containers)} product containers using selector: {container_selector}")
                            product_containers_found = True
                            
                            processed_count = 0
                            wheel_count = 0
                            skipped_count = 0
                            
                            for container in containers:
                                try:
                                    # Step 1: Extract title from the container FIRST
                                    # Based on HTML structure:
                                    # - Product name/model: div.mt-md > strong (e.g., "Maserati 920018041")
                                    # - Description: div.mt-xs.text-sm (e.g., "ALLOY WHEEL RIM ERACLE")
                                    # Combine both to form the full title
                                    title = ''
                                    
                                    # First, try the specific structure for scuderiacarparts.com
                                    try:
                                        # Get product name/model from div.mt-md > strong
                                        # HTML structure: <div class="mt-md"><strong>Maserati 920018041</strong></div>
                                        name_elem = None
                                        try:
                                            name_elem = container.find_element(By.CSS_SELECTOR, "div.mt-md strong")
                                        except:
                                            # Try alternative: div.mt-md > strong might not exist, try just strong within mt-md
                                            try:
                                                mt_md_div = container.find_element(By.CSS_SELECTOR, "div.mt-md")
                                                name_elem = mt_md_div.find_element(By.TAG_NAME, "strong")
                                            except:
                                                pass
                                        
                                        if name_elem:
                                            product_name = name_elem.text.strip()
                                            
                                            # Get description from div.mt-xs.text-sm
                                            # HTML structure: <div class="mt-xs text-sm">ALLOY WHEEL RIM ERACLE</div>
                                            desc_elem = None
                                            description = ''
                                            
                                            # Try multiple approaches to find the description element
                                            try:
                                                # Approach 1: Direct selector for div.mt-xs.text-sm (both classes)
                                                desc_elems = container.find_elements(By.CSS_SELECTOR, "div.mt-xs.text-sm")
                                                for desc in desc_elems:
                                                    desc_text = desc.text.strip()
                                                    # Skip if it's a label (contains "To Order", "In Stock", etc.)
                                                    if desc_text and not any(label in desc_text.upper() for label in ['TO ORDER', 'IN STOCK', 'OUT OF STOCK', 'AVAILABLE']):
                                                        # Check if it doesn't contain a label element
                                                        if not desc.find_elements(By.CSS_SELECTOR, "span.label"):
                                                            desc_elem = desc
                                                            description = desc_text
                                                            break
                                            except:
                                                pass
                                            
                                            # Approach 2: If not found, try finding all div.mt-xs and check which has text-sm class
                                            if not desc_elem:
                                                try:
                                                    desc_elems = container.find_elements(By.CSS_SELECTOR, "div.mt-xs")
                                                    for desc in desc_elems:
                                                        # Check if this element has the text-sm class
                                                        classes = desc.get_attribute('class') or ''
                                                        if 'text-sm' in classes:
                                                            desc_text = desc.text.strip()
                                                            # Skip if it's a label
                                                            if desc_text and not any(label in desc_text.upper() for label in ['TO ORDER', 'IN STOCK', 'OUT OF STOCK', 'AVAILABLE']):
                                                                # Check if it doesn't contain a label element
                                                                if not desc.find_elements(By.CSS_SELECTOR, "span.label"):
                                                                    desc_elem = desc
                                                                    description = desc_text
                                                                    break
                                                except:
                                                    pass
                                            
                                            # Approach 3: If still not found, try finding div.mt-xs that comes after div.mt-md
                                            if not desc_elem:
                                                try:
                                                    # Find all div.mt-xs elements and pick the first one that's not a label
                                                    desc_elems = container.find_elements(By.CSS_SELECTOR, "div.mt-xs")
                                                    for desc in desc_elems:
                                                        desc_text = desc.text.strip()
                                                        # Skip if it's a label or contains label text
                                                        if desc_text and not any(label in desc_text.upper() for label in ['TO ORDER', 'IN STOCK', 'OUT OF STOCK', 'AVAILABLE']):
                                                            # Check if it doesn't contain a label element
                                                            if not desc.find_elements(By.CSS_SELECTOR, "span.label"):
                                                                # Additional check: if it contains wheel-related keywords, it's likely the description
                                                                desc_upper = desc_text.upper()
                                                                if any(keyword in desc_upper for keyword in ['WHEEL', 'RIM', 'ALLOY', 'STEEL', 'CAP']):
                                                                    desc_elem = desc
                                                                    description = desc_text
                                                                    break
                                                except:
                                                    pass
                                            
                                            # Combine product name and description
                                            if description:
                                                # Remove any label text that might have been included
                                                description = re.sub(r'\s*(To Order|In Stock|Out of Stock|Available).*', '', description, flags=re.IGNORECASE)
                                                title = f"{product_name} {description}".strip()
                                            else:
                                                title = product_name
                                            
                                            if title and len(title) >= 3:
                                                self.logger.debug(f"Extracted title from mt-md/mt-xs structure: '{title[:60]}'")
                                    except:
                                        # Fallback: Try generic selectors
                                        title_selectors = [
                                            "h1, h2, h3, h4",
                                            ".product-title",
                                            ".product-name",
                                            "[class*='title']",
                                            "[class*='name']",
                                            "a[title]",  # Title attribute
                                        ]
                                        
                                        for title_selector in title_selectors:
                                            try:
                                                title_elem = container.find_element(By.CSS_SELECTOR, title_selector)
                                                if title_elem:
                                                    # Try text content first
                                                    title = title_elem.text.strip()
                                                    if not title:
                                                        # Try title attribute
                                                        title = title_elem.get_attribute('title') or title_elem.get_attribute('data-title') or ''
                                                    if title and len(title) >= 3:
                                                        break
                                            except:
                                                continue
                                        
                                        # If no title found in container, try to find link and get its text
                                        if not title:
                                            try:
                                                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                                                if link_elem:
                                                    title = link_elem.text.strip()
                                                    if not title:
                                                        title = link_elem.get_attribute('title') or link_elem.get_attribute('data-title') or ''
                                            except:
                                                pass
                                    
                                    # Step 2: Check if title matches wheel keywords (WHEEL_KEYWORDS and EXCLUDE_KEYWORDS)
                                    if title and len(title) >= 3:
                                        # Use the is_wheel_product method from base_scraper
                                        is_wheel = self.is_wheel_product(title)
                                        
                                        if is_wheel:
                                            # Step 3: Only if it's a wheel product, extract the URL
                                            try:
                                                href = None
                                                
                                                # First, try to find a link element
                                                try:
                                                    link_elem = container.find_element(By.CSS_SELECTOR, "a")
                                                    if link_elem:
                                                        href = link_elem.get_attribute('href')
                                                        if not href:
                                                            href = link_elem.get_attribute('data-url')
                                                        if not href:
                                                            href = link_elem.get_attribute('data-href')
                                                except:
                                                    pass
                                                
                                                # If no href found, try to extract from onclick attribute
                                                # HTML structure: onclick="window.location='/part/...';"
                                                if not href:
                                                    try:
                                                        onclick = container.get_attribute('onclick')
                                                        if onclick:
                                                            # Extract URL from onclick: window.location='/part/...';
                                                            match = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", onclick)
                                                            if match:
                                                                href = match.group(1)
                                                    except:
                                                        pass
                                                
                                                # If still no href, try data attributes
                                                if not href:
                                                    href = container.get_attribute('data-url') or container.get_attribute('data-href')
                                                
                                                if href and href != 'javascript:void(0)' and href != '#':
                                                    # Normalize URL
                                                    if href.startswith('javascript:'):
                                                        continue
                                                    
                                                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                                    
                                                    # Remove fragment (#)
                                                    if '#' in full_url:
                                                        full_url = full_url.split('#')[0]
                                                    
                                                    # Remove query params
                                                    if '?' in full_url:
                                                        full_url = full_url.split('?')[0]
                                                    
                                                    # Normalize trailing slashes
                                                    full_url = full_url.rstrip('/')
                                        
                                                    # Exclude non-product URLs
                                                    skip_patterns = ['/search/', '/category/', '/cart/', '/checkout/', '/account/', '/login/', '/register/', '/contact/', '/about/', '/help/']
                                                    if any(exclude in full_url.lower() for exclude in skip_patterns):
                                                        continue
                                                    
                                                    # Add to product URLs if not already present
                                                    if full_url not in product_urls:
                                                        product_urls.append(full_url)
                                                        wheel_count += 1
                                                        processed_count += 1
                                                        safe_title = self.safe_str(title[:60])
                                                        self.logger.info(f"✓ [{processed_count}] WHEEL: '{safe_title}' -> {full_url[:80]}")
                                            except Exception as url_error:
                                                self.logger.debug(f"Error extracting URL from wheel product container: {str(url_error)[:50]}")
                                        else:
                                            # Not a wheel product - filtered out by WHEEL_KEYWORDS/EXCLUDE_KEYWORDS
                                            skipped_count += 1
                                            safe_title = self.safe_str(title[:60])
                                            self.logger.debug(f"  SKIPPED (not wheel): '{safe_title}'")
                                    else:
                                        # No title found - skip this container
                                        self.logger.debug(f"  SKIPPED (no title found in container)")
                                
                                except Exception as container_error:
                                    self.logger.debug(f"Error processing container: {str(container_error)[:50]}")
                                    continue
                    
                            self.logger.info(f"✓ Processed {len(containers)} product containers")
                            self.logger.info(f"  - Wheel products found: {wheel_count}")
                            self.logger.info(f"  - Skipped (not wheel): {skipped_count}")
                            
                            if wheel_count > 0:
                                # Found wheel products, no need to try other selectors
                                break
                    except Exception as e:
                        self.logger.debug(f"Error with container selector {container_selector}: {str(e)[:50]}")
                        continue
                
                # Fallback: Try BeautifulSoup if Selenium didn't find containers
                if not product_containers_found or len(product_urls) == 0:
                    self.logger.info("Trying BeautifulSoup to find product items by title...")
                    
                    # Find all potential product containers in the HTML
                    # First try searchresultbox (specific to scuderiacarparts.com)
                    product_containers_bs = soup.find_all('div', class_=re.compile(r'searchresult', re.I))
                    
                    if not product_containers_bs:
                        product_containers_bs = soup.find_all(['div', 'article', 'li'], class_=re.compile(r'product|item|catalog', re.I))
                    
                    if not product_containers_bs:
                        # Try finding by data attributes
                        product_containers_bs = soup.find_all(attrs={'data-product-id': True}) + soup.find_all(attrs={'data-item-id': True})
                    
                    if product_containers_bs:
                        self.logger.info(f"Found {len(product_containers_bs)} product containers using BeautifulSoup")
                        
                        for container in product_containers_bs:
                            try:
                                # Extract title from container
                                # Based on HTML structure: div.mt-md > strong and div.mt-xs.text-sm
                                title = ''
                                
                                # First, try the specific structure for scuderiacarparts.com
                                try:
                                    # Get product name/model from div.mt-md > strong
                                    name_div = container.find('div', class_='mt-md')
                                    if name_div:
                                        name_strong = name_div.find('strong')
                                        if name_strong:
                                            product_name = name_strong.get_text(strip=True)
                                            
                                            # Get description from div.mt-xs.text-sm (but exclude labels)
                                            desc_divs = container.find_all('div', class_=re.compile(r'mt-xs.*text-sm', re.I))
                                            description = ''
                                            for desc_div in desc_divs:
                                                desc_text = desc_div.get_text(strip=True)
                                                # Skip if it's a label (contains "To Order", "In Stock", etc.)
                                                if desc_text and not any(label in desc_text.upper() for label in ['TO ORDER', 'IN STOCK', 'OUT OF STOCK', 'AVAILABLE']):
                                                    # Check if it doesn't contain a label element
                                                    if not desc_div.find('span', class_='label'):
                                                        description = desc_text
                                                        # Remove any label text that might have been included
                                                        description = re.sub(r'\s*(To Order|In Stock|Out of Stock|Available).*', '', description, flags=re.IGNORECASE)
                                                        break
                                            
                                            # Combine product name and description
                                            if description:
                                                title = f"{product_name} {description}".strip()
                                            else:
                                                title = product_name
                                except:
                                    pass
                                
                                # Fallback: Try generic title selectors
                                if not title:
                                    # Try finding title elements
                                    title_elem = (container.find('h1') or 
                                                container.find('h2') or 
                                                container.find('h3') or
                                                container.find('h4') or
                                                container.find(class_=re.compile(r'title|name', re.I)))
                                    
                                    if title_elem:
                                        title = title_elem.get_text(strip=True)
                                    
                                    # Try link text if no title found
                                    if not title:
                                        link = container.find('a')
                                        if link:
                                            title = link.get_text(strip=True)
                                            if not title:
                                                title = link.get('title', '')
                                
                                # Check if title matches wheel keywords
                                if title and len(title) >= 3:
                                    is_wheel = self.is_wheel_product(title)
                                    
                                    if is_wheel:
                                        # Extract URL - try multiple sources
                                        href = None
                                        
                                        # First try link href
                                        link = container.find('a', href=True)
                                        if link:
                                            href = link.get('href', '')
                                        
                                        # If no href, try onclick attribute
                                        if not href:
                                            onclick = container.get('onclick', '')
                                            if onclick:
                                                # Extract URL from onclick: window.location='/part/...';
                                                match = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", onclick)
                                                if match:
                                                    href = match.group(1)
                                        
                                        # If still no href, try data attributes
                                        if not href:
                                            href = container.get('data-url') or container.get('data-href')
                                        
                                        if href and href != 'javascript:void(0)' and href != '#':
                                            if href.startswith('javascript:'):
                                                continue
                                            
                                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                            if '#' in full_url:
                                                full_url = full_url.split('#')[0]
                                            if '?' in full_url:
                                                full_url = full_url.split('?')[0]
                                            full_url = full_url.rstrip('/')
                                            
                                            # Exclude non-product URLs
                                            skip_patterns = ['/search/', '/category/', '/cart/', '/checkout/']
                                            if not any(exclude in full_url.lower() for exclude in skip_patterns):
                                                if full_url not in product_urls:
                                                    product_urls.append(full_url)
                                                    safe_title = self.safe_str(title[:60])
                                                    self.logger.info(f"✓ WHEEL (BS): '{safe_title}' -> {full_url[:80]}")
                            except Exception as bs_error:
                                self.logger.debug(f"Error processing BeautifulSoup container: {str(bs_error)[:50]}")
                                continue
                
            except Exception as e:
                self.logger.warning(f"Error finding product items by title: {str(e)}")
            
            if product_urls and len(product_urls) > 0:
                self.logger.info(f"✓ Found {len(product_urls)} wheel product URLs by filtering titles on search results page")
                self.logger.info(f"Sample wheel product URLs: {product_urls[:5]}")
            else:
                self.logger.warning("⚠️ No wheel product URLs found! No product items matched WHEEL_KEYWORDS and EXCLUDE_KEYWORDS.")
                self.logger.warning("This could mean:")
                self.logger.warning("  1. No product containers were found on the page")
                self.logger.warning("  2. No product titles matched WHEEL_KEYWORDS and EXCLUDE_KEYWORDS")
                self.logger.warning("  3. An error occurred before URLs could be extracted")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            # Don't return empty list here - return whatever URLs were found before the error
            # product_urls should still contain any URLs found before the exception
        
        # Always return product_urls (even if empty) so calling code can proceed
        self.logger.info(f"Returning {len(product_urls)} product URLs from _search_for_wheels()")
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
            # Based on user's HTML structure: title should combine h1 > span and h2.subh1
            try:
                # First try: h1 > span (specific structure for scuderiacarparts)
                h1_elem = soup.find('h1')
                title_parts = []
                
                if h1_elem:
                    span_elem = h1_elem.find('span')
                    if span_elem:
                        title_parts.append(span_elem.get_text(strip=True))
                
                # Also get h2.subh1 for full title
                h2_elem = soup.find('h2', class_='subh1')
                if h2_elem:
                    h2_text = h2_elem.get_text(strip=True)
                    if h2_text:
                        title_parts.append(h2_text)
                
                # Combine title parts
                if title_parts:
                    product_data['title'] = ' '.join(title_parts)
                
                # Fallback: try h1 with class 'product-title'
                if not product_data['title']:
                    title_elem = soup.find('h1', class_='product-title')
                    if title_elem:
                        product_data['title'] = title_elem.get_text(strip=True)
                
                # Fallback: try any h1
                if not product_data['title']:
                    title_elem = soup.find('h1')
                    if title_elem:
                        product_data['title'] = title_elem.get_text(strip=True)
                
                    # Last resort: try to find title in meta tags
                if not product_data['title']:
                    title_elem = soup.find('meta', property='og:title')
                    if title_elem:
                        product_data['title'] = title_elem.get('content', '').strip()
                    else:
                        # Try page title
                        title_tag = soup.find('title')
                        if title_tag:
                            product_data['title'] = title_tag.get_text(strip=True)
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
            # Based on user's HTML structure: part number is in p.mt-sm
            try:
                # Helper function to extract just the part number from text (remove labels)
                def extract_part_number(text):
                    """Extract just the part number, removing labels like 'Part Number: '"""
                    if not text:
                        return ''
                    text = str(text).strip()
                    # Remove common labels
                    text = re.sub(r'^part\s*number\s*:?\s*', '', text, flags=re.IGNORECASE)
                    text = re.sub(r'^sku\s*:?\s*', '', text, flags=re.IGNORECASE)
                    text = re.sub(r'^pn\s*:?\s*', '', text, flags=re.IGNORECASE)
                    # Return the full part number text (may contain spaces like "36A 601 025 AD")
                    # Don't extract just the first alphanumeric sequence - keep the full part number
                    return text.strip()
                
                # First try: p.mt-sm (specific structure for scuderiacarparts)
                pn_elem = soup.find('p', class_='mt-sm')
                if pn_elem:
                    raw_text = pn_elem.get_text(strip=True)
                    # Extract just the part number (remove "Part Number: " label)
                    part_number = extract_part_number(raw_text)
                    if part_number:
                        # SKU: keep with spaces (e.g., "36A 601 025 AD")
                        product_data['sku'] = part_number
                        # PN: remove all spaces (e.g., "36A601025AD")
                        product_data['pn'] = re.sub(r'\s+', '', part_number)
                        self.logger.info(f"📦 Found Part Number from p.mt-sm: SKU='{product_data['sku']}', PN='{product_data['pn']}'")
                
                # Fallback: try other common selectors
                if not product_data['sku']:
                    sku_elem = (soup.find('span', class_=re.compile(r'sku|part.*number|part-number', re.I)) or
                               soup.find('div', class_=re.compile(r'sku|part.*number|part-number', re.I)) or
                               soup.find('span', class_='sku-display') or
                               soup.find('span', id=re.compile(r'sku|part.*number', re.I)) or
                               soup.find('div', id=re.compile(r'sku|part.*number', re.I)))
                    if sku_elem:
                        raw_text = sku_elem.get_text(strip=True)
                        part_number = extract_part_number(raw_text)
                        if part_number:
                            # SKU: keep with spaces
                            product_data['sku'] = part_number
                            # PN: remove all spaces
                            product_data['pn'] = re.sub(r'\s+', '', part_number)
                
                # Last resort: try to extract from meta tags or data attributes
                if not product_data['sku']:
                    sku_meta = soup.find('meta', property=re.compile(r'sku|part.*number', re.I))
                    if sku_meta:
                        raw_text = sku_meta.get('content', '').strip()
                        part_number = extract_part_number(raw_text)
                        if part_number:
                            # SKU: keep with spaces
                            product_data['sku'] = part_number
                            # PN: remove all spaces
                            product_data['pn'] = re.sub(r'\s+', '', part_number)
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
            # Based on user's HTML structure: price is in p#part-price-right > span.bold.text-lg
            # Price is in EUR and needs to be converted to USD
            try:
                # First try: p#part-price-right > span.bold.text-lg (specific structure for scuderiacarparts)
                price_container = soup.find('p', id='part-price-right')
                if price_container:
                    # Look for span with both 'bold' and 'text-lg' classes
                    price_spans = price_container.find_all('span')
                    price_span = None
                    for span in price_spans:
                        span_classes = span.get('class', [])
                        if isinstance(span_classes, str):
                            span_classes = [span_classes]
                        span_classes_str = ' '.join(span_classes).lower()
                        if 'bold' in span_classes_str and 'text-lg' in span_classes_str:
                            price_span = span
                            break
                    
                    if price_span:
                        price_text = price_span.get_text(strip=True)
                        # Check if price contains EUR symbol
                        if '€' in price_text or 'EUR' in price_text.upper():
                            # Extract numeric value (handle format like "€510.76 each")
                            price_value = self.extract_price(price_text)
                            if price_value:
                                # Convert EUR to USD
                                product_data['actual_price'] = self.convert_currency(price_value, 'EUR', 'USD')
                                self.logger.info(f"💰 Found EUR price: €{price_value}, converted to USD: ${product_data['actual_price']}")
                        else:
                            # Already in USD or other currency
                            product_data['actual_price'] = self.extract_price(price_text)
                            self.logger.info(f"💰 Found price from p#part-price-right > span.bold.text-lg: {product_data['actual_price']}")
                
                # Fallback: try other common selectors
                if not product_data['actual_price']:
                    sale_price_elem = (soup.find('span', class_=re.compile(r'sale.*price|price.*sale|current.*price', re.I)) or
                                     soup.find('div', class_=re.compile(r'sale.*price|price.*sale|current.*price', re.I)) or
                                     soup.find('strong', class_=re.compile(r'sale.*price|price.*sale', re.I)) or
                                     soup.find('span', class_='sale-price') or
                                     soup.find('span', class_='price') or
                                     soup.find('div', class_='price'))
                    if sale_price_elem:
                        price_text = sale_price_elem.get_text(strip=True)
                        # Check if price contains EUR symbol
                        if '€' in price_text or 'EUR' in price_text.upper():
                            price_value = self.extract_price(price_text)
                            if price_value:
                                product_data['actual_price'] = self.convert_currency(price_value, 'EUR', 'USD')
                        else:
                            product_data['actual_price'] = self.extract_price(price_text)
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
            # Based on user's HTML structure: image is img.img-responsive within #part-image-left a
            try:
                img_elem = None
                
                # Helper function to check if element has all required classes
                def has_classes(elem, *required_classes):
                    if not elem:
                        return False
                    elem_classes = elem.get('class', [])
                    if isinstance(elem_classes, str):
                        elem_classes = [elem_classes]
                    elem_classes_str = ' '.join(elem_classes).lower()
                    return all(cls.lower() in elem_classes_str for cls in required_classes)
                
                # First try: #part-image-left a > img.img-responsive (specific structure for scuderiacarparts)
                part_image_left = soup.find('div', id='part-image-left')
                if part_image_left:
                    img_link = part_image_left.find('a')
                    if img_link:
                        img_elem = img_link.find('img', class_='img-responsive')
                        if not img_elem:
                            # Fallback: any img within the link
                            img_elem = img_link.find('img')
                
                # Fallback: try a.img-thumbnail.lightbox > img.img-responsive
                if not img_elem:
                    img_links = soup.find_all('a')
                    for img_link in img_links:
                        if has_classes(img_link, 'img-thumbnail', 'lightbox'):
                            img_elem = img_link.find('img', class_='img-responsive')
                            if not img_elem:
                                img_elem = img_link.find('img')
                            if img_elem:
                                break
                
                # Fallback: try direct img with img-responsive class
                if not img_elem:
                    img_elem = soup.find('img', class_='img-responsive')
                
                # Fallback: try other common selectors
                if not img_elem:
                    img_elem = (soup.find('img', class_=re.compile(r'product.*image|main.*image', re.I)) or
                              soup.find('img', id=re.compile(r'product.*image|main.*image', re.I)) or
                              soup.find('img', class_='product-main-image') or
                              soup.find('img', class_='product-image') or
                              (soup.find('div', class_=re.compile(r'product.*image', re.I)).find('img') if soup.find('div', class_=re.compile(r'product.*image', re.I)) else None))
                
                if img_elem:
                    img_url = (img_elem.get('src') or 
                             img_elem.get('data-src') or 
                             img_elem.get('data-lazy-src') or
                             img_elem.get('data-original') or
                             img_elem.get('data-image'))
                    if img_url:
                        # Exclude payment card icons and other non-product images
                        if 'payment-card-icons' in img_url or 'frame' in img_url.lower():
                            self.logger.warning(f"⚠️ Skipping non-product image: {img_url[:80]}")
                            img_url = None
                        
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
                            self.logger.info(f"🖼️ Found image URL: {product_data['image_url'][:80]}")
            except Exception as e:
                self.logger.debug(f"Error extracting image URL: {str(e)}")
            
            # Extract basic info from main page first (before clicking tabs)
            # Title, price, SKU, image are usually visible on main page
            
            # Extract "replaces" information from "Supersession Information" section (green box)
            # This is usually visible on the main page before clicking tabs
            try:
                # Look for "Supersession Information" section - could be in various containers
                # Try to find by text content first
                supersession_containers = soup.find_all(['div', 'section', 'p', 'span'], 
                    string=re.compile(r'supersession|superseded|replaced', re.I))
                
                # Also try to find by class/id that might indicate a green box or info box
                info_boxes = soup.find_all(['div', 'section'], 
                    class_=re.compile(r'alert|info|notice|supersession|green|success', re.I))
                
                # Combine both approaches
                all_containers = list(supersession_containers) + list(info_boxes)
                
                for container in all_containers:
                    # Get parent container if this is just text
                    parent = container if container.name in ['div', 'section'] else container.find_parent(['div', 'section'])
                    if not parent:
                        parent = container
                    
                    # Get all text from the container
                    container_text = parent.get_text(separator=' ', strip=True)
                    
                    # Check if it contains supersession information
                    if re.search(r'supersession|superseded|replaced', container_text, re.I):
                        # Extract the part numbers mentioned (e.g., "part 670025877 has been superseded with part 980156641")
                        # Pattern: "part X has been superseded with part Y" or "part X superseded with part Y"
                        supersession_pattern = r'part\s+(\d+)\s+has\s+been\s+superseded\s+with\s+part\s+(\d+)'
                        match = re.search(supersession_pattern, container_text, re.I)
                        if match:
                            old_part = match.group(1)
                            new_part = match.group(2)
                            # Only export the new/replacement part number, not the old one
                            product_data['replaces'] = new_part
                            self.logger.info(f"🔄 Found replaces from Supersession Information: {product_data['replaces']} (replaces {old_part})")
                            break
                        else:
                            # Fallback: just extract the text
                            product_data['replaces'] = container_text[:500]  # Limit length
                            self.logger.info(f"🔄 Found replaces text: {product_data['replaces'][:100]}")
                            break
                
                # If not found, try to find by looking for specific text patterns in the page
                if not product_data['replaces']:
                    page_text = soup.get_text(separator=' ', strip=True)
                    supersession_match = re.search(
                        r'(?:The\s+)?(?:\w+\s+)?part\s+(\d+)\s+has\s+been\s+superseded\s+with\s+part\s+(\d+)',
                        page_text, re.I
                    )
                    if supersession_match:
                        old_part = supersession_match.group(1)
                        new_part = supersession_match.group(2)
                        # Only export the new/replacement part number, not the old one
                        product_data['replaces'] = new_part
                        self.logger.info(f"🔄 Found replaces from page text: {product_data['replaces']} (replaces {old_part})")
            except Exception as e:
                self.logger.debug(f"Error extracting replaces from Supersession Information: {str(e)}")
            
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
                    # Based on user's HTML structure: description is in p > strong.custom-blacktext followed by text
                    try:
                        # First try: p > strong.custom-blacktext (specific structure for scuderiacarparts)
                        # Find all strong elements with custom-blacktext class
                        desc_strongs = soup.find_all('strong', class_='custom-blacktext')
                        for desc_strong in desc_strongs:
                            # Check if this strong element contains "Product Description:" text
                            strong_text = desc_strong.get_text(strip=True)
                            if 'Product Description' in strong_text or 'product description' in strong_text.lower():
                                # Get the parent p tag
                                desc_p = desc_strong.find_parent('p')
                                if desc_p:
                                    # Get all text from the p tag, but skip the "Product Description:" label
                                    desc_text = desc_p.get_text(strip=True, separator=' ')
                                    # Remove "Product Description:" label if present (more aggressive removal)
                                    desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                                    # Also remove any leading/trailing whitespace and <br> tags effect
                                    desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                                    if desc_text and len(desc_text) > 10:  # Ensure it's not just a label
                                        product_data['description'] = desc_text
                                        self.logger.info(f"📝 Found description from p > strong.custom-blacktext: {desc_text[:100]}...")
                                        break
                        
                        # Fallback: try other common selectors (but exclude elements that contain "Availability & Shipping")
                        if not product_data['description']:
                            desc_elems = (soup.find_all('div', class_=re.compile(r'description|product.*description', re.I)) +
                                        soup.find_all('span', class_=re.compile(r'description', re.I)) +
                                        soup.find_all('p', class_=re.compile(r'description', re.I)) +
                                        soup.find_all('div', id=re.compile(r'description', re.I)) +
                                        soup.find_all('section', class_=re.compile(r'description', re.I)))
                            for desc_elem in desc_elems:
                                desc_text = desc_elem.get_text(strip=True, separator=' ')
                                # Skip if it contains "Availability & Shipping" or other non-description text
                                if desc_text and len(desc_text) > 10 and 'Availability & Shipping' not in desc_text:
                                    # Check if it contains "Product Description:" label
                                    if 'Product Description' in desc_text or 'product description' in desc_text.lower():
                                        desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                                        desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                                        if desc_text and len(desc_text) > 10:
                                            product_data['description'] = desc_text
                                            break
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
                        # Try p > strong.custom-blacktext first
                        desc_strongs = soup.find_all('strong', class_='custom-blacktext')
                        for desc_strong in desc_strongs:
                            strong_text = desc_strong.get_text(strip=True)
                            if 'Product Description' in strong_text or 'product description' in strong_text.lower():
                                desc_p = desc_strong.find_parent('p')
                                if desc_p:
                                    desc_text = desc_p.get_text(strip=True, separator=' ')
                                    desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                                    desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                                    if desc_text and len(desc_text) > 10:
                                        product_data['description'] = desc_text
                                        break
                        
                        # Fallback to other selectors (but exclude "Availability & Shipping")
                        if not product_data['description']:
                            desc_elems = (soup.find_all('div', class_=re.compile(r'description|product.*description', re.I)) +
                                        soup.find_all('span', class_=re.compile(r'description', re.I)) +
                                        soup.find_all('p', class_=re.compile(r'description', re.I)))
                            for desc_elem in desc_elems:
                                desc_text = desc_elem.get_text(strip=True, separator=' ')
                                if desc_text and len(desc_text) > 10 and 'Availability & Shipping' not in desc_text:
                                    if 'Product Description' in desc_text or 'product description' in desc_text.lower():
                                        desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                                        desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                                        if desc_text and len(desc_text) > 10:
                                            product_data['description'] = desc_text
                                            break
                    except Exception as e:
                        self.logger.debug(f"Error extracting description: {str(e)}")
            except Exception as e:
                self.logger.warning(f"Error handling Specifications tab: {str(e)}")
                # Fallback: try to extract description from current page if an error occurred before the 'else' block
                try:
                    # Try p > strong.custom-blacktext first
                    desc_strongs = soup.find_all('strong', class_='custom-blacktext')
                    for desc_strong in desc_strongs:
                        strong_text = desc_strong.get_text(strip=True)
                        if 'Product Description' in strong_text or 'product description' in strong_text.lower():
                            desc_p = desc_strong.find_parent('p')
                            if desc_p:
                                desc_text = desc_p.get_text(strip=True, separator=' ')
                                desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                                desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                                if desc_text and len(desc_text) > 10:
                                    product_data['description'] = desc_text
                                    break
                    
                    # Fallback to other selectors (but exclude "Availability & Shipping")
                    if not product_data['description']:
                        desc_elems = (soup.find_all('div', class_=re.compile(r'description|product.*description', re.I)) +
                                    soup.find_all('span', class_=re.compile(r'description', re.I)) +
                                    soup.find_all('p', class_=re.compile(r'description', re.I)))
                        for desc_elem in desc_elems:
                            desc_text = desc_elem.get_text(strip=True, separator=' ')
                            if desc_text and len(desc_text) > 10 and 'Availability & Shipping' not in desc_text:
                                if 'Product Description' in desc_text or 'product description' in desc_text.lower():
                                    desc_text = re.sub(r'^Product\s+Description\s*:?\s*', '', desc_text, flags=re.IGNORECASE)
                                    desc_text = re.sub(r'\s+', ' ', desc_text).strip()
                                    if desc_text and len(desc_text) > 10:
                                        product_data['description'] = desc_text
                                        break
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
                    # Get updated HTML after clicking Fitment Details tab
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Based on user's HTML structure: fitments are in div#fitment tab's table
                    # Format: "Range Rover Evoque (2012-2018) [2.0 Turbo Diesel]"
                    # Model: "Range Rover Evoque (2012-2018)"
                    # Engine: "2.0 Turbo Diesel"
                    try:
                        # Find the fitment tab content
                        fitment_div = soup.find('div', id='fitment')
                        if not fitment_div:
                            # Try alternative selectors
                            fitment_div = (soup.find('div', class_=re.compile(r'fitment', re.I)) or
                                         soup.find('div', {'data-tab': 'fitment'}) or
                                         soup.find('div', {'data-tab-content': 'fitment'}))
                        
                        if fitment_div:
                            # Find table within fitment div
                            fitment_table = fitment_div.find('table')
                            if fitment_table:
                                rows = fitment_table.find_all('tr')
                                for row in rows[1:]:  # Skip header row
                                    cells = row.find_all(['td', 'th'])
                                    if len(cells) >= 1:
                                        try:
                                            # IMPORTANT: Only use the FIRST cell for model (not all cells combined)
                                            # The first cell contains the model, the second cell contains subcategory
                                            # Example: <td>Bentayga (2015-2020)</td> <td>Complete wheels and tires, Mulliner Styling Specification</td>
                                            # We only want "Bentayga (2015-2020)" as model, not the subcategory
                                            first_cell = cells[0]
                                            cell_text = first_cell.get_text(strip=True)
                                            
                                            # Parse format: "Range Rover Evoque (2012-2018) [2.0 Turbo Diesel]" or "Bentayga (2015-2020)"
                                            # Model should be: "Range Rover Evoque (2012-2018)" or "Bentayga (2015-2020)" (keep full string with year range)
                                            # Engine should be: "2.0 Turbo Diesel" (extracted from brackets, if present)
                                            # Year should be: "2012" (extracted from year range start)
                                            # Make should be extracted from model string if possible
                                            
                                            # Pattern: text with optional (year range) followed by [engine]
                                            # Example: "Range Rover Evoque (2012-2018) [2.0 Turbo Diesel]"
                                            engine_pattern = r'\[([^\]]+)\]'
                                            engine_match = re.search(engine_pattern, cell_text)
                                            engine = engine_match.group(1).strip() if engine_match else ''
                                            
                                            # Remove engine from cell_text to get model part
                                            model_text = re.sub(r'\s*\[[^\]]+\]', '', cell_text).strip()
                                            
                                            # Extract year range from model if present (e.g., "(2012-2018)")
                                            year_range_match = re.search(r'\((\d{4})-(\d{4})\)', model_text)
                                            if year_range_match:
                                                year_start = year_range_match.group(1)
                                                year_end = year_range_match.group(2)
                                                # Use start year as primary year
                                                year = year_start
                                                # Keep the full model string with year range: "Range Rover Evoque (2012-2018)"
                                                model = model_text.strip()
                                            else:
                                                year = ''
                                                model = model_text.strip()
                                            
                                            # Extract make from model string
                                            # For "Range Rover Evoque (2012-2018)", try to extract make
                                            # Common patterns: "Land Rover Range Rover Evoque", "Range Rover Evoque"
                                            make = ''
                                            # Remove year range from model for make extraction
                                            model_without_year = re.sub(r'\s*\(\d{4}-\d{4}\)', '', model).strip()
                                            model_words = model_without_year.split()
                                            
                                            if len(model_words) >= 2:
                                                # Common patterns: "Land Rover Range Rover Evoque", "Range Rover Evoque"
                                                if model_words[0].lower() == 'land' and len(model_words) > 1:
                                                    # "Land Rover Range Rover Evoque" -> make: "Land Rover", model: "Range Rover Evoque (2012-2018)"
                                                    make = f"{model_words[0]} {model_words[1]}"
                                                elif model_words[0].lower() == 'range' and len(model_words) > 1 and model_words[1].lower() == 'rover':
                                                    # "Range Rover Evoque" -> make: "Range Rover" or "Land Rover" (heuristic)
                                                    # Try to determine if it's "Land Rover" brand or "Range Rover" model
                                                    # For now, use "Range Rover" as make, but keep full model string
                                                    make = "Range Rover"  # Could also be "Land Rover" depending on context
                                                else:
                                                    # Try first word or two as make
                                                    make = model_words[0]
                                                    if len(model_words) > 1:
                                                        # Check if second word is part of make (e.g., "Range Rover")
                                                        if model_words[1].lower() in ['rover', 'motor', 'motors', 'automotive']:
                                                            make = f"{model_words[0]} {model_words[1]}"
                                            
                                            # Keep the full model string with year range as model
                                            # Model is already set above: "Range Rover Evoque (2012-2018)"
                                            
                                            # Only add if we have at least model
                                            if model:
                                                    fitment_entry = {
                                                        'year': year,
                                                        'make': make,
                                                        'model': model,
                                                    'trim': '',
                                                        'engine': engine
                                                    }
                                                    # Check if this fitment is already added
                                                    if fitment_entry not in product_data['fitments']:
                                                        product_data['fitments'].append(fitment_entry)
                                                    self.logger.debug(f"Extracted fitment: {model} ({year}) [{engine}]")
                                        except Exception as e:
                                            self.logger.debug(f"Error parsing fitment row: {str(e)}")
                                            continue
                                
                            # If no table found, try to find fitment text in other elements
                            if not product_data['fitments']:
                                # Define patterns for parsing
                                model_pattern = r'^(.+?)(?:\s*\[|$)'
                                engine_pattern = r'\[([^\]]+)\]'
                                
                                # Look for any text that matches the pattern
                                fitment_texts = fitment_div.find_all(string=re.compile(r'\(20\d{2}-20\d{2}\)|\[.*\]'))
                                for text in fitment_texts:
                                    try:
                                        text_str = str(text).strip()
                                        if text_str:
                                            # Apply same parsing logic as above
                                            engine_pattern = r'\[([^\]]+)\]'
                                            engine_match = re.search(engine_pattern, text_str)
                                            engine = engine_match.group(1).strip() if engine_match else ''
                                            
                                            # Remove engine from text to get model part
                                            model_text = re.sub(r'\s*\[[^\]]+\]', '', text_str).strip()
                                            
                                            # Extract year range from model if present
                                            year_range_match = re.search(r'\((\d{4})-(\d{4})\)', model_text)
                                            if year_range_match:
                                                year_start = year_range_match.group(1)
                                                year = year_start
                                                # Keep the full model string with year range
                                                model = model_text.strip()
                                            else:
                                                year = ''
                                                model = model_text.strip()
                                            
                                            # Extract make from model string (same logic as above)
                                            make = ''
                                            model_without_year = re.sub(r'\s*\(\d{4}-\d{4}\)', '', model).strip()
                                            model_words = model_without_year.split()
                                            
                                            if len(model_words) >= 2:
                                                if model_words[0].lower() == 'land' and len(model_words) > 1:
                                                    make = f"{model_words[0]} {model_words[1]}"
                                                elif model_words[0].lower() == 'range' and len(model_words) > 1 and model_words[1].lower() == 'rover':
                                                    make = "Range Rover"
                                                else:
                                                    make = model_words[0]
                                                    if len(model_words) > 1:
                                                        if model_words[1].lower() in ['rover', 'motor', 'motors', 'automotive']:
                                                            make = f"{model_words[0]} {model_words[1]}"
                                            
                                            if model:
                                                fitment_entry = {
                                                    'year': year,
                                                    'make': make,
                                                    'model': model,  # Keep full model string with year range
                                                    'trim': '',
                                                    'engine': engine
                                                }
                                                if fitment_entry not in product_data['fitments']:
                                                    product_data['fitments'].append(fitment_entry)
                                    except:
                                        continue
                        
                        if product_data['fitments']:
                            self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from Fitment Details tab")
                        else:
                            self.logger.warning("No fitments found in Fitment Details tab")
                    except Exception as e:
                        self.logger.warning(f"Error extracting fitments from Fitment Details tab: {str(e)}")
                        import traceback
                        self.logger.debug(f"Traceback: {traceback.format_exc()}")
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