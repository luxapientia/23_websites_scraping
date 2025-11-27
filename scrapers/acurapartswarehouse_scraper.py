"""Scraper for acurapartswarehouse.com (Acura parts)"""
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

class AcuraPartsWarehouseScraper(BaseScraper):
    """Scraper for acurapartswarehouse.com"""
    
    def __init__(self):
        super().__init__('acurapartswarehouse', use_selenium=True)
        self.base_url = 'https://www.acurapartswarehouse.com'
        
    def get_product_urls(self):
        """
        Get all wheel product URLs from acurapartswarehouse.com
        Comprehensive discovery strategy:
        1. Discover wheel-related category/listing pages (e.g., /oem-acura-spare_wheel.html)
        2. Discover individual product pages in /oem/ directory
        3. Discover model-specific wheel pages in /parts-list/
        4. Discover accessory pages in /accessories/
        5. Browse category pages
        6. Use search as fallback
        """
        product_urls = []
        
        try:
            # METHOD 1: Discover wheel-related category/listing pages
            self.logger.info("METHOD 1: Discovering wheel-related category/listing pages...")
            category_listing_urls = self._discover_wheel_category_pages()
            product_urls.extend(category_listing_urls)
            self.logger.info(f"Found {len(category_listing_urls)} wheel category/listing pages")
            
            # METHOD 2: Discover individual product pages in /oem/ directory
            self.logger.info("METHOD 2: Discovering individual product pages in /oem/ directory...")
            oem_product_urls = self._discover_oem_product_pages()
            product_urls.extend(oem_product_urls)
            self.logger.info(f"Found {len(oem_product_urls)} individual product pages in /oem/")
            
            # METHOD 3: Discover model-specific wheel pages
            self.logger.info("METHOD 3: Discovering model-specific wheel pages...")
            model_wheel_urls = self._discover_model_wheel_pages()
            product_urls.extend(model_wheel_urls)
            self.logger.info(f"Found {len(model_wheel_urls)} model-specific wheel pages")
            
            # METHOD 4: Discover accessory pages
            self.logger.info("METHOD 4: Discovering wheel accessory pages...")
            accessory_urls = self._discover_accessory_pages()
            product_urls.extend(accessory_urls)
            self.logger.info(f"Found {len(accessory_urls)} wheel accessory pages")
            
            # METHOD 5: Browse category pages (wheels are typically in chassis category)
            self.logger.info("METHOD 5: Browsing category pages...")
            categories = [
                'acura-chassis',  # Most likely to have wheels
                'acura-body_air_conditioning',  # May have wheel covers
                'acura-interior_bumper',  # May have wheel-related accessories
            ]
            
            for category in categories:
                category_urls = self._browse_category(category)
                product_urls.extend(category_urls)
                self.logger.info(f"Category {category}: Found {len(category_urls)} wheel product URLs")
            
            # Remove duplicates
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique wheel product URLs after discovery: {len(product_urls)}")
            
            # METHOD 6: Try search as additional method
            self.logger.info("METHOD 6: Using search as additional method...")
            search_urls = self._search_for_wheels()
            for url in search_urls:
                if url not in product_urls:
                    product_urls.append(url)
            self.logger.info(f"After search: Total {len(product_urls)} wheel product URLs")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _is_wheel_url(self, url, link_text=''):
        """
        Check if URL or link text contains wheel-related keywords
        Returns True if it's likely a wheel product
        Uses WHEEL_KEYWORDS and EXCLUDE_KEYWORDS from base_scraper.py
        """
        url_lower = url.lower()
        text_lower = link_text.lower() if link_text else ''
        combined = f"{url_lower} {text_lower}"
        
        # Normalize hyphens and underscores to spaces for better matching
        combined = re.sub(r'[-_]', ' ', combined)
        
        # Use wheel keywords and exclude keywords from base_scraper
        wheel_keywords = self.WHEEL_KEYWORDS
        exclude_keywords = self.EXCLUDE_KEYWORDS
        
        # Check exclusions first
        for exclude in exclude_keywords:
            if exclude.lower() in combined:
                return False
        
        # Check if any wheel keyword is in URL or text
        for keyword in wheel_keywords:
            keyword_lower = keyword.lower()
            # For single-word keywords like 'rim', match as whole word
            # For multi-word keywords like 'wheel cap', use substring match
            if len(keyword.split()) == 1:
                # Single word: match as whole word using word boundaries
                pattern = r'\b' + re.escape(keyword_lower) + r'\b'
                if re.search(pattern, combined):
                    return True
            else:
                # Multi-word: use substring match
                if keyword_lower in combined:
                    return True
        
        return False
    
    def _discover_wheel_category_pages(self):
        """
        Discover wheel-related category/listing pages like:
        - /oem-acura-spare_wheel.html
        - /oem-acura-zdx-rims.html
        - /oem-acura-wheel_cover.html
        - /oem-acura-rims.html
        Then extract all product links from these pages
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Known wheel-related category pages
            wheel_category_pages = [
                'oem-acura-spare_wheel',
                'oem-acura-rims',
                'oem-acura-wheel_cover',
                'oem-acura-alloy_wheel',
                'oem-acura-steel_wheel',
                'oem-acura-wheel_cap',
                'oem-acura-hub_cap',
                'oem-acura-center_cap',
            ]
            
            for category_page in wheel_category_pages:
                category_url = f"{self.base_url}/{category_page}.html"
                self.logger.info(f"Discovering products from: {category_url}")
                
                try:
                    html = self.get_page(category_url, use_selenium=True, wait_time=2)
                    if not html:
                        continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Find all product links - multiple patterns
                    link_patterns = [
                        r'/oem-acura-',  # Category listing links
                        r'/oem/acura~',  # Individual product pages
                        r'/parts-list/',  # Model-specific pages
                    ]
                    
                    for pattern in link_patterns:
                        links = soup.find_all('a', href=re.compile(pattern))
                        
                        for link in links:
                            href = link.get('href', '')
                            if href:
                                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                
                                # Remove fragment and query params
                                if '#' in full_url:
                                    full_url = full_url.split('#')[0]
                                if '?' in full_url:
                                    full_url = full_url.split('?')[0]
                                
                                full_url = full_url.rstrip('/')
                                
                                # Filter for wheel products
                                link_text = link.get_text(strip=True)
                                if self._is_wheel_url(full_url, link_text):
                                    if full_url not in product_urls:
                                        product_urls.append(full_url)
                    
                    # Also extract individual product links from this category page
                    # Look for links to individual products (they may be listed on this page)
                    # Scroll to load all products if lazy loading
                    try:
                        last_height = self.driver.execute_script("return document.body.scrollHeight")
                        scroll_attempts = 0
                        while scroll_attempts < 20:
                            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(1)
                            new_height = self.driver.execute_script("return document.body.scrollHeight")
                            if new_height == last_height:
                                break
                            last_height = new_height
                            scroll_attempts += 1
                        
                        # Get updated HTML after scrolling
                        html = self.driver.page_source
                        soup = BeautifulSoup(html, 'lxml')
                    except:
                        pass
                    
                    # Extract all product links - multiple patterns
                    all_product_links = soup.find_all('a', href=re.compile(r'/oem/acura~|/oem-acura-|/parts-list/.*wheels|/accessories/acura-'))
                    for link in all_product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            full_url = full_url.rstrip('/')
                            
                            link_text = link.get_text(strip=True)
                            if self._is_wheel_url(full_url, link_text):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    
                    # Handle pagination on category pages
                    # Look for pagination links and extract products from each page
                    pagination_links = soup.find_all('a', href=re.compile(rf'{re.escape(category_page)}.*page|p=\d+|pageNumber=\d+'))
                    unique_pag_urls = set()
                    for pag_link in pagination_links[:20]:  # Limit to first 20 pages
                        pag_href = pag_link.get('href', '')
                        if pag_href:
                            pag_url = pag_href if pag_href.startswith('http') else f"{self.base_url}{pag_href}"
                            if '#' in pag_url:
                                pag_url = pag_url.split('#')[0]
                            if '?' in pag_url:
                                pag_url = pag_url.split('?')[0]
                            pag_url = pag_url.rstrip('/')
                            
                            if pag_url not in unique_pag_urls:
                                unique_pag_urls.add(pag_url)
                                try:
                                    pag_html = self.get_page(pag_url, use_selenium=True, wait_time=1)
                                    if pag_html:
                                        pag_soup = BeautifulSoup(pag_html, 'lxml')
                                        pag_links = pag_soup.find_all('a', href=re.compile(r'/oem/acura~|/oem-acura-|/parts-list/.*wheels|/accessories/acura-'))
                                        for link in pag_links:
                                            href = link.get('href', '')
                                            if href:
                                                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                                if '#' in full_url:
                                                    full_url = full_url.split('#')[0]
                                                if '?' in full_url:
                                                    full_url = full_url.split('?')[0]
                                                full_url = full_url.rstrip('/')
                                                
                                                link_text = link.get_text(strip=True)
                                                if self._is_wheel_url(full_url, link_text):
                                                    if full_url not in product_urls:
                                                        product_urls.append(full_url)
                                except:
                                    continue
                    
                    time.sleep(random.uniform(1, 2))  # Delay between pages
                    
                except Exception as e:
                    self.logger.warning(f"Error discovering from {category_page}: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error discovering wheel category pages: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _discover_oem_product_pages(self):
        """
        Discover individual product pages in /oem/ directory
        Pattern: /oem/acura~*~*.html
        Example: /oem/acura~disk~wheel~17x4t~topy~42700-tk4-a51.html
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Search for wheel-related terms to find /oem/ product pages
            search_terms = ['wheel', 'rim', 'spare wheel', 'alloy wheel', 'steel wheel']
            
            for search_term in search_terms:
                search_url = f"{self.base_url}/search?search_str={search_term}"
                self.logger.info(f"Searching for /oem/ products with term: {search_term}")
                
                try:
                    html = self.get_page(search_url, use_selenium=True, wait_time=2)
                    if not html:
                        continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Find all /oem/ product links
                    oem_links = soup.find_all('a', href=re.compile(r'/oem/acura~'))
                    
                    for link in oem_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove fragment and query params
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            
                            full_url = full_url.rstrip('/')
                            
                            # Filter for wheel products
                            link_text = link.get_text(strip=True)
                            if self._is_wheel_url(full_url, link_text):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    self.logger.warning(f"Error searching for /oem/ products with '{search_term}': {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error discovering /oem/ product pages: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _discover_model_wheel_pages(self):
        """
        Discover model-specific wheel pages
        Pattern: /parts-list/{year}-acura-{model}/wheels.html
        Example: /parts-list/2024-acura-mdx/wheels.html
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Acura models
            acura_models = [
                'mdx', 'rdx', 'tlx', 'ilx', 'rlx', 'tsx', 'tl', 'rl', 'legend',
                'integra', 'cl', 'nsx', 'slx', 'vigor', 'zdx', 'ilx_hybrid'
            ]
            
            # Years range
            years = list(range(1990, 2026))  # 1990 to 2025
            
            # Strategy: Use search to find model-specific wheel pages instead of brute force
            # Search for "wheels" with model names to find /parts-list/ pages
            for model in acura_models[:10]:  # Limit to first 10 models to avoid too many requests
                search_url = f"{self.base_url}/search?search_str={model} wheels"
                
                try:
                    html = self.get_page(search_url, use_selenium=True, wait_time=1)
                    if not html:
                        continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Find /parts-list/ wheel pages
                    parts_list_links = soup.find_all('a', href=re.compile(r'/parts-list/.*wheels'))
                    
                    for link in parts_list_links:
                        href = link.get('href', '')
                        if href:
                            parts_list_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Visit this parts-list page and extract products
                            try:
                                parts_html = self.get_page(parts_list_url, use_selenium=True, wait_time=1)
                                if parts_html:
                                    parts_soup = BeautifulSoup(parts_html, 'lxml')
                                    product_links = parts_soup.find_all('a', href=re.compile(r'/oem/acura~|/oem-acura-'))
                                    
                                    for prod_link in product_links:
                                        prod_href = prod_link.get('href', '')
                                        if prod_href:
                                            full_url = prod_href if prod_href.startswith('http') else f"{self.base_url}{prod_href}"
                                            
                                            if '#' in full_url:
                                                full_url = full_url.split('#')[0]
                                            if '?' in full_url:
                                                full_url = full_url.split('?')[0]
                                            
                                            full_url = full_url.rstrip('/')
                                            
                                            link_text = prod_link.get_text(strip=True)
                                            if self._is_wheel_url(full_url, link_text):
                                                if full_url not in product_urls:
                                                    product_urls.append(full_url)
                            except:
                                continue
                    
                    time.sleep(random.uniform(0.5, 1.0))
                    
                except Exception as e:
                    continue
            
        except Exception as e:
            self.logger.error(f"Error discovering model wheel pages: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _discover_accessory_pages(self):
        """
        Discover wheel accessory pages
        Pattern: /accessories/acura-*.html
        Example: /accessories/acura-alloy_wheels.html
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Known wheel accessory pages
            accessory_pages = [
                'acura-alloy_wheels',
                'acura-wheel_covers',
                'acura-wheel_caps',
                'acura-hub_caps',
                'acura-center_caps',
            ]
            
            for accessory_page in accessory_pages:
                accessory_url = f"{self.base_url}/accessories/{accessory_page}.html"
                self.logger.info(f"Discovering products from: {accessory_url}")
                
                try:
                    html = self.get_page(accessory_url, use_selenium=True, wait_time=2)
                    if not html:
                        continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Find product links
                    product_links = soup.find_all('a', href=re.compile(r'/oem/acura~|/oem-acura-|/accessories/acura-'))
                    
                    for link in product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            
                            full_url = full_url.rstrip('/')
                            
                            link_text = link.get_text(strip=True)
                            if self._is_wheel_url(full_url, link_text):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                    
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    self.logger.warning(f"Error discovering from {accessory_page}: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error discovering accessory pages: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _browse_category(self, category_name):
        """
        Browse a category page and extract only wheel product URLs with pagination
        Filters for wheel products early based on URL and link text
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            category_url = f"{self.base_url}/category/{category_name}.html"
            self.logger.info(f"Browsing category: {category_url}")
            
            # Increase page load timeout for category pages
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                
                # Load first page
                html = self.get_page(category_url, use_selenium=True, wait_time=2)
                if not html:
                    self.logger.error(f"Failed to fetch category page: {category_url}")
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
            
            # Wait for product links to appear (multiple patterns)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-acura-'], a[href*='/oem/acura~'], a[href*='/parts-list/'], a[href*='/accessories/']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all products on current page (if lazy loading)
            self.logger.info("Scrolling to load all products on current page...")
            try:
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                scroll_attempts = 0
                max_scrolls = 50
                no_change_count = 0
                
                while scroll_attempts < max_scrolls:
                    try:
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1.5)
                        
                        try:
                            new_height = self.driver.execute_script("return document.body.scrollHeight")
                        except Exception as scroll_error:
                            self.logger.warning(f"Error checking scroll height: {str(scroll_error)}")
                            break
                        
                        if new_height == last_height:
                            no_change_count += 1
                            if no_change_count >= 3:
                                break
                        else:
                            no_change_count = 0
                        last_height = new_height
                        scroll_attempts += 1
                    except Exception as scroll_error:
                        self.logger.warning(f"Error during scrolling: {str(scroll_error)}")
                        break
            except Exception as scroll_init_error:
                self.logger.warning(f"Error initializing scroll: {str(scroll_init_error)}")
            
            # Get page source after scrolling
            try:
                html = self.driver.page_source
            except Exception as page_source_error:
                self.logger.error(f"Error accessing page_source: {str(page_source_error)}")
                html = self.get_page(category_url, use_selenium=True, wait_time=1)
                if not html:
                    self.logger.error("Could not retrieve page source")
                    return product_urls
            
            soup = BeautifulSoup(html, 'lxml')
            
            # Find ALL product links - multiple patterns
            # Pattern 1: /oem-acura-*.html (category/listing pages)
            # Pattern 2: /oem/acura~*~*.html (individual product pages)
            # Pattern 3: /parts-list/*/wheels.html (model-specific pages)
            # Pattern 4: /accessories/acura-*.html (accessory pages)
            product_links = soup.find_all('a', href=re.compile(r'/oem-acura-|/oem/acura~|/parts-list/.*wheels|/accessories/acura-'))
            
            for link in product_links:
                href = link.get('href', '')
                if href:
                    # Normalize URL
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    
                    # Remove fragment (#)
                    if '#' in full_url:
                        full_url = full_url.split('#')[0]
                    
                    # Extract only the base product URL, remove query params
                    if '?' in full_url:
                        full_url = full_url.split('?')[0]
                    
                    # Normalize trailing slashes
                    full_url = full_url.rstrip('/')
                    
                    # Filter for wheel products only
                    link_text = link.get_text(strip=True)
                    if self._is_wheel_url(full_url, link_text):
                        if full_url not in product_urls:
                            product_urls.append(full_url)
            
            self.logger.info(f"Found {len(product_links)} product links on page 1, {len(product_urls)} unique URLs")
            
            # Handle pagination - iterate through all pages
            page_num = 2
            max_pages = 500  # Safety limit
            consecutive_empty_pages = 0
            max_consecutive_empty = 4  # Stop after 4 consecutive pages with no new products
            
            while page_num <= max_pages:
                try:
                    self.logger.info(f"Loading category page {page_num}...")
                    
                    # Try multiple pagination URL patterns
                    pagination_urls = [
                        f"{self.base_url}/category/{category_name}.html?page={page_num}",
                        f"{self.base_url}/category/{category_name}.html?p={page_num}",
                        f"{self.base_url}/category/{category_name}.html?pageNumber={page_num}",
                        f"{self.base_url}/category/{category_name}/page/{page_num}",
                        f"{self.base_url}/category/{category_name}/p/{page_num}",
                    ]
                    
                    page_loaded = False
                    pag_url_used = None
                    
                    for pag_url in pagination_urls:
                        try:
                            self.logger.debug(f"Trying pagination URL: {pag_url}")
                            
                            original_pag_timeout = self.page_load_timeout
                            try:
                                self.page_load_timeout = 60
                                self.driver.set_page_load_timeout(60)
                                
                                self.driver.get(pag_url)
                                time.sleep(2)
                                
                                try:
                                    WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-acura-'], a[href*='/oem/acura~'], a[href*='/parts-list/'], a[href*='/accessories/']"))
                                    )
                                except:
                                    self.logger.debug(f"Product links not found immediately on {pag_url}, continuing...")
                                
                                page_links_check = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/oem-acura-'], a[href*='/oem/acura~'], a[href*='/parts-list/'], a[href*='/accessories/']")
                                if len(page_links_check) > 0:
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
                    
                    # Scroll to load all products on this page
                    try:
                        last_height = self.driver.execute_script("return document.body.scrollHeight")
                        scroll_attempts = 0
                        no_change_count = 0
                        
                        while scroll_attempts < 30:
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
                    
                    # Extract product links from this page
                    try:
                        html = self.driver.page_source
                    except Exception as page_source_error:
                        self.logger.warning(f"Error accessing page_source on page {page_num}: {str(page_source_error)}")
                        html = self.get_page(pag_url_used, use_selenium=True, wait_time=1)
                        if not html:
                            self.logger.warning(f"Could not retrieve page source for page {page_num}, skipping")
                            page_num += 1
                            continue
                    
                    soup = BeautifulSoup(html, 'lxml')
                    # Find all product links - multiple patterns
                    page_links = soup.find_all('a', href=re.compile(r'/oem-acura-|/oem/acura~|/parts-list/.*wheels|/accessories/acura-'))
                    
                    page_urls_count = 0
                    for link in page_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            
                            # Remove query params
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            
                            full_url = full_url.rstrip('/')
                            
                            # Filter for wheel products only
                            link_text = link.get_text(strip=True)
                            if self._is_wheel_url(full_url, link_text):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                                    page_urls_count += 1
                    
                    self.logger.info(f"Page {page_num}: Found {len(page_links)} product links, {page_urls_count} new unique URLs (Total: {len(product_urls)})")
                    
                    if page_urls_count == 0:
                        consecutive_empty_pages += 1
                        self.logger.warning(f"No new products on page {page_num} (consecutive empty: {consecutive_empty_pages})")
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.logger.info(f"Stopping pagination: {consecutive_empty_pages} consecutive pages with no new products")
                            break
                    else:
                        consecutive_empty_pages = 0
                    
                    page_num += 1
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        self.logger.warning(f"Stopping pagination due to errors")
                        break
                    page_num += 1
                    continue
            
            self.logger.info(f"Category browsing complete. Total unique product URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error browsing category: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """
        Search for wheels using site search - handles pagination and dynamic content
        Filters for wheel products early based on URL and link text
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Try multiple search URL patterns
            search_urls = [
                f"{self.base_url}/search?search_str=wheel",
                f"{self.base_url}/search?q=wheel",
                f"{self.base_url}/search/wheel",
            ]
            
            for search_url in search_urls:
                self.logger.info(f"Trying search: {search_url}")
                
                # Increase page load timeout for search page
                original_timeout = self.page_load_timeout
                try:
                    self.page_load_timeout = 60
                    self.driver.set_page_load_timeout(60)
                    
                    # Use get_page() instead of direct driver.get() for better error handling
                    html = self.get_page(search_url, use_selenium=True, wait_time=2)
                    if not html:
                        self.logger.warning(f"Failed to fetch search page: {search_url}")
                        continue  # Try next search URL pattern
                    
                except Exception as e:
                    self.logger.warning(f"Error loading search page {search_url}: {str(e)}")
                    continue  # Try next search URL pattern
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
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-acura-']"))
                    )
                except:
                    self.logger.warning("Product links not found immediately, continuing anyway...")
                
                # Scroll to load more products (if lazy loading)
                self.logger.info("Scrolling to load all products on current page...")
                try:
                    last_height = self.driver.execute_script("return document.body.scrollHeight")
                    scroll_attempts = 0
                    max_scrolls = 50
                    no_change_count = 0
                    
                    while scroll_attempts < max_scrolls:
                        try:
                            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(1.5)
                            
                            try:
                                new_height = self.driver.execute_script("return document.body.scrollHeight")
                            except Exception as scroll_error:
                                self.logger.warning(f"Error checking scroll height: {str(scroll_error)}")
                                break
                            
                            if new_height == last_height:
                                no_change_count += 1
                                if no_change_count >= 3:
                                    break
                            else:
                                no_change_count = 0
                            last_height = new_height
                            scroll_attempts += 1
                        except Exception as scroll_error:
                            self.logger.warning(f"Error during scrolling: {str(scroll_error)}")
                            break
                except Exception as scroll_error:
                    self.logger.warning(f"Error initializing scroll: {str(scroll_error)}")
                
                # Get page source after scrolling
                try:
                    html = self.driver.page_source
                except Exception as page_source_error:
                    self.logger.error(f"Error accessing page_source: {str(page_source_error)}")
                    html = self.get_page(search_url, use_selenium=True, wait_time=1)
                    if not html:
                        self.logger.warning("Could not retrieve page source, trying next search pattern...")
                        continue
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Find ALL product links - multiple patterns
                # Pattern 1: /oem-acura-*.html (category/listing pages)
                # Pattern 2: /oem/acura~*~*.html (individual product pages)
                # Pattern 3: /parts-list/*/wheels.html (model-specific pages)
                # Pattern 4: /accessories/acura-*.html (accessory pages)
                product_links = soup.find_all('a', href=re.compile(r'/oem-acura-|/oem/acura~|/parts-list/.*wheels|/accessories/acura-'))
                
                if len(product_links) > 0:
                    # Found products with this search pattern, extract only wheel products
                    wheel_count = 0
                    for link in product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            
                            # Remove query params
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            
                            full_url = full_url.rstrip('/')
                            
                            # Filter for wheel products only
                            link_text = link.get_text(strip=True)
                            if self._is_wheel_url(full_url, link_text):
                                if full_url not in product_urls:
                                    product_urls.append(full_url)
                                    wheel_count += 1
                    
                    self.logger.info(f"Found {len(product_links)} product links, {wheel_count} wheel products with search pattern: {search_url}")
                    # Found working search pattern, exit loop
                    break
                else:
                    self.logger.warning(f"No products found with search pattern: {search_url}")
                    continue  # Try next search pattern
            
            self.logger.info(f"Search complete. Total unique product URLs found: {len(product_urls)}")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def scrape_product(self, url):
        """
        Scrape single product from AcuraPartsWarehouse with retry logic
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
                    
                    # Quick error check - only critical errors
                    current_url = self.driver.current_url.lower()
                    title_lower = title_text.lower() if title_text else ''
                    
                    # Check if redirected away from target domain (critical)
                    if 'acurapartswarehouse.com' not in current_url and not current_url.startswith(('chrome-error://', 'about:')):
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
                    # Product URLs can contain "403" or "404" as part numbers
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
                    
                    # Additional check: if URL contains error codes but also has '/oem-acura-', it's likely a product page, not an error
                    if has_critical_error and '/oem-acura-' in current_url:
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
                    title_is_domain = title_text.lower() in ['www.acurapartswarehouse.com', 'acurapartswarehouse.com', 'acurapartswarehouse', '']
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
                    if 'acurapartswarehouse.com' in html_lower[:500] and page_is_small and not has_product_elements:
                        self.logger.warning(f"Page appears to be blocked (minimal content: {len(html)} bytes, no product elements)")
                return None
            
            self.logger.info(f"📝 Found title: {product_data['title'][:60]}")
            
            # Extract SKU with error handling
            try:
                sku_elem = None
                product_data['sku'] = ''
                
                # Method 1: Try standard selectors first
                sku_elem = soup.find('span', class_='sku-display')
                if not sku_elem:
                    # Try alternative selectors for Acura site
                    sku_elem = soup.find('span', class_=re.compile(r'sku|part.*number', re.I))
                if not sku_elem:
                    sku_elem = soup.find('div', class_=re.compile(r'sku|part.*number', re.I))
                if not sku_elem:
                    sku_elem = soup.find('td', string=re.compile(r'part.*number', re.I))
                if not sku_elem:
                    sku_elem = soup.find('th', string=re.compile(r'part.*number', re.I))
                
                if sku_elem:
                    product_data['sku'] = sku_elem.get_text(strip=True)
                    product_data['pn'] = self.clean_sku(product_data['sku'])
                    self.logger.info(f"📝 Found SKU via element: {product_data['sku']}")
                
                # Method 2: Search for "Part Number:" text and get the value after it
                if not product_data['sku']:
                    # Look for text containing "Part Number:" and extract the value
                    all_text_elements = soup.find_all(string=re.compile(r'Part Number:', re.I))
                    for text_elem in all_text_elements:
                        parent = text_elem.parent
                        if parent:
                            # Get the text after "Part Number:"
                            full_text = parent.get_text(strip=True)
                            if 'Part Number:' in full_text:
                                # Extract part number after "Part Number:"
                                match = re.search(r'Part Number:\s*([A-Z0-9\-]+)', full_text, re.I)
                                if match:
                                    product_data['sku'] = match.group(1).strip()
                                    product_data['pn'] = self.clean_sku(product_data['sku'])
                                    self.logger.info(f"📝 Found SKU via text search: {product_data['sku']}")
                                    break
                
                # Method 3: Try finding in product specifications table
                if not product_data['sku']:
                    spec_tables = soup.find_all('table', class_=re.compile(r'spec|product.*spec', re.I))
                    for table in spec_tables:
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            for i, cell in enumerate(cells):
                                cell_text = cell.get_text(strip=True)
                                if 'Part Number' in cell_text and i + 1 < len(cells):
                                    # Next cell should contain the part number
                                    next_cell = cells[i + 1]
                                    part_num = next_cell.get_text(strip=True)
                                    if part_num:
                                        product_data['sku'] = part_num
                                        product_data['pn'] = self.clean_sku(product_data['sku'])
                                        self.logger.info(f"📝 Found SKU in table: {product_data['sku']}")
                                        break
                            if product_data['sku']:
                                break
                        if product_data['sku']:
                            break
                
                # Method 4: Extract from meta tags (og:title, description)
                if not product_data['sku']:
                    # Try og:title meta tag (e.g., "42700-TZ3-A91 Genuine Acura 19" Wheel Rim")
                    og_title = soup.find('meta', property='og:title')
                    if og_title:
                        og_title_content = og_title.get('content', '')
                        if og_title_content:
                            match = re.search(r'([A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,})', og_title_content.upper())
                            if match:
                                product_data['sku'] = match.group(1)
                                product_data['pn'] = self.clean_sku(product_data['sku'])
                                self.logger.info(f"📝 Found SKU from og:title meta: {product_data['sku']}")
                    
                    # Try description meta tag
                    if not product_data['sku']:
                        desc_meta = soup.find('meta', {'name': 'description'})
                        if desc_meta:
                            desc_content = desc_meta.get('content', '')
                            if desc_content:
                                # Pattern: "Genuine 42700-TZ3-A91 19" Wheel Rim" or "AcuraPartsWarehouse offers 42700TZ3A91"
                                match = re.search(r'([A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,})', desc_content.upper())
                                if not match:
                                    # Try without dashes: "42700TZ3A91"
                                    match = re.search(r'([A-Z0-9]{8,})', desc_content.upper())
                                    if match and len(match.group(1)) >= 8:
                                        # Try to format as part number (e.g., 42700TZ3A91 -> 42700-TZ3-A91)
                                        part_num = match.group(1)
                                        # Common Acura part number pattern: XXXXX-XXX-XXX
                                        if len(part_num) >= 10:
                                            formatted = f"{part_num[:5]}-{part_num[5:8]}-{part_num[8:]}"
                                            product_data['sku'] = formatted
                                            product_data['pn'] = self.clean_sku(product_data['sku'])
                                            self.logger.info(f"📝 Found SKU from description meta (formatted): {product_data['sku']}")
                                elif match:
                                    product_data['sku'] = match.group(1)
                                    product_data['pn'] = self.clean_sku(product_data['sku'])
                                    self.logger.info(f"📝 Found SKU from description meta: {product_data['sku']}")
                
                # Method 5: Extract from canonical link URL
                if not product_data['sku']:
                    canonical_link = soup.find('link', {'rel': 'canonical'})
                    if canonical_link:
                        canonical_url = canonical_link.get('href', '')
                        if canonical_url:
                            # URLs like /oem/acura~wheel~19x8j~42700-tz3-a91.html
                            match = re.search(r'([A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,})', canonical_url.upper())
                            if match:
                                product_data['sku'] = match.group(1)
                                product_data['pn'] = self.clean_sku(product_data['sku'])
                                self.logger.info(f"📝 Found SKU from canonical link: {product_data['sku']}")
                
                # Method 6: Extract from script tags with JSON data
                if not product_data['sku']:
                    script_tags = soup.find_all('script', type=re.compile(r'application/ld\+json|application/json', re.I))
                    for script in script_tags:
                        try:
                            script_content = script.string
                            if script_content:
                                import json
                                data = json.loads(script_content)
                                # Look for SKU in various JSON-LD fields
                                if isinstance(data, dict):
                                    # Try common product schema fields
                                    sku_fields = ['sku', 'productID', 'mpn', 'partNumber', 'identifier']
                                    for field in sku_fields:
                                        if field in data:
                                            sku_value = str(data[field]).strip()
                                            if sku_value and re.match(r'^[A-Z0-9\-]+$', sku_value.upper()):
                                                product_data['sku'] = sku_value
                                                product_data['pn'] = self.clean_sku(product_data['sku'])
                                                self.logger.info(f"📝 Found SKU from JSON-LD ({field}): {product_data['sku']}")
                                                break
                                    if product_data['sku']:
                                        break
                        except (json.JSONDecodeError, AttributeError):
                            continue
                
                # Method 7: Extract from page title tag (before URL/title fallback)
                if not product_data['sku']:
                    title_tag = soup.find('title')
                    if title_tag:
                        title_text = title_tag.get_text(strip=True)
                        if title_text:
                            # Pattern: "42700-TZ3-A91 Genuine Acura 19" Wheel Rim"
                            match = re.search(r'([A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,})', title_text.upper())
                            if match:
                                product_data['sku'] = match.group(1)
                                product_data['pn'] = self.clean_sku(product_data['sku'])
                                self.logger.info(f"📝 Found SKU from page title tag: {product_data['sku']}")
                
                # Method 8: LAST RESORT - Extract from URL if part number still not found
                if not product_data['sku']:
                    # URLs like /oem/acura~disk~wheel~17x4t~topy~42700-tk4-a51.html contain part number
                    url_match = re.search(r'([A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,})', url.upper())
                    if url_match:
                        product_data['sku'] = url_match.group(1)
                        product_data['pn'] = self.clean_sku(product_data['sku'])
                        self.logger.info(f"📝 Found SKU from URL (fallback): {product_data['sku']}")
                
                # Method 9: LAST RESORT - Extract from product title if part number still not found
                if not product_data['sku'] and product_data['title']:
                    # Titles like "Acura 42700-TK4-A51 Spare Wheel" contain part number
                    title_match = re.search(r'([A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,})', product_data['title'].upper())
                    if title_match:
                        product_data['sku'] = title_match.group(1)
                        product_data['pn'] = self.clean_sku(product_data['sku'])
                        self.logger.info(f"📝 Found SKU from product title (last resort): {product_data['sku']}")
                
                # Log warning if SKU still not found
                if not product_data['sku']:
                    self.logger.warning(f"⚠️ Could not extract SKU from {url}")
                else:
                    self.logger.info(f"✅ SKU extracted: {product_data['sku']}")
                    
            except Exception as e:
                self.logger.warning(f"Error extracting SKU: {str(e)}")
                import traceback
                self.logger.debug(f"SKU extraction traceback: {traceback.format_exc()}")
            
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
                    sale_price_elem = (soup.find('span', class_='sale-price') or 
                                      soup.find('div', class_='sale-price') or
                                      soup.find('span', class_=re.compile(r'price|cost', re.I)))
                if sale_price_elem:
                    product_data['actual_price'] = self.extract_price(sale_price_elem.get_text(strip=True))
            except Exception as e:
                self.logger.debug(f"Error extracting sale price: {str(e)}")
            
            # Extract MSRP with error handling
            try:
                msrp_elem = soup.find('span', class_='list-price-value')
                if not msrp_elem:
                    # Try alternative selectors
                    msrp_elem = (soup.find('span', class_='list-price') or 
                                 soup.find('div', class_='msrp') or
                                 soup.find('span', class_=re.compile(r'msrp|list.*price', re.I)))
                if msrp_elem:
                    product_data['msrp'] = self.extract_price(msrp_elem.get_text(strip=True))
            except Exception as e:
                self.logger.debug(f"Error extracting MSRP: {str(e)}")
            
            # Extract image URL with error handling
            try:
                img_elem = soup.find('img', class_='product-main-image')
                if not img_elem:
                    # Try alternative selectors
                    img_elem = (soup.find('img', class_='product-image') or 
                               soup.find('img', id='product-image') or
                               soup.find('img', class_=re.compile(r'product.*img|main.*image', re.I)))
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
                    desc_elem = (soup.find('div', class_='description') or 
                                soup.find('p', class_='product-description') or
                                soup.find('div', class_=re.compile(r'description|product.*desc', re.I)))
                if desc_elem:
                    product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
            except Exception as e:
                self.logger.debug(f"Error extracting description: {str(e)}")
            
            # Extract also_known_as (Other Names) with error handling
            try:
                also_known_elem = soup.find('li', class_='also_known_as')
                if not also_known_elem:
                    # Try alternative selectors
                    also_known_elem = soup.find('div', class_=re.compile(r'also.*known|other.*name', re.I))
                if also_known_elem:
                    value_elem = also_known_elem.find('h2', class_='list-value') or also_known_elem.find('span', class_='value')
                    if value_elem:
                        product_data['also_known_as'] = value_elem.get_text(strip=True)
                    else:
                        product_data['also_known_as'] = also_known_elem.get_text(strip=True)
            except Exception as e:
                self.logger.debug(f"Error extracting also_known_as: {str(e)}")
            
            # Extract footnotes/notes (positions) with error handling
            try:
                notes_elem = soup.find('li', class_='footnotes')
                if not notes_elem:
                    # Try alternative selectors
                    notes_elem = soup.find('div', class_=re.compile(r'footnote|note|position', re.I))
                if notes_elem:
                    value_elem = notes_elem.find('span', class_='list-value') or notes_elem.find('span', class_='value')
                    if value_elem:
                        product_data['positions'] = value_elem.get_text(strip=True)
                    else:
                        product_data['positions'] = notes_elem.get_text(strip=True)
            except Exception as e:
                self.logger.debug(f"Error extracting positions: {str(e)}")
            
            # Extract fitment data from JSON with comprehensive error handling
            try:
                script_elem = soup.find('script', id='product_data')
                if not script_elem:
                    # Try alternative script tags
                    script_tags = soup.find_all('script', type='application/json')
                    for tag in script_tags:
                        if tag.string and 'fitment' in tag.string.lower():
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
