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
from selenium.webdriver.common.action_chains import ActionChains

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
                    
                    # Scroll to load all products on the first page (if lazy loading)
                    try:
                        last_height = self.driver.execute_script("return document.body.scrollHeight")
                        scroll_attempts = 0
                        while scroll_attempts < 30:
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
                    
                    # Extract individual product links from the first page
                    # Focus on individual product pages: /oem/acura~...~...html (not category pages)
                    # NOTE: Since we're on a wheel category page, ALL products are wheels - no filtering needed
                    self.logger.info("Extracting products from page 1...")
                    
                    # Try multiple patterns to find ALL product links
                    # Pattern 1: /oem/acura~ (individual product pages)
                    all_product_links = soup.find_all('a', href=re.compile(r'/oem/acura~'))
                    
                    # Also look for links in product cards/sections that might have different structures
                    # Pattern 2: Look for links containing part numbers in href (e.g., href containing "42700-TZ3-A91")
                    part_number_links = soup.find_all('a', href=re.compile(r'[A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,}'))
                    for link in part_number_links:
                        href = link.get('href', '')
                        if href and '/oem/acura~' in href and link not in all_product_links:
                            all_product_links.append(link)
                    
                    # Pattern 3: Look for links in product title/heading areas
                    product_sections = soup.find_all(['div', 'section', 'article'], class_=re.compile(r'product|item|card', re.I))
                    for section in product_sections:
                        section_links = section.find_all('a', href=re.compile(r'/oem/acura~'))
                        for link in section_links:
                            if link not in all_product_links:
                                all_product_links.append(link)
                    
                    self.logger.info(f"Found {len(all_product_links)} product links on page 1")
                    first_page_count = 0
                    
                    for link in all_product_links:
                        href = link.get('href', '')
                        if href:
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            
                            # Remove fragment and query params
                            if '#' in full_url:
                                full_url = full_url.split('#')[0]
                            if '?' in full_url:
                                full_url = full_url.split('?')[0]
                            
                            full_url = full_url.rstrip('/')
                            
                            # On wheel category pages, ALL products are wheels - collect all of them
                            # No need to filter - we're already on a wheel-specific category page
                            # Only collect if it's an individual product page (not a category listing page)
                            if '/oem/acura~' in full_url and full_url not in product_urls:
                                product_urls.append(full_url)
                                first_page_count += 1
                    
                    self.logger.info(f"Page 1: Collected {first_page_count} unique product URLs (Total: {len(product_urls)})")
                    
                    # Handle pagination on category pages
                    # Extract total page count from the page (e.g., "Page 1 of 20")
                    total_pages = 1
                    try:
                        # Look for "Page X of Y" pattern
                        page_info = soup.find(string=re.compile(r'Page\s+\d+\s+of\s+\d+', re.I))
                        if page_info:
                            page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', str(page_info), re.I)
                            if page_match:
                                total_pages = int(page_match.group(1))
                                self.logger.info(f"Found pagination: {total_pages} total pages")
                        
                        # Alternative: Look for "X-Y of Z Results" pattern
                        if total_pages == 1:
                            results_info = soup.find(string=re.compile(r'\d+-\d+\s+of\s+\d+\s+Results', re.I))
                            if results_info:
                                results_match = re.search(r'\d+-\d+\s+of\s+(\d+)', str(results_info), re.I)
                                if results_match:
                                    total_results = int(results_match.group(1))
                                    # Estimate pages (usually ~20 products per page)
                                    estimated_pages = max(1, (total_results + 19) // 20)
                                    total_pages = estimated_pages
                                    self.logger.info(f"Found {total_results} total results, estimating {total_pages} pages")
                        
                        # Alternative: Count pagination number links
                        if total_pages == 1:
                            pagination_numbers = soup.find_all('a', href=re.compile(r'page=\d+|p=\d+', re.I))
                            if pagination_numbers:
                                max_page_num = 1
                                for pag_link in pagination_numbers:
                                    pag_href = pag_link.get('href', '')
                                    if pag_href:
                                        # Extract page number from URL
                                        page_match = re.search(r'[?&](?:page|p)=(\d+)', pag_href, re.I)
                                        if page_match:
                                            page_num = int(page_match.group(1))
                                            max_page_num = max(max_page_num, page_num)
                                if max_page_num > 1:
                                    total_pages = max_page_num
                                    self.logger.info(f"Found pagination links up to page {total_pages}")
                    except Exception as e:
                        self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
                    
                    # Extract products from remaining pages (page 1 already extracted above)
                    if total_pages > 1:
                        self.logger.info(f"Extracting products from pages 2-{total_pages} of {category_page}")
                        
                        # Try different pagination URL patterns
                        pagination_patterns = [
                            f"{category_url}?page={{page_num}}",
                            f"{category_url}?p={{page_num}}",
                            f"{category_url}?pageNumber={{page_num}}",
                        ]
                        
                        for page_num in range(2, total_pages + 1):
                            page_found = False
                            
                            for pattern in pagination_patterns:
                                pag_url = pattern.format(page_num=page_num)
                                
                                try:
                                    self.logger.info(f"Loading page {page_num}/{total_pages}: {pag_url}")
                                    
                                    pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                                    if not pag_html or len(pag_html) < 5000:
                                        continue
                                    
                                    pag_soup = BeautifulSoup(pag_html, 'lxml')
                                    
                                    # Scroll to load all products on this page (if lazy loading)
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
                                        pag_html = self.driver.page_source
                                        pag_soup = BeautifulSoup(pag_html, 'lxml')
                                    except:
                                        pass
                                    
                                    # Extract product links from this page - only individual product pages
                                    # NOTE: Since we're on a wheel category page, ALL products are wheels - no filtering needed
                                    
                                    # Try multiple patterns to find ALL product links
                                    pag_links = pag_soup.find_all('a', href=re.compile(r'/oem/acura~'))
                                    
                                    # Also look for links in product cards/sections
                                    product_sections = pag_soup.find_all(['div', 'section', 'article'], class_=re.compile(r'product|item|card', re.I))
                                    for section in product_sections:
                                        section_links = section.find_all('a', href=re.compile(r'/oem/acura~'))
                                        for link in section_links:
                                            if link not in pag_links:
                                                pag_links.append(link)
                                    
                                    # Also look for links containing part numbers
                                    part_number_links = pag_soup.find_all('a', href=re.compile(r'[A-Z0-9]{5,}-[A-Z0-9]{1,}-[A-Z0-9]{1,}'))
                                    for link in part_number_links:
                                        href = link.get('href', '')
                                        if href and '/oem/acura~' in href and link not in pag_links:
                                            pag_links.append(link)
                                    
                                    if len(pag_links) > 0:
                                        page_found = True
                                        page_product_count = 0
                                        
                                        self.logger.info(f"Page {page_num}: Found {len(pag_links)} product links")
                                        
                                        for link in pag_links:
                                            href = link.get('href', '')
                                            if href:
                                                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                                
                                                # Remove fragment and query params
                                                if '#' in full_url:
                                                    full_url = full_url.split('#')[0]
                                                if '?' in full_url:
                                                    # Keep only base URL, remove query params
                                                    full_url = full_url.split('?')[0]
                                                
                                                full_url = full_url.rstrip('/')
                                                
                                                # On wheel category pages, ALL products are wheels - collect all of them
                                                # No need to filter - we're already on a wheel-specific category page
                                                # Only collect if it's an individual product page (not a category listing page)
                                                if '/oem/acura~' in full_url and full_url not in product_urls:
                                                    product_urls.append(full_url)
                                                    page_product_count += 1
                                        
                                        self.logger.info(f"Page {page_num}: Collected {page_product_count} new unique product URLs (Total: {len(product_urls)})")
                                        
                                        if page_found:
                                            break  # Found working pagination pattern
                                            
                                except Exception as e:
                                    self.logger.debug(f"Error loading page {page_num} with pattern {pattern}: {str(e)}")
                                    continue
                            
                            if not page_found:
                                self.logger.warning(f"Could not load page {page_num} with any pagination pattern")
                            
                            # Delay between pages
                            if page_num < total_pages:
                                time.sleep(random.uniform(1, 2))
                    
                    time.sleep(random.uniform(1, 2))  # Delay between category pages
                    
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
    
    def _handle_vehicle_selection_modal(self):
        """
        Handle the vehicle selection modal that appears on search
        Tries to close the modal or select a default vehicle
        """
        try:
            # Wait a moment for modal to appear
            time.sleep(2)
            
            # Try multiple strategies to handle the modal
            # Strategy 1: Try to close the modal with X button
            close_selectors = [
                ".v-vin-confirm-close",  # Close button in modal
                ".ab-modal-times",  # Generic modal close button
                ".v-cm-close",  # Vehicle selection modal close
                "button[aria-label*='close']",
                "button[aria-label*='Close']",
                ".modal-close",
                "button.close",
            ]
            
            for selector in close_selectors:
                try:
                    close_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if close_btn.is_displayed():
                        close_btn.click()
                        self.logger.info(f"Closed vehicle selection modal using selector: {selector}")
                        time.sleep(1)
                        return True
                except:
                    continue
            
            # Strategy 2: Try clicking outside the modal (on overlay/backdrop)
            try:
                overlay = self.driver.find_element(By.CSS_SELECTOR, ".ab-mask, .ab-modal, .modal-backdrop")
                if overlay.is_displayed():
                    # Click on overlay to close modal
                    ActionChains(self.driver).move_to_element(overlay).click().perform()
                    self.logger.info("Closed modal by clicking overlay")
                    time.sleep(1)
                    return True
            except:
                pass
            
            # Strategy 3: Press Escape key
            try:
                from selenium.webdriver.common.keys import Keys
                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                self.logger.info("Pressed Escape key to close modal")
                time.sleep(1)
                return True
            except:
                pass
            
            # Strategy 4: Select a default vehicle (if modal doesn't close)
            # Look for vehicle selection dropdowns and select first available
            try:
                # Try to select a model dropdown
                model_selectors = [
                    ".v-base-model-content .av-ipt",
                    ".av-ipt",
                    "select[name*='model']",
                    ".ab-select-input-control",
                ]
                
                for selector in model_selectors:
                    try:
                        model_dropdown = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if model_dropdown.is_displayed():
                            # Click to open dropdown
                            model_dropdown.click()
                            time.sleep(0.5)
                            
                            # Try to select first available model
                            first_option = self.driver.find_element(By.CSS_SELECTOR, ".ab-select-col > li:first-child, .av-ipt-drop li:first-child")
                            if first_option:
                                first_option.click()
                                time.sleep(0.5)
                                
                                # Try to select year
                                year_dropdown = self.driver.find_element(By.CSS_SELECTOR, ".v-base-model-content .av-ipt:last-child, select[name*='year']")
                                if year_dropdown:
                                    year_dropdown.click()
                                    time.sleep(0.5)
                                    first_year = self.driver.find_element(By.CSS_SELECTOR, ".ab-select-col > li:first-child, option:not([value=''])")
                                    if first_year:
                                        first_year.click()
                                        time.sleep(0.5)
                                
                                # Click Go button
                                go_btn = self.driver.find_element(By.CSS_SELECTOR, ".av-btn-red, button[type='submit'], .v-base-model-content .av-btn")
                                if go_btn:
                                    go_btn.click()
                                    self.logger.info("Selected default vehicle and clicked Go")
                                    time.sleep(2)
                                    return True
                    except:
                        continue
            except:
                pass
            
            # If modal still appears, try JavaScript to close it
            try:
                self.driver.execute_script("""
                    // Try to close modal with JavaScript
                    var modals = document.querySelectorAll('.ab-modal, .v-vin-confirm-confirm, .v-cm-modal');
                    for (var i = 0; i < modals.length; i++) {
                        var modal = modals[i];
                        if (modal.style.display !== 'none') {
                            var closeBtn = modal.querySelector('.ab-modal-times, .v-vin-confirm-close, .v-cm-close');
                            if (closeBtn) closeBtn.click();
                            else {
                                // Hide modal directly
                                modal.style.display = 'none';
                                if (modal.parentElement) {
                                    modal.parentElement.style.display = 'none';
                                }
                            }
                        }
                    }
                    
                    // Also try to remove overlay
                    var overlays = document.querySelectorAll('.ab-mask, .modal-backdrop');
                    for (var i = 0; i < overlays.length; i++) {
                        overlays[i].style.display = 'none';
                    }
                """)
                self.logger.info("Attempted to close modal using JavaScript")
                time.sleep(1)
                return True
            except:
                pass
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Error handling vehicle selection modal: {str(e)}")
            return False
    
    def _search_for_wheels(self):
        """
        Search for wheels using site search - handles vehicle selection modal and pagination
        Filters for wheel products early based on URL and link text
        
        NOTE: This site requires vehicle selection before search. This method tries to:
        1. Close the vehicle selection modal
        2. Or select a default vehicle
        3. Or use direct category URLs that don't require vehicle selection
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # Since search requires vehicle selection, try alternative approaches first:
            # 1. Use direct category/listing URLs that don't require vehicle selection
            self.logger.info("Trying direct wheel category URLs (bypassing search)...")
            direct_category_urls = [
                f"{self.base_url}/oem-acura-spare_wheel.html",
                f"{self.base_url}/oem-acura-rims.html",
                f"{self.base_url}/oem-acura-wheel_cover.html",
                f"{self.base_url}/oem-acura-alloy_wheel.html",
                f"{self.base_url}/oem-acura-steel_wheel.html",
                f"{self.base_url}/category/acura-chassis.html",  # Wheels are often in chassis category
                f"{self.base_url}/accessories/acura-alloy_wheels.html",
            ]
            
            for category_url in direct_category_urls:
                try:
                    self.logger.info(f"Trying direct category URL: {category_url}")
                    html = self.get_page(category_url, use_selenium=True, wait_time=2)
                    if html and len(html) > 5000:  # Valid page content
                        soup = BeautifulSoup(html, 'lxml')
                        product_links = soup.find_all('a', href=re.compile(r'/oem-acura-|/oem/acura~|/parts-list/.*wheels|/accessories/acura-'))
                        
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
                        
                        if product_links:
                            self.logger.info(f"Found {len(product_links)} product links from {category_url}")
                except Exception as e:
                    self.logger.debug(f"Error trying direct category URL {category_url}: {str(e)}")
                    continue
            
            # 2. Now try search with modal handling
            if len(product_urls) < 50:  # Only try search if we didn't find many products
                self.logger.info("Trying search method (will handle vehicle selection modal)...")
                
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
                        
                        # Handle vehicle selection modal if it appears
                        self._handle_vehicle_selection_modal()
                        
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
                    
                    # Wait for search results to load (after modal is handled)
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-acura-'], a[href*='/oem/acura~']"))
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
                    
                    # Scroll to load all dynamic content (fitments, specifications, etc.)
                    try:
                        self.logger.info("Scrolling to load all dynamic content...")
                        last_height = self.driver.execute_script("return document.body.scrollHeight")
                        scroll_attempts = 0
                        max_scrolls = 30
                        no_change_count = 0
                        
                        while scroll_attempts < max_scrolls:
                            # Scroll down
                            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(0.5)
                            
                            # Scroll back up a bit (human-like behavior)
                            self.driver.execute_script("window.scrollBy(0, -200);")
                            time.sleep(0.3)
                            
                            # Scroll down again
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
                        
                        # Scroll to top to ensure all content is accessible
                        self.driver.execute_script("window.scrollTo(0, 0);")
                        time.sleep(0.5)
                        
                        # Scroll to middle to trigger any lazy-loaded content
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                        time.sleep(0.5)
                        
                        # Scroll to bottom again
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)
                        
                        self.logger.info("Finished scrolling, content should be fully loaded")
                    except Exception as scroll_error:
                        self.logger.debug(f"Error during scrolling: {str(scroll_error)}")
                    
                    # Wait for product title and key elements to load using WebDriverWait
                    try:
                        # Wait for product title
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h1, h2, .product-title, [data-product-title]"))
                        )
                        
                        # Wait a bit more for fitment data to load (if dynamically loaded)
                        time.sleep(1)
                        
                        # Try to wait for fitment-related elements
                        try:
                            WebDriverWait(self.driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "script[type='application/json'], ul, ol, table, .fitment, [class*='fitment']"))
                            )
                        except:
                            pass  # Fitment elements might not be present, continue anyway
                            
                    except Exception as wait_error:
                        self.logger.debug(f"Timeout waiting for elements: {str(wait_error)}")
                    
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
            
            # Extract image URL with error handling - try to get highest quality image
            try:
                img_elem = soup.find('img', class_='product-main-image')
                if not img_elem:
                    # Try alternative selectors
                    img_elem = (soup.find('img', class_='product-image') or 
                               soup.find('img', id='product-image') or
                               soup.find('img', class_=re.compile(r'product.*img|main.*image', re.I)))
                
                # Also try og:image meta tag
                if not img_elem:
                    og_image = soup.find('meta', property='og:image')
                    if og_image:
                        img_url = og_image.get('content', '')
                        if img_url:
                            product_data['image_url'] = img_url if img_url.startswith('http') else f"{self.base_url}{img_url}"
                
                if img_elem:
                    # Try multiple attributes for image URL (prefer higher quality)
                    img_url = (img_elem.get('data-src-large') or 
                              img_elem.get('data-src') or 
                              img_elem.get('data-lazy-src') or
                              img_elem.get('src'))
                    
                    if img_url:
                        # Normalize image URL
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        elif not img_url.startswith('http'):
                            product_data['image_url'] = f"{self.base_url}/{img_url}"
                        else:
                            product_data['image_url'] = img_url
            except Exception as e:
                self.logger.debug(f"Error extracting image URL: {str(e)}")
            
            # Extract description with error handling - try multiple methods
            try:
                desc_elem = soup.find('span', class_='description_body')
                if not desc_elem:
                    # Try alternative selectors
                    desc_elem = (soup.find('div', class_='description') or 
                                soup.find('p', class_='product-description') or
                                soup.find('div', class_=re.compile(r'description|product.*desc', re.I)))
                
                # Also try meta description
                if not desc_elem:
                    meta_desc = soup.find('meta', {'name': 'description'})
                    if meta_desc:
                        desc_content = meta_desc.get('content', '')
                        if desc_content and len(desc_content) > 20:  # Only use if substantial
                            product_data['description'] = desc_content.strip()
                
                if desc_elem and not product_data['description']:
                    product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
                
                # Also try to get full product specifications as description
                if not product_data['description'] or len(product_data['description']) < 20:
                    spec_section = soup.find('div', class_=re.compile(r'product.*spec|specification', re.I))
                    if spec_section:
                        spec_text = spec_section.get_text(strip=True, separator=' ')
                        if spec_text and len(spec_text) > 20:
                            product_data['description'] = spec_text
            except Exception as e:
                self.logger.debug(f"Error extracting description: {str(e)}")
            
            # Extract also_known_as (Other Names) with error handling
            try:
                also_known_elem = soup.find('li', class_='also_known_as')
                if not also_known_elem:
                    # Try alternative selectors
                    also_known_elem = soup.find('div', class_=re.compile(r'also.*known|other.*name', re.I))
                
                # Also look for "Other Name:" text in product specifications
                if not also_known_elem:
                    # Look for text containing "Other Name:" and extract the value after it
                    all_text_elements = soup.find_all(string=re.compile(r'Other Name:', re.I))
                    for text_elem in all_text_elements:
                        parent = text_elem.parent
                        if parent:
                            # Get the text after "Other Name:"
                            full_text = parent.get_text(strip=True)
                            if 'Other Name:' in full_text:
                                # Extract value after "Other Name:"
                                match = re.search(r'Other Name:\s*(.+)', full_text, re.I)
                                if match:
                                    product_data['also_known_as'] = match.group(1).strip()
                                    self.logger.info(f"📝 Found also_known_as via text search: {product_data['also_known_as']}")
                                    break
                
                if also_known_elem and not product_data['also_known_as']:
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
            
            # Extract "Replaces" field (if present)
            try:
                replaces_elem = soup.find('li', class_='replaces')
                if not replaces_elem:
                    # Look for text containing "Replaces:" and extract the value after it
                    all_text_elements = soup.find_all(string=re.compile(r'Replaces:', re.I))
                    for text_elem in all_text_elements:
                        parent = text_elem.parent
                        if parent:
                            full_text = parent.get_text(strip=True)
                            if 'Replaces:' in full_text:
                                match = re.search(r'Replaces:\s*(.+)', full_text, re.I)
                                if match:
                                    product_data['replaces'] = match.group(1).strip()
                                    self.logger.info(f"📝 Found replaces: {product_data['replaces']}")
                                    break
                
                if replaces_elem and not product_data['replaces']:
                    value_elem = replaces_elem.find('h2', class_='list-value') or replaces_elem.find('span', class_='value')
                    if value_elem:
                        product_data['replaces'] = value_elem.get_text(strip=True)
                    else:
                        product_data['replaces'] = replaces_elem.get_text(strip=True)
            except Exception as e:
                self.logger.debug(f"Error extracting replaces: {str(e)}")
            
            # Extract "Applications" field (if present) - often contains fitment summary
            try:
                applications_elem = soup.find('li', class_='applications')
                if not applications_elem:
                    applications_elem = soup.find('div', class_=re.compile(r'application', re.I))
                if applications_elem:
                    value_elem = applications_elem.find('h2', class_='list-value') or applications_elem.find('span', class_='value')
                    if value_elem:
                        product_data['applications'] = value_elem.get_text(strip=True)
                    else:
                        product_data['applications'] = applications_elem.get_text(strip=True)
            except Exception as e:
                self.logger.debug(f"Error extracting applications: {str(e)}")
            
            # Extract fitment data - try multiple methods to get ALL fitments
            # Method 1: Extract from JSON script tags
            try:
                script_elem = soup.find('script', id='product_data')
                if not script_elem:
                    # Try alternative script tags
                    script_tags = soup.find_all('script', type='application/json')
                    for tag in script_tags:
                        if tag.string and 'fitment' in tag.string.lower():
                            script_elem = tag
                            break
                
                # Also try all script tags for JSON-LD or other JSON data
                if not script_elem:
                    all_scripts = soup.find_all('script')
                    for script in all_scripts:
                        if script.string:
                            try:
                                data = json.loads(script.string)
                                if isinstance(data, dict) and 'fitment' in data:
                                    script_elem = script
                                    break
                            except:
                                continue
                
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
                        
                        self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from JSON")
                        
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Error parsing JSON: {str(e)}")
                    except Exception as e:
                        self.logger.warning(f"Error extracting fitments from JSON: {str(e)}")
            except Exception as e:
                self.logger.debug(f"Error finding fitment script: {str(e)}")
            
            # Method 2: Extract from HTML "Fits the following Acura Models:" list
            if not product_data['fitments']:
                try:
                    # Look for "Fits the following" text
                    fits_text = soup.find(string=re.compile(r'Fits the following.*Models?', re.I))
                    if fits_text:
                        # Find the parent container
                        parent = fits_text.find_parent()
                        if parent:
                            # Look for list items (li) or list (ul/ol) containing model information
                            # Pattern: "TLX 2018-2020" or "MDX 2019-2020"
                            fitment_list = parent.find_next(['ul', 'ol', 'div'])
                            if fitment_list:
                                # Find all list items or divs containing model info
                                items = fitment_list.find_all(['li', 'div', 'p'])
                                
                                for item in items:
                                    item_text = item.get_text(strip=True)
                                    if item_text and len(item_text) > 3:
                                        # Parse pattern like "TLX 2018-2020" or "MDX 2019-2020"
                                        # Also handle "MDX 2022-2024" format
                                        match = re.search(r'([A-Z]+(?:\s+[A-Z]+)?)\s+(\d{4})(?:\s*-\s*(\d{4}))?', item_text)
                                        if match:
                                            model_name = match.group(1).strip()
                                            start_year = match.group(2)
                                            end_year = match.group(3) if match.group(3) else start_year
                                            
                                            # Create fitment for each year in range
                                            try:
                                                start = int(start_year)
                                                end = int(end_year)
                                                for year in range(start, end + 1):
                                                    product_data['fitments'].append({
                                                        'year': str(year),
                                                        'make': 'Acura',
                                                        'model': model_name,
                                                        'trim': '',
                                                        'engine': ''
                                                    })
                                            except:
                                                # If range parsing fails, just use start year
                                                product_data['fitments'].append({
                                                    'year': start_year,
                                                    'make': 'Acura',
                                                    'model': model_name,
                                                    'trim': '',
                                                    'engine': ''
                                                })
                                
                                if product_data['fitments']:
                                    self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from HTML list")
                except Exception as e:
                    self.logger.debug(f"Error extracting fitments from HTML: {str(e)}")
            
            # Method 3: Extract from fitment table if present
            if not product_data['fitments']:
                try:
                    fitment_table = soup.find('table', class_=re.compile(r'fitment|compatibility', re.I))
                    if not fitment_table:
                        # Look for any table with vehicle information
                        tables = soup.find_all('table')
                        for table in tables:
                            headers = table.find_all(['th', 'td'])
                            header_text = ' '.join([h.get_text(strip=True) for h in headers[:5]]).lower()
                            if any(word in header_text for word in ['year', 'make', 'model', 'trim', 'engine']):
                                fitment_table = table
                                break
                    
                    if fitment_table:
                        rows = fitment_table.find_all('tr')[1:]  # Skip header row
                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if len(cols) >= 3:
                                year = cols[0].get_text(strip=True) if len(cols) > 0 else ''
                                make = cols[1].get_text(strip=True) if len(cols) > 1 else 'Acura'
                                model = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                                trim = cols[3].get_text(strip=True) if len(cols) > 3 else ''
                                engine = cols[4].get_text(strip=True) if len(cols) > 4 else ''
                                
                                if year and model:
                                    product_data['fitments'].append({
                                        'year': year,
                                        'make': make if make else 'Acura',
                                        'model': model,
                                        'trim': trim,
                                        'engine': engine
                                    })
                        
                        if product_data['fitments']:
                            self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from table")
                except Exception as e:
                    self.logger.debug(f"Error extracting fitments from table: {str(e)}")
            
            # Method 4: Extract from product specifications section
            if not product_data['fitments']:
                try:
                    # Look for product specifications section
                    spec_section = soup.find('div', class_=re.compile(r'product.*spec|specification', re.I))
                    if not spec_section:
                        spec_section = soup.find('section', class_=re.compile(r'spec', re.I))
                    
                    if spec_section:
                        # Look for fitment information in specifications
                        spec_text = spec_section.get_text()
                        # Pattern: "Fits: 2018-2020 Acura TLX"
                        fits_match = re.search(r'Fits?:\s*(\d{4}(?:-\d{4})?)\s+([A-Za-z]+)\s+([A-Z]+)', spec_text, re.I)
                        if fits_match:
                            year_range = fits_match.group(1)
                            make = fits_match.group(2)
                            model = fits_match.group(3)
                            
                            # Parse year range
                            if '-' in year_range:
                                start_year, end_year = year_range.split('-')
                                for year in range(int(start_year), int(end_year) + 1):
                                    product_data['fitments'].append({
                                        'year': str(year),
                                        'make': make,
                                        'model': model,
                                        'trim': '',
                                        'engine': ''
                                    })
                            else:
                                product_data['fitments'].append({
                                    'year': year_range,
                                    'make': make,
                                    'model': model,
                                    'trim': '',
                                    'engine': ''
                                })
                            
                            if product_data['fitments']:
                                self.logger.info(f"✅ Extracted {len(product_data['fitments'])} fitment combinations from specifications")
                except Exception as e:
                    self.logger.debug(f"Error extracting fitments from specifications: {str(e)}")
            
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
            else:
                self.logger.info(f"✅ Total fitments extracted: {len(product_data['fitments'])}")
            
            self.logger.info(f"✅ Successfully scraped: {product_data['title']}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"❌ Error scraping product {url}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
