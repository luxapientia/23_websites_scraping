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
            
            # CRITICAL VALIDATION: Filter out listing/category page URLs
            # Only keep individual product URLs matching pattern: /oem/acura~...~...html
            # Exclude listing pages like:
            #   - /oem-acura-wheel_cover.html (category listing)
            #   - /accessories/acura-mdx-alloy_wheels.html (accessory listing)
            #   - /parts-list/2024-acura-mdx/wheels.html (model listing)
            self.logger.info("Validating URLs - filtering out listing/category pages...")
            validated_urls = []
            filtered_out = []
            
            for url in product_urls:
                # Individual product URLs MUST contain '/oem/acura~' pattern
                # This pattern indicates a specific product, not a listing page
                if '/oem/acura~' in url:
                    validated_urls.append(url)
                else:
                    # This is likely a listing/category page, not an individual product
                    filtered_out.append(url)
                    self.logger.debug(f"Filtered out listing page: {url}")
            
            if filtered_out:
                self.logger.warning(f"Filtered out {len(filtered_out)} listing/category page URLs that were incorrectly collected:")
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
                        
                        consecutive_no_new = 0
                        
                        for page_num in range(2, total_pages + 1):
                            if consecutive_no_new >= 3:
                                self.logger.info(f"Stopping pagination: {consecutive_no_new} consecutive pages with no new products")
                                break
                                
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
                                        
                                        if page_product_count == 0:
                                            consecutive_no_new += 1
                                        else:
                                            consecutive_no_new = 0
                                        
                                        if page_found:
                                            break  # Found working pagination pattern
                                            
                                except Exception as e:
                                    self.logger.debug(f"Error loading page {page_num} with pattern {pattern}: {str(e)}")
                                    continue
                            
                            if not page_found:
                                self.logger.warning(f"Could not load page {page_num} with any pagination pattern")
                                consecutive_no_new += 1 # Count as no new products if page failed
                            
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
        Scrape single product from AcuraPartsWarehouse with refined extraction logic
        Returns a LIST of dictionaries (one for each fitment/trim combination)
        """
        import random
        import json
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        
        # Import TimeoutException - try to import it
        try:
            from selenium.common.exceptions import TimeoutException
        except ImportError:
            TimeoutException = type('TimeoutException', (Exception,), {})
        
        max_retries = 3
        retry_count = 0
        html = None
        
        while retry_count < max_retries:
            try:
                if not self.check_health():
                    self.logger.error("Scraper health check failed, stopping")
                    return []
                
                self.logger.info(f"Loading product page (attempt {retry_count + 1}/{max_retries}): {url}")
                
                try:
                    self.ensure_driver()
                except Exception as driver_error:
                    recovery = self.error_handler.handle_error(driver_error, retry_count)
                    if recovery['should_retry'] and retry_count < max_retries - 1:
                        time.sleep(random.uniform(recovery['wait_time'][0], recovery['wait_time'][1]))
                        retry_count += 1
                        continue
                    else:
                        return []
                
                try:
                    self.driver.get(url)
                    time.sleep(random.uniform(1.0, 2.0))
                    
                    # Cloudflare check
                    if ('challenges.cloudflare.com' in self.driver.current_url.lower() or len(self.driver.page_source) < 5000) and self.has_cloudflare_challenge():
                        self.logger.info("🛡️ Cloudflare challenge detected...")
                        if not self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1):
                            if len(self.driver.page_source) <= 5000:
                                retry_count += 1
                                time.sleep(random.uniform(10, 15))
                                continue
                        time.sleep(1)
                    
                    self.simulate_human_behavior()
                    
                    # Handle "View More" for fitment table specifically
                    try:
                        # Look for the specific "View More" button for the fitment table
                        view_more_btn = None
                        try:
                            # Try specific class first
                            view_more_btn = self.driver.find_element(By.CSS_SELECTOR, ".fit-vehicle-list-view-text")
                        except:
                            # Try generic text search if specific class fails
                            xpath_selectors = [
                                "//div[contains(@class, 'fit-vehicle-list-view-text')]",
                                "//div[contains(text(), 'View More') and contains(@class, 'fit-vehicle-list')]",
                                "//span[contains(text(), 'View More')]"
                            ]
                            for xpath in xpath_selectors:
                                try:
                                    btns = self.driver.find_elements(By.XPATH, xpath)
                                    for btn in btns:
                                        if btn.is_displayed():
                                            view_more_btn = btn
                                            break
                                    if view_more_btn: break
                                except: continue
                        
                        if view_more_btn and view_more_btn.is_displayed():
                            self.logger.info("Found 'View More' button for fitment, clicking...")
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_more_btn)
                            time.sleep(0.5)
                            self.driver.execute_script("arguments[0].click();", view_more_btn)
                            time.sleep(1.5)
                    except Exception as e:
                        self.logger.debug(f"Note: Could not click 'View More' (might not exist): {e}")
                    
                    html = self.driver.page_source
                    if not html or len(html) < 1000: raise Exception("Page content too small")
                    break
                    
                except TimeoutException:
                    retry_count += 1
                    continue
                except Exception as e:
                    self.logger.warning(f"⚠️ Error loading page: {e}")
                    retry_count += 1
                    time.sleep(random.uniform(5, 10))
                    continue
            except Exception as e:
                self.logger.error(f"❌ Critical error: {e}")
                return []

        if not html: return []
        
        soup = BeautifulSoup(html, 'lxml')
        
        base_data = {
            'url': url, 'image_url': '', 'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sku': '', 'pn': '', 'actual_price': '', 'msrp': '', 'title': '',
            'also_known_as': '', 'positions': '', 'description': '', 'applications': '', 'replaces': ''
        }
        
        try:
            # 1. Title
            title_elem = soup.find('h1', class_='product-title') or soup.find('h1', class_='pn-detail-h1') or soup.find('h1')
            if title_elem:
                base_data['title'] = title_elem.get_text(strip=True)
            else:
                title_tag = soup.find('title')
                if title_tag:
                    base_data['title'] = title_tag.get_text(strip=True).split('|')[0].strip()

            if not base_data['title']: return []

            # 2. SKU/PN (Pattern: XXXXX-XXX-XXX)
            # Priority 1: Meta tags / JSON-LD
            try:
                # Check JSON-LD
                json_ld = soup.find('script', type='application/ld+json')
                if json_ld:
                    data = json.loads(json_ld.string)
                    if isinstance(data, list): data = data[0]
                    if 'sku' in data: base_data['sku'] = data['sku']
                    if 'mpn' in data: base_data['sku'] = data['mpn']
                
                # Check meta tags
                if not base_data['sku']:
                    meta_sku = soup.find('meta', itemprop='sku')
                    if meta_sku: base_data['sku'] = meta_sku.get('content')
            except: pass

            # Priority 2: Specific HTML element
            if not base_data['sku']:
                pn_div = soup.find('div', class_='acc-pn-detail-sub-title')
                if pn_div:
                    strong = pn_div.find('strong')
                    if strong: base_data['sku'] = strong.get_text(strip=True)

            # Priority 3: HTML (Parts Page)
            if not base_data['sku']:
                sub_desc = soup.find('p', class_='pn-detail-sub-desc')
                if sub_desc:
                    text = sub_desc.get_text(strip=True)
                    parts = text.split()
                    if parts:
                        potential_pn = parts[-1]
                        if any(c.isdigit() for c in potential_pn):
                            base_data['sku'] = potential_pn

            # Priority 4: Regex from Title/URL
            if not base_data['sku']:
                pattern = r'\b([A-Z0-9]{3,5}-[A-Z0-9]{2,3}-[A-Z0-9]{2,3})\b'
                match = re.search(pattern, base_data['title'], re.I)
                if match: base_data['sku'] = match.group(1).upper()
                else:
                    match = re.search(pattern, url.upper())
                    if match: base_data['sku'] = match.group(1)

            if base_data['sku']:
                base_data['sku'] = base_data['sku'].upper()
                # Clean any existing non-alphanumeric chars if it's not a standard format
                clean_sku = re.sub(r'[^A-Z0-9]', '', base_data['sku'])
                base_data['pn'] = clean_sku
                
                # Format SKU as XXXXX-XXX-XXX if it's 11 chars and doesn't have hyphens
                if len(clean_sku) == 11 and '-' not in base_data['sku']:
                    base_data['sku'] = f"{clean_sku[:5]}-{clean_sku[5:8]}-{clean_sku[8:]}"

            # 3. Image URL
            # Priority 1: JSON-LD
            try:
                if not base_data['image_url']:
                    json_ld = soup.find('script', type='application/ld+json')
                    if json_ld:
                        data = json.loads(json_ld.string)
                        if isinstance(data, list): data = data[0]
                        if 'image' in data:
                            imgs = data['image']
                            if isinstance(imgs, list) and imgs: base_data['image_url'] = imgs[0]
                            elif isinstance(imgs, str): base_data['image_url'] = imgs
            except: pass

            # Priority 2: Specific img tag
            if not base_data['image_url']:
                # Accessory Page
                img_tag = soup.find('img', class_='pn-img-img')
                if not img_tag:
                    # Parts Page
                    img_div = soup.find('div', class_='pn-detail-img-area')
                    if img_div:
                        img_tag = img_div.find('img')
                
                if not img_tag:
                    # Fallback to src pattern
                    img_tag = soup.find('img', src=re.compile(r'/resources/.*accessory-image/'))
                    if not img_tag:
                        img_tag = soup.find('img', src=re.compile(r'/resources/.*part-picture/'))
                
                if img_tag:
                    base_data['image_url'] = img_tag.get('src')

            # Normalize URL
            if base_data['image_url']:
                if base_data['image_url'].startswith('//'): 
                    base_data['image_url'] = 'https:' + base_data['image_url']
                elif base_data['image_url'].startswith('/'): 
                    base_data['image_url'] = self.base_url + base_data['image_url']

            # 4. MSRP & Price
            # MSRP
            msrp_span = soup.find('span', class_='price-section-retail')
            if msrp_span:
                inner_span = msrp_span.find('span')
                if inner_span:
                    base_data['msrp'] = self.extract_price(inner_span.get_text(strip=True))
                else:
                    base_data['msrp'] = self.extract_price(msrp_span.get_text(strip=True))
            
            # Actual Price
            price_span = soup.find('span', class_='price-section-price')
            if price_span:
                base_data['actual_price'] = self.extract_price(price_span.get_text(strip=True))

            # 5. Description
            # Priority 1: Marketing description div (Accessory)
            desc_div = soup.find('div', class_=lambda c: c and 'acc-pn-detail-marketing' in c)
            if desc_div:
                ul = desc_div.find('ul')
                if ul:
                    items = [li.get_text(strip=True) for li in ul.find_all('li')]
                    base_data['description'] = ' | '.join(items)
            
            # Priority 2: Parts Page Description List
            if not base_data['description']:
                detail_list = soup.find('ul', class_='pn-detail-list')
                if detail_list:
                    for li in detail_list.find_all('li'):
                        span = li.find('span')
                        if span and 'Part Description' in span.get_text(strip=True):
                            div = li.find('div')
                            if div:
                                base_data['description'] = div.get_text(strip=True)
                            break
                
            # Priority 3: Meta description
            if not base_data['description']:
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc: base_data['description'] = meta_desc.get('content', '')

            # Priority 4: Product Specifications
            if not base_data['description']:
                spec_div = soup.find('div', class_=re.compile(r'product.*spec', re.I))
                if spec_div: base_data['description'] = spec_div.get_text(strip=True, separator=' ')

            # 6. Fitment Data extraction
            fitment_rows = []
            
            # Strategy 1: Parse the fitment table
            fitment_table = soup.find('div', class_='fit-vehicle-list')
            if not fitment_table:
                fitment_table = soup.find('div', class_='fit-vehicle-list-table')
            
            if fitment_table:
                table = fitment_table.find('table', class_='fit-vehicle-list-table')
                if table:
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_row = thead.find('tr')
                        if header_row:
                            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                    
                    col_map = {}
                    combined_ymm_col = -1
                    
                    for i, h in enumerate(headers):
                        # Check for combined "Year Make Model" column
                        if 'year' in h and 'make' in h and 'model' in h:
                            combined_ymm_col = i
                            continue
                            
                        if 'year' in h: col_map['year'] = i
                        elif 'make' in h: col_map['make'] = i
                        elif 'model' in h: col_map['model'] = i
                        elif 'trim' in h or 'body' in h: col_map['trim'] = i
                        elif 'engine' in h: col_map['engine'] = i
                    
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if not cols: continue
                            
                            row_data = {
                                'year': '', 'make': 'Acura', 'model': '', 'trim': '', 'engine': ''
                            }
                            
                            # Handle combined Year Make Model column
                            if combined_ymm_col != -1 and combined_ymm_col < len(cols):
                                first_col_text = cols[combined_ymm_col].get_text(strip=True)
                                # Try to parse "2024 Acura MDX"
                                parts = first_col_text.split()
                                if len(parts) >= 3 and parts[0].isdigit():
                                    row_data['year'] = parts[0]
                                    row_data['make'] = parts[1]
                                    row_data['model'] = ' '.join(parts[2:])
                            
                            # Handle standard columns
                            for key, idx in col_map.items():
                                if idx < len(cols):
                                    value = cols[idx].get_text(strip=True)
                                    if value and value.endswith('.'):
                                        value = value[:-1].strip()
                                    row_data[key] = value
                            
                            # Fallback for combined column if not explicitly detected in header but structure implies it
                            if not row_data['year'] and not row_data['model'] and len(cols) > 0 and 'year' not in col_map:
                                first_col_text = cols[0].get_text(strip=True)
                                parts = first_col_text.split()
                                if len(parts) >= 3 and parts[0].isdigit():
                                    row_data['year'] = parts[0]
                                    row_data['make'] = parts[1]
                                    row_data['model'] = ' '.join(parts[2:])
                            
                            if not row_data['make']: row_data['make'] = 'Acura'
                            
                            if not row_data['year'] or not row_data['model']:
                                continue
                            
                            if ',' in row_data['trim']:
                                trims = [t.strip() for t in row_data['trim'].split(',') if t.strip()]
                                for trim in trims:
                                    new_row = row_data.copy()
                                    new_row['trim'] = trim
                                    fitment_rows.append(new_row)
                            else:
                                fitment_rows.append(row_data)

            # Strategy 2: Fallback to JSON data
            if not fitment_rows:
                try:
                    scripts = soup.find_all('script', type='application/json')
                    for script in scripts:
                        if script.string and 'fitment' in script.string.lower():
                            try:
                                data = json.loads(script.string)
                                fitments = []
                                if 'fitment' in data: fitments = data['fitment']
                                elif 'props' in data and 'fitment' in data['props']: fitments = data['props']['fitment']
                                
                                if fitments:
                                    for f in fitments:
                                        year = str(f.get('year', ''))
                                        make = f.get('make', 'Acura')
                                        model = f.get('model', '')
                                        trims = f.get('trims', [])
                                        if isinstance(trims, str): trims = [trims]
                                        if not trims: trims = ['']
                                
                                for trim in trims:
                                            fitment_rows.append({
                                            'year': year,
                                            'make': make,
                                            'model': model,
                                            'trim': trim,
                                                'engine': f.get('engine', '')
                                            })
                            except: continue
                except: pass

            # Strategy 3: Fallback to "This Part Fits" text parsing
            if not fitment_rows:
                try:
                    fits_text_elem = soup.find(string=re.compile(r'Fits\s+\d{4}', re.I))
                    if fits_text_elem:
                        text = fits_text_elem.parent.get_text() if fits_text_elem.parent else fits_text_elem
                        year_match = re.search(r'(\d{4})[-\s]+(\d{4})', text)
                        if year_match:
                            start, end = int(year_match.group(1)), int(year_match.group(2))
                            models_part = text.split('Acura')[-1]
                            models = [m.strip() for m in re.split(r'[,&]', models_part) if m.strip()]
                            
                            for year in range(start, end + 1):
                                for model in models:
                                    fitment_rows.append({
                                        'year': str(year), 'make': 'Acura', 'model': model, 'trim': '', 'engine': ''
                                    })
                except: pass

            # Construct final result list
            final_list = []
            if fitment_rows:
                self.logger.info(f"✅ Extracted {len(fitment_rows)} fitment rows")
                for f in fitment_rows:
                    row = base_data.copy()
                    row.update(f)
                    final_list.append(row)
            else:
                self.logger.warning("⚠️ No fitments found, returning single row")
                row = base_data.copy()
                row.update({'year': '', 'make': 'Acura', 'model': '', 'trim': '', 'engine': ''})
                final_list.append(row)

            return final_list
            
        except Exception as e:
            self.logger.error(f"❌ Error scraping product: {e}")
            return []
