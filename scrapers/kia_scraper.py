"""Scraper for www.kiapartsnow.com (Kia parts) - Auto Parts Prime platform"""
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

class KiaScraper(BaseScraper):
    """Scraper for www.kiapartsnow.com - Uses Auto Parts Prime platform (similar to AcuraPartsWarehouse)"""
    
    def __init__(self):
        super().__init__('kia', use_selenium=True)
        self.base_url = 'https://www.kiapartsnow.com'
        
    def get_product_urls(self):
        """Get all wheel product URLs from www.kiapartsnow.com"""
        product_urls = []
        
        try:
            self.logger.info("Extracting wheel product URLs from wheel cover listing page...")
            search_urls = self._search_for_wheels()
            product_urls.extend(search_urls)
            
            product_urls = list(set(product_urls))
            self.logger.info(f"Total unique URLs found: {len(product_urls)}")
            
            # Filter out category/listing pages - only keep individual product pages
            # Product URLs: /genuine/kia-{name}~{part}.html
            validated_urls = []
            for url in product_urls:
                if '/genuine/kia-' in url and '~' in url and url.endswith('.html'):
                    if not any(pattern in url for pattern in ['/accessories/', '/category/', '/oem-kia-']):
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
        Visit all four wheel category listing pages and extract all product URLs, handling pagination
        URLs:
        1. https://www.kiapartsnow.com/oem-kia-wheel_cover.html (127 wheel covers)
        2. https://www.kiapartsnow.com/oem-kia-spare_wheel.html (370 spare wheels)
        3. https://www.kiapartsnow.com/accessories/kia-wheels.html (7 genuine kia wheels)
        4. https://www.kiapartsnow.com/accessories/kia-spare_wheel_kit.html (21 Genuine Kia Spare Wheel Kits)
        """
        product_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            # STEP 1: Visit parts-list search product pages first
            # Start with specific parts-list URLs
            parts_list_urls = [
                f"{self.base_url}/parts-list/2009-kia-amanti/chassis/wheel_cap.html",
                f"{self.base_url}/parts-list/2006-kia-amanti-new_body_style_produced_after_nov_2006/chassis/wheel_cap.html",
                f"{self.base_url}/parts-list/2006-kia-amanti-new_body_style_produced_before_oct_2006/chassis/wheel_cap.html",
            ]
            
            # Generate additional parts-list URLs dynamically
            generated_urls = self._generate_parts_list_urls()
            parts_list_urls.extend(generated_urls)
            
            self.logger.info(f"STEP 1: Visiting {len(parts_list_urls)} parts-list search product pages ({len(generated_urls)} generated)...")
            for idx, parts_list_url in enumerate(parts_list_urls, 1):
                try:
                    self.logger.info(f"[{idx}/{len(parts_list_urls)}] Visiting parts-list page: {parts_list_url}")
                    parts_list_products = self._extract_products_from_parts_list(parts_list_url, product_urls)
                    self.logger.info(f"[{idx}/{len(parts_list_urls)}] Parts-list page completed: Found {len(parts_list_products)} new products (Total so far: {len(product_urls)})")
                    
                    # Delay between pages
                    if idx < len(parts_list_urls):
                        time.sleep(random.uniform(1, 2))
                except Exception as e:
                    self.logger.error(f"Error processing parts-list page {idx}/{len(parts_list_urls)} ({parts_list_url}): {str(e)}")
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.info(f"All {len(parts_list_urls)} parts-list pages processed. Total unique product URLs found: {len(product_urls)}")
            
            # STEP 2: All four category URLs to visit one by one
            category_urls = [
                f"{self.base_url}/oem-kia-wheel_cover.html",
                f"{self.base_url}/oem-kia-spare_wheel.html",
                f"{self.base_url}/accessories/kia-wheels.html",
                f"{self.base_url}/accessories/kia-spare_wheel_kit.html",
            ]
            
            self.logger.info(f"STEP 2: Starting to visit {len(category_urls)} category pages one by one...")
            
            # Visit each category URL one by one and extract products
            for idx, category_url in enumerate(category_urls, 1):
                try:
                    self.logger.info(f"[{idx}/{len(category_urls)}] Visiting category page: {category_url}")
                    category_products = self._extract_products_from_category(category_url, product_urls)
                    self.logger.info(f"[{idx}/{len(category_urls)}] Category completed: Found {len(category_products)} new products (Total so far: {len(product_urls)})")
                    
                    # Delay between categories
                    if idx < len(category_urls):
                        time.sleep(random.uniform(1, 2))
                except Exception as e:
                    self.logger.error(f"Error processing category {idx}/{len(category_urls)} ({category_url}): {str(e)}")
                    import traceback
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.info(f"All {len(category_urls)} category pages processed. Total unique product URLs found: {len(product_urls)}")
            
            # Additional discovery method - try to find all product URLs from sitemap or category index
            self.logger.info("Attempting additional product URL discovery...")
            additional_urls = self._discover_additional_product_urls(product_urls)
            if additional_urls:
                product_urls.extend(additional_urls)
                self.logger.info(f"Found {len(additional_urls)} additional product URLs via discovery method")
            
        except Exception as e:
            self.logger.error(f"Error searching for wheels: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return product_urls
    
    def _extract_products_from_category(self, category_url, existing_urls):
        """
        Extract all product URLs from a single category page, handling pagination
        Returns list of new product URLs found (not in existing_urls)
        """
        new_urls = []
        
        try:
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(category_url, use_selenium=True, wait_time=2)
                if not html:
                    self.logger.warning(f"Failed to fetch category page: {category_url}")
                    return new_urls
            except Exception as e:
                self.logger.warning(f"Error loading category page {category_url}: {str(e)}")
                return new_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links to appear
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/genuine/kia-']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # IMPROVED: More thorough scrolling to load ALL lazy-loaded content
            self.logger.info("Scrolling to load all products...")
            for scroll_round in range(5):  # Scroll multiple times
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                # Scroll back up a bit
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                time.sleep(0.5)
                # Scroll to bottom again
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
            
            # Final scroll using existing method
            self._scroll_to_load_content()
            time.sleep(3)  # Additional wait for all content to load
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract total page count from "Page 1 of X" pattern
            total_pages = 1
            try:
                # Look for "Page X of Y" pattern
                page_info = soup.find(string=re.compile(r'Page\s+\d+\s+of\s+\d+', re.I))
                if page_info:
                    page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', str(page_info), re.I)
                    if page_match:
                        total_pages = int(page_match.group(1))
                        self.logger.info(f"Found pagination: {total_pages} total pages for {category_url}")
            except Exception as e:
                self.logger.debug(f"Could not determine total pages: {str(e)}, defaulting to 1")
            
            # Extract products from all pages
            consecutive_zero_count = 0  # Track consecutive pages with zero new products
            for page_num in range(1, total_pages + 1):
                try:
                    if page_num > 1:
                        # Navigate to the next page
                        pag_url = f"{category_url}?page={page_num}"
                        self.logger.info(f"Loading page {page_num}/{total_pages}: {pag_url}")
                        
                        try:
                            self.page_load_timeout = 60
                            self.driver.set_page_load_timeout(60)
                            pag_html = self.get_page(pag_url, use_selenium=True, wait_time=2)
                            if not pag_html or len(pag_html) < 5000:
                                self.logger.warning(f"Page {page_num} content too short, skipping")
                                continue
                            
                            # IMPROVED: More thorough scrolling to load ALL lazy-loaded content
                            self.logger.info("Scrolling to load all products on this page...")
                            for scroll_round in range(5):  # Scroll multiple times
                                # Scroll to bottom
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(1.5)
                                # Scroll back up a bit
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                                time.sleep(0.5)
                                # Scroll to bottom again
                                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(1.5)
                            
                            # Final scroll using existing method
                            self._scroll_to_load_content()
                            time.sleep(3)  # Additional wait for all content to load
                            
                            pag_html = self.driver.page_source
                            soup = BeautifulSoup(pag_html, 'lxml')
                        except Exception as e:
                            self.logger.warning(f"Error loading page {page_num}: {str(e)}")
                            continue
                        finally:
                            try:
                                self.page_load_timeout = original_timeout
                                self.driver.set_page_load_timeout(original_timeout)
                            except:
                                pass
                    
                    # Extract product links from current page
                    self.logger.info(f"Extracting products from page {page_num}/{total_pages}...")
                    
                    # Strategy 1: Use Selenium to find all links (more reliable for dynamic content)
                    found_urls = set()
                    if self.driver:
                        try:
                            # Find all links that match the product URL pattern
                            selenium_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/genuine/kia-']")
                            for elem in selenium_links:
                                try:
                                    href = elem.get_attribute('href')
                                    if href and '/genuine/kia-' in href and '~' in href and href.endswith('.html'):
                                        # Normalize URL
                                        if '#' in href:
                                            href = href.split('#')[0]
                                        if '?' in href:
                                            href = href.split('?')[0]
                                        href = href.rstrip('/')
                                        
                                        # Filter out category/listing pages
                                        if not any(pattern in href for pattern in ['/accessories/', '/category/', '/oem-kia-']):
                                            found_urls.add(href)
                                except:
                                    continue
                            
                            self.logger.info(f"Found {len(found_urls)} product URLs via Selenium")
                        except Exception as e:
                            self.logger.debug(f"Selenium link extraction failed: {str(e)}")
                    
                    # Strategy 2: Use BeautifulSoup as fallback
                    if not found_urls:
                        # Product URLs: /genuine/kia-{name}~{part}.html
                        product_links = soup.find_all('a', href=re.compile(r'/genuine/kia-.*~.*\.html'))
                        
                        for link in product_links:
                            href = link.get('href', '')
                            if href:
                                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                
                                # Remove fragment and query params
                                if '#' in full_url:
                                    full_url = full_url.split('#')[0]
                                if '?' in full_url:
                                    full_url = full_url.split('?')[0]
                                
                                full_url = full_url.rstrip('/')
                                
                                # Only collect individual product pages
                                if '/genuine/kia-' in full_url and '~' in full_url and full_url.endswith('.html'):
                                    # Filter out category/listing pages
                                    if not any(pattern in full_url for pattern in ['/accessories/', '/category/', '/oem-kia-']):
                                        found_urls.add(full_url)
                        
                        self.logger.info(f"Found {len(found_urls)} product URLs via BeautifulSoup")
                    
                    # Strategy 3: Try alternative patterns (in case links use different format)
                    if not found_urls and self.driver:
                        try:
                            # Try finding links by text content or other attributes
                            all_links = self.driver.find_elements(By.TAG_NAME, 'a')
                            for link in all_links:
                                try:
                                    href = link.get_attribute('href') or ''
                                    if href and '/genuine/kia-' in href.lower() and '~' in href and '.html' in href.lower():
                                        # Normalize URL
                                        if '#' in href:
                                            href = href.split('#')[0]
                                        if '?' in href:
                                            href = href.split('?')[0]
                                        href = href.rstrip('/')
                                        
                                        # Filter out category/listing pages
                                        if not any(pattern in href.lower() for pattern in ['/accessories/', '/category/', '/oem-kia-']):
                                            found_urls.add(href)
                                except:
                                    continue
                            
                            if found_urls:
                                self.logger.info(f"Found {len(found_urls)} product URLs via alternative pattern matching")
                        except Exception as e:
                            self.logger.debug(f"Alternative link extraction failed: {str(e)}")
                    
                    # Add found URLs to the collection
                    page_count = 0
                    for full_url in found_urls:
                        if full_url not in existing_urls and full_url not in new_urls:
                            new_urls.append(full_url)
                            existing_urls.append(full_url)
                            page_count += 1
                    
                    self.logger.info(f"Page {page_num}/{total_pages}: Found {len(found_urls)} product links, {page_count} new unique URLs (Category total: {len(new_urls)})")
                    
                    # Check for consecutive zero new products
                    if page_count == 0:
                        consecutive_zero_count += 1
                        self.logger.info(f"No new products found on page {page_num} (consecutive zero count: {consecutive_zero_count}/4)")
                        if consecutive_zero_count >= 4:
                            self.logger.info(f"Stopping pagination: Found zero new products {consecutive_zero_count} times consecutively")
                            break
                    else:
                        # Reset counter if we found new products
                        consecutive_zero_count = 0
                    
                    # Small delay between pages
                    if page_num < total_pages:
                        time.sleep(random.uniform(1, 2))
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    continue
            
            self.logger.info(f"Completed category {category_url}: Found {len(new_urls)} new unique product URLs")
            
        except Exception as e:
            self.logger.error(f"Error extracting products from category {category_url}: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return new_urls
    
    def _extract_products_from_parts_list(self, parts_list_url, existing_urls):
        """
        Extract all product URLs from a parts-list page (e.g., /parts-list/2009-kia-amanti/chassis/wheel_cap.html)
        These pages show individual products with links to /genuine/kia-*~*.html pages
        """
        new_urls = []
        
        try:
            if not self.driver:
                self.ensure_driver()
            
            original_timeout = self.page_load_timeout
            try:
                self.page_load_timeout = 60
                self.driver.set_page_load_timeout(60)
                html = self.get_page(parts_list_url, use_selenium=True, wait_time=3)
                if not html or len(html) < 5000:
                    self.logger.warning(f"Parts-list page content too short: {parts_list_url}")
                    return new_urls
            except Exception as e:
                self.logger.warning(f"Error loading parts-list page {parts_list_url}: {str(e)}")
                return new_urls
            finally:
                try:
                    self.page_load_timeout = original_timeout
                    self.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait for product links to appear
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/genuine/kia-']"))
                )
            except:
                self.logger.warning("Product links not found immediately, continuing anyway...")
            
            # Scroll to load all content
            self.logger.info("Scrolling to load all products...")
            for scroll_round in range(5):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                time.sleep(0.5)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
            
            # Final scroll using existing method
            self._scroll_to_load_content()
            time.sleep(3)
            
            # Get updated HTML after scrolling
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract product links from parts-list page
            self.logger.info("Extracting products from parts-list page...")
            
            # Strategy 1: Use Selenium to find all links
            found_urls = set()
            if self.driver:
                try:
                    selenium_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/genuine/kia-']")
                    for elem in selenium_links:
                        try:
                            href = elem.get_attribute('href')
                            if href and '/genuine/kia-' in href and '~' in href and href.endswith('.html'):
                                # Normalize URL
                                if '#' in href:
                                    href = href.split('#')[0]
                                if '?' in href:
                                    href = href.split('?')[0]
                                href = href.rstrip('/')
                                
                                # Filter out category/listing pages
                                if not any(pattern in href for pattern in ['/accessories/', '/category/', '/oem-kia-', '/parts-list/']):
                                    found_urls.add(href)
                        except:
                            continue
                    
                    self.logger.info(f"Found {len(found_urls)} product URLs via Selenium")
                except Exception as e:
                    self.logger.debug(f"Selenium link extraction failed: {str(e)}")
            
            # Strategy 2: Use BeautifulSoup as fallback
            if not found_urls:
                product_links = soup.find_all('a', href=re.compile(r'/genuine/kia-.*~.*\.html'))
                
                for link in product_links:
                    href = link.get('href', '')
                    if href:
                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                        
                        # Remove fragment and query params
                        if '#' in full_url:
                            full_url = full_url.split('#')[0]
                        if '?' in full_url:
                            full_url = full_url.split('?')[0]
                        
                        full_url = full_url.rstrip('/')
                        
                        # Only collect individual product pages
                        if '/genuine/kia-' in full_url and '~' in full_url and full_url.endswith('.html'):
                            # Filter out category/listing pages
                            if not any(pattern in full_url for pattern in ['/accessories/', '/category/', '/oem-kia-', '/parts-list/']):
                                found_urls.add(full_url)
                
                self.logger.info(f"Found {len(found_urls)} product URLs via BeautifulSoup")
            
            # Add found URLs to the collection
            for full_url in found_urls:
                if full_url not in existing_urls and full_url not in new_urls:
                    new_urls.append(full_url)
                    existing_urls.append(full_url)
            
            self.logger.info(f"Parts-list page {parts_list_url}: Found {len(found_urls)} product links, {len(new_urls)} new unique URLs")
            
        except Exception as e:
            self.logger.error(f"Error extracting products from parts-list page {parts_list_url}: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return new_urls
    
    def _generate_parts_list_urls(self):
        """
        Generate parts-list URLs for all Kia models with their specific year ranges
        Pattern: https://www.kiapartsnow.com/parts-list/[year]-kia-[model]/chassis/wheel_cap.html
        
        Only generates URLs for years between 2000-2024 (ignores years outside this range)
        """
        # Model to year range mapping (start_year, end_year inclusive)
        model_year_ranges = {
            'Amanti': (2004, 2009),
            'Borrego': (2008, 2012),
            'Cadenza': (2013, 2020),
            'Carnival': (2022, 2024),
            'EV6': (2022, 2024),
            'Forte': (2009, 2023),
            'Forte Koup': (2009, 2016),
            'K5': (2021, 2024),
            'K900': (2015, 2020),
            'Niro': (2017, 2024),
            'Niro EV': (2019, 2024),
            'Optima': (2000, 2020),
            'Optima Hybrid': (2011, 2020),
            'Rio': (2000, 2023),
            'Rondo': (2006, 2011),
            'Sedona': (2001, 2021),
            'Seltos': (2021, 2024),
            'Sephia': (1997, 2001),  # Includes 1997-1999
            'Sorento': (2003, 2023),
            'Soul': (2009, 2024),
            'Soul EV': (2015, 2019),
            'Spectra': (2000, 2009),
            'Spectra SX': (2007, 2009),
            'Spectra5 SX': (2007, 2009),
            'Sportage': (1997, 2024),  # Includes 1997-1999
            'Stinger': (2018, 2023),
            'Telluride': (2020, 2024),
        }
        
        generated_urls = []
        total_combinations = 0
        
        for model, (start_year, end_year) in model_year_ranges.items():
            # Ensure end_year doesn't exceed 2024
            end_year = min(2024, end_year)
            
            # Convert model name to URL format (lowercase, spaces to underscores)
            # Examples: "Forte Koup" -> "forte_koup", "Niro EV" -> "niro_ev"
            model_url = model.lower().replace(' ', '_')
            
            # Generate URLs for the year range (inclusive)
            for year in range(start_year, end_year + 1):
                url = f"{self.base_url}/parts-list/{year}-kia-{model_url}/chassis/wheel_cap.html"
                generated_urls.append(url)
                total_combinations += 1
        
        self.logger.info(f"Generated {len(generated_urls)} parts-list URLs from {len(model_year_ranges)} models with specific year ranges")
        return generated_urls
    
    def _discover_additional_product_urls(self, existing_urls):
        """
        Discover additional product URLs by:
        1. Trying to find sitemap
        2. Visiting category index pages
        3. Finding all links matching /genuine/kia-*~*.html pattern from homepage or category pages
        """
        new_urls = []
        existing_set = set(existing_urls)
        
        try:
            # Method 1: Try to find sitemap
            sitemap_urls = [
                f"{self.base_url}/sitemap.xml",
                f"{self.base_url}/sitemap_index.xml",
                f"{self.base_url}/sitemap-products.xml",
            ]
            
            for sitemap_url in sitemap_urls:
                try:
                    self.logger.info(f"Trying sitemap: {sitemap_url}")
                    html = self.get_page(sitemap_url, use_selenium=False, wait_time=1)
                    if html and 'genuine/kia-' in html:
                        # Extract URLs from sitemap
                        try:
                            import xml.etree.ElementTree as ET
                            root = ET.fromstring(html)
                            for url_elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
                                url = url_elem.text
                                if url and '/genuine/kia-' in url.lower() and '~' in url and '.html' in url.lower():
                                    if url not in existing_set:
                                        new_urls.append(url)
                                        existing_set.add(url)
                            self.logger.info(f"Found {len(new_urls)} URLs from sitemap")
                            break
                        except:
                            # Try regex extraction if XML parsing fails
                            urls = re.findall(r'<loc>(https?://[^<]+/genuine/kia-[^<]*~[^<]*\.html)</loc>', html, re.I)
                            for url in urls:
                                if url not in existing_set:
                                    new_urls.append(url)
                                    existing_set.add(url)
                            if urls:
                                self.logger.info(f"Found {len(urls)} URLs from sitemap (regex)")
                                break
                except:
                    continue
            
            # Method 2: Visit homepage and find all product links
            if not new_urls:
                try:
                    self.logger.info("Visiting homepage to discover product URLs...")
                    html = self.get_page(self.base_url, use_selenium=True, wait_time=2)
                    if html:
                        # Scroll to load all content
                        self._scroll_to_load_content()
                        html = self.driver.page_source
                        soup = BeautifulSoup(html, 'lxml')
                        
                        # Find all links matching the pattern
                        all_links = soup.find_all('a', href=re.compile(r'/genuine/kia-.*~.*\.html', re.I))
                        for link in all_links:
                            href = link.get('href', '')
                            if href:
                                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                                if '#' in full_url:
                                    full_url = full_url.split('#')[0]
                                if '?' in full_url:
                                    full_url = full_url.split('?')[0]
                                full_url = full_url.rstrip('/')
                                
                                if '/genuine/kia-' in full_url.lower() and '~' in full_url and '.html' in full_url.lower():
                                    if not any(pattern in full_url.lower() for pattern in ['/accessories/', '/category/', '/oem-kia-']):
                                        if full_url not in existing_set:
                                            new_urls.append(full_url)
                                            existing_set.add(full_url)
                        
                        if new_urls:
                            self.logger.info(f"Found {len(new_urls)} product URLs from homepage")
                except Exception as e:
                    self.logger.debug(f"Homepage discovery failed: {str(e)}")
            
            # Method 3: Try additional category/listing pages that might contain wheel products
            additional_category_urls = [
                f"{self.base_url}/oem-parts/kia-wheel.html",
                f"{self.base_url}/oem-parts/kia-rim.html",
                f"{self.base_url}/genuine/kia-wheel.html",
                f"{self.base_url}/genuine/kia-rim.html",
            ]
            
            for cat_url in additional_category_urls:
                try:
                    self.logger.info(f"Trying additional category: {cat_url}")
                    category_products = self._extract_products_from_category(cat_url, existing_urls)
                    for url in category_products:
                        if url not in existing_set:
                            new_urls.append(url)
                            existing_set.add(url)
                    if category_products:
                        self.logger.info(f"Found {len(category_products)} products from {cat_url}")
                except:
                    continue
            
        except Exception as e:
            self.logger.debug(f"Additional discovery failed: {str(e)}")
        
        return new_urls
    
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
        """Scrape single product from www.kiapartsnow.com"""
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
                    
                    # Verify driver is still connected before checking Cloudflare
                    try:
                        if self.driver:
                            _ = self.driver.current_url  # Test connection
                    except (ConnectionResetError, OSError, Exception) as conn_error:
                        self.logger.warning(f"Driver connection lost after page load: {str(conn_error)}")
                        if retry_count < max_retries:
                            retry_count += 1
                            time.sleep(random.uniform(5, 10))
                            continue
                        else:
                            return None
                    
                    try:
                        if self.has_cloudflare_challenge():
                            cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                            if not cloudflare_bypassed:
                                retry_count += 1
                                if retry_count < max_retries:
                                    time.sleep(random.uniform(10, 15))
                                    continue
                                else:
                                    return None
                    except (ConnectionResetError, OSError) as conn_error:
                        self.logger.warning(f"Connection error during Cloudflare check: {str(conn_error)}")
                        if retry_count < max_retries:
                            retry_count += 1
                            time.sleep(random.uniform(5, 10))
                            continue
                        else:
                            return None
                    
                    time.sleep(random.uniform(1.5, 3.0))
                    try:
                        html = self.driver.page_source
                    except (ConnectionResetError, OSError) as conn_error:
                        self.logger.warning(f"Connection lost while getting page source: {str(conn_error)}")
                        if retry_count < max_retries:
                            retry_count += 1
                            time.sleep(random.uniform(5, 10))
                            continue
                        else:
                            return None
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
            # Extract title
            title_elem = soup.find('h1')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            if not product_data['title']:
                title_tag = soup.find('title')
                if title_tag:
                    product_data['title'] = self.safe_find_text(soup, title_tag)
            
            if not product_data['title'] or len(product_data['title']) < 3:
                return None
            
            self.logger.info(f"📝 Found title: {self.safe_str(product_data['title'][:60])}")
            
            # Extract SKU/Part Number - from URL pattern: /genuine/kia-{name}~{part}.html
            url_match = re.search(r'/genuine/kia-.*~([^~]+)\.html', url)
            if url_match:
                product_data['sku'] = url_match.group(1)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Also try from page: "Part Number: U82002K000"
            if not product_data['sku']:
                part_number_div = soup.find('div', string=re.compile(r'Part\s+Number\s*:', re.I))
                if part_number_div:
                    part_link = part_number_div.find_next('a')
                    if part_link:
                        product_data['sku'] = part_link.get_text(strip=True)
                        product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Check if wheel product
            try:
                is_wheel = self.is_wheel_product(product_data['title'])
                if not is_wheel:
                    return None
            except:
                return None
            
            # Extract price - structure: $79.90 with MSRP: $105.00
            price_div = soup.find('div', class_=re.compile(r'price|sale', re.I))
            if price_div:
                price_text = price_div.get_text(strip=True)
                # Look for dollar amount
                price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
                if price_match:
                    product_data['actual_price'] = price_match.group(1).replace(',', '')
            
            # Extract MSRP
            msrp_elem = soup.find(string=re.compile(r'MSRP\s*:', re.I))
            if msrp_elem:
                msrp_parent = msrp_elem.find_parent()
                if msrp_parent:
                    msrp_text = msrp_parent.get_text(strip=True)
                    msrp_match = re.search(r'MSRP\s*:\s*\$([\d,]+\.?\d*)', msrp_text, re.I)
                    if msrp_match:
                        product_data['msrp'] = msrp_match.group(1).replace(',', '')
            
            # Extract image URL - look for main product image in pn-img-img div
            img_container = soup.find('div', class_='pn-img-img')
            if img_container:
                img_elem = img_container.find('img')
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Fallback: try other image patterns
            if not product_data['image_url']:
                img_elem = soup.find('img', src=re.compile(r'/resources/encry/actual-picture', re.I))
                if img_elem:
                    img_url = img_elem.get('src') or img_elem.get('data-src')
                    if img_url:
                        if img_url.startswith('//'):
                            product_data['image_url'] = f"https:{img_url}"
                        elif img_url.startswith('/'):
                            product_data['image_url'] = f"{self.base_url}{img_url}"
                        else:
                            product_data['image_url'] = img_url
            
            # Extract description
            desc_list = soup.find('ul', class_=re.compile(r'description|spec', re.I))
            if desc_list:
                desc_items = desc_list.find_all('li')
                desc_texts = [item.get_text(strip=True) for item in desc_items]
                product_data['description'] = ' '.join(desc_texts)
            
            # Extract fitment from table structure
            # Table: fit-vehicle-list-table with columns: Year Make Model, Trim & Engine, Important vehicle option details
            fitment_table = soup.find('table', class_='fit-vehicle-list-table')
            if fitment_table:
                tbody = fitment_table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 3:
                            # First column: Year Make Model (e.g., "2009-2021 Kia Forte")
                            year_model_cell = cells[0]
                            year_model_text = year_model_cell.get_text(strip=True)
                            
                            # Extract year range and model
                            year_range_match = re.search(r'(\d{4})\s*-\s*(\d{4})', year_model_text)
                            start_year = None
                            end_year = None
                            
                            if year_range_match:
                                start_year = int(year_range_match.group(1))
                                end_year = int(year_range_match.group(2))
                            else:
                                # Try single year
                                year_match = re.search(r'(\d{4})', year_model_text)
                                if year_match:
                                    start_year = int(year_match.group(1))
                                    end_year = int(year_match.group(1))
                            
                            if start_year is not None and end_year is not None:
                                
                                # Extract model name (remove year range/single year and "Kia" prefix)
                                model_text = re.sub(r'\d{4}\s*-\s*\d{4}\s*', '', year_model_text)  # Remove year range
                                model_text = re.sub(r'^\d{4}\s+', '', model_text)  # Remove single year if still present
                                model_text = re.sub(r'^Kia\s+', '', model_text, flags=re.I).strip()
                                
                                # Second column: Trim & Engine (comma-separated, may contain pipe "|")
                                trim_engine_cell = cells[1]
                                trim_engine_text = trim_engine_cell.get_text(strip=True)
                                # Split by comma, handling formats like "High Grade|1.6L, 1.6L - Alpha DOHC"
                                trim_engines = [te.strip() for te in trim_engine_text.split(',') if te.strip()]
                                
                                # Third column: Important vehicle option details (comma-separated)
                                options_cell = cells[2]
                                options_text = options_cell.get_text(strip=True)
                                options = [opt.strip() for opt in options_text.split(',') if opt.strip()]
                                
                                # Expand year range and create fitment records for all combinations
                                # Formula: years × trim_engines × options
                                for year in range(start_year, end_year + 1):
                                    year_str = str(year)
                                    
                                    # If we have trim/engines, create one record per trim/engine
                                    if trim_engines:
                                        for trim_engine in trim_engines:
                                            # Each trim_engine is the full engine description (e.g., "1.6L - GAMMA" or "High Grade|1.6L")
                                            engine = trim_engine.strip()
                                            
                                            # If we have options, create one record per option
                                            if options:
                                                for option in options:
                                                    product_data['fitments'].append({
                                                        'year': year_str,
                                                        'make': 'Kia',
                                                        'model': model_text,
                                                        'trim': option,
                                                        'engine': engine
                                                    })
                                            else:
                                                # No options, create one record per engine
                                                product_data['fitments'].append({
                                                    'year': year_str,
                                                    'make': 'Kia',
                                                    'model': model_text,
                                                    'trim': '',
                                                    'engine': engine
                                                })
                                    else:
                                        # No trim/engine, create one record per year
                                        if options:
                                            # Create one record per option
                                            for option in options:
                                                product_data['fitments'].append({
                                                    'year': year_str,
                                                    'make': 'Kia',
                                                    'model': model_text,
                                                    'trim': option,
                                                    'engine': ''
                                                })
                                        else:
                                            # No options either, create one record per year
                                            product_data['fitments'].append({
                                                'year': year_str,
                                                'make': 'Kia',
                                                'model': model_text,
                                                'trim': '',
                                                'engine': ''
                                            })
            
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
            return None

