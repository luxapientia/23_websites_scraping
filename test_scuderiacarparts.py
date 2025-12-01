"""Test script for ScuderiaCarParts scraper - finds 5 products and exports to Excel"""
import sys
import logging
import os
import time
import re
from datetime import datetime
from tqdm import tqdm

from scrapers.scuderiacarparts_scraper import ScuderiaCarPartsScraper
from utils.data_processor import DataProcessor
from utils.excel_exporter import ExcelExporter


def setup_logging():
    """Setup logging with UTF-8 encoding to handle emojis"""
    logger = logging.getLogger('test_scuderiacarparts')
    logger.setLevel(logging.INFO)
    logger.handlers = []  # Remove existing handlers
    
    # File handler with UTF-8 encoding
    log_file = f'logs/test_scuderiacarparts_{datetime.now().strftime("%Y%m%d")}.log'
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Console handler with safe Unicode handling
    class SafeUnicodeHandler(logging.StreamHandler):
        def emit(self, record):
            try:
                super().emit(record)
            except UnicodeEncodeError:
                # Replace emojis with plain text alternatives
                msg = self.format(record)
                msg = msg.replace('🔍', '[SEARCH]')
                msg = msg.replace('✅', '[OK]')
                msg = msg.replace('❌', '[X]')
                msg = msg.replace('💰', '[PRICE]')
                msg = msg.replace('🛡️', '[SHIELD]')
                msg = msg.replace('⏳', '[WAIT]')
                msg = msg.replace('🔄', '[RETRY]')
                msg = msg.replace('📝', '[NOTE]')
                msg = msg.replace('📦', '[BOX]')
                msg = msg.replace('⚠️', '[WARN]')
                try:
                    self.stream.write(msg + self.terminator)
                    self.flush()
                except UnicodeEncodeError:
                    # Last resort: encode with errors='replace'
                    safe_msg = msg.encode(self.stream.encoding or 'utf-8', errors='replace').decode(self.stream.encoding or 'utf-8', errors='replace')
                    self.stream.write(safe_msg + self.terminator)
                    self.flush()
    
    console_handler = SafeUnicodeHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger


def main():
    """Main test function"""
    logger = setup_logging()
    scraper = None
    
    try:
        logger.info("="*70)
        logger.info("TEST: ScuderiaCarParts Scraper - 5 Products")
        logger.info("="*70)
        
        # Create scraper
        logger.info("Initializing scraper...")
        scraper = ScuderiaCarPartsScraper()
        logger.info("✓ Scraper initialized")
        
        # Step 1: Get only 5 product URLs from initial page load (before clicking "Load more results")
        logger.info("\n" + "="*70)
        logger.info("STEP 1: Finding 5 product URLs from initial page (NO 'Load more results' clicks)...")
        logger.info("="*70)
        
        max_urls = 5
        test_urls = []
        
        try:
            if not scraper.driver:
                scraper.ensure_driver()
            
            # Load the search page (same URL as in _search_for_wheels)
            search_url = f"{scraper.base_url}/search/?stc=RM8&sac=N&q=wheel&params=eyJtYXNlcmF0aSI6bnVsbCwiYmVudGxleSI6bnVsbCwibGFuZHJvdmVyIjpudWxsLCJ0eXBlIjpbIk9yaWdpbmFsIFBhcnRzIiwiVHVuaW5nIFBhcnRzIl0sImFzdG9ubWFydGluIjpudWxsLCJhdWRpIjpudWxsLCJibXciOm51bGwsImZlcnJhcmkiOm51bGwsImhvbmRhIjpudWxsLCJsYW1ib3JnaGluaSI6bnVsbCwibWNsYXJlbiI6bnVsbCwibWVyY2VkZXMiOm51bGwsIm5pc3NhbiI6bnVsbCwicG9yc2NoZSI6bnVsbCwicm9sbHNyb3ljZSI6bnVsbCwidGVzbGEiOm51bGx9"
            logger.info(f"Loading search page: {search_url}")
            
            # Load the page with timeout
            original_timeout = scraper.page_load_timeout
            try:
                scraper.page_load_timeout = 60
                scraper.driver.set_page_load_timeout(60)
                scraper.driver.get(search_url)
                time.sleep(3)  # Wait for initial page load
            finally:
                try:
                    scraper.page_load_timeout = original_timeout
                    scraper.driver.set_page_load_timeout(original_timeout)
                except:
                    pass
            
            # Wait a bit for products to render
            time.sleep(2)
            
            # Now extract products from the INITIAL page state (before clicking "Load more results")
            # Use TITLE-BASED filtering (NOT URL-based selectors)
            from bs4 import BeautifulSoup
            from selenium.webdriver.common.by import By
            import re
            
            logger.info("Extracting product items from initial page by TITLE (not by URL pattern)...")
            
            # Find product containers on the page
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
                if len(test_urls) >= max_urls:
                    break
                
                try:
                    containers = scraper.driver.find_elements(By.CSS_SELECTOR, container_selector)
                    if containers and len(containers) > 0:
                        logger.info(f"Found {len(containers)} product containers using selector: {container_selector}")
                        
                        for container in containers:
                            if len(test_urls) >= max_urls:
                                break
                            
                            try:
                                # Step 1: Extract TITLE from container FIRST
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
                                            logger.debug(f"Extracted title from mt-md/mt-xs structure: '{title[:60]}'")
                                except:
                                    # Fallback: Try generic selectors
                                    title_selectors = [
                                        "h1, h2, h3, h4",
                                        ".product-title",
                                        ".product-name",
                                        "[class*='title']",
                                        "[class*='name']",
                                        "a[title]",
                                    ]
                                    
                                    for title_selector in title_selectors:
                                        try:
                                            title_elem = container.find_element(By.CSS_SELECTOR, title_selector)
                                            if title_elem:
                                                title = title_elem.text.strip()
                                                if not title:
                                                    title = title_elem.get_attribute('title') or title_elem.get_attribute('data-title') or ''
                                                if title and len(title) >= 3:
                                                    break
                                        except:
                                            continue
                                    
                                    # If no title found in container, try link text
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
                                    is_wheel = scraper.is_wheel_product(title)
                                    
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
                                                if href.startswith('javascript:'):
                                                    continue
                                                
                                                full_url = href if href.startswith('http') else f"{scraper.base_url}{href}"
                                                if '#' in full_url:
                                                    full_url = full_url.split('#')[0]
                                                if '?' in full_url:
                                                    full_url = full_url.split('?')[0]
                                                full_url = full_url.rstrip('/')
                                                
                                                skip_patterns = ['/search/', '/category/', '/cart/', '/checkout/', '/account/', '/login/', '/register/', '/contact/', '/about/', '/help/']
                                                if not any(exclude in full_url.lower() for exclude in skip_patterns):
                                                    if full_url not in test_urls:
                                                        test_urls.append(full_url)
                                                        safe_title = scraper.safe_str(title[:60])
                                                        logger.info(f"✓ [{len(test_urls)}/{max_urls}] WHEEL: '{safe_title}' -> {full_url[:80]}")
                                                        if len(test_urls) >= max_urls:
                                                            logger.info(f"✓ Found {max_urls} product URLs, stopping search...")
                                                            break
                                        except Exception as url_error:
                                            continue
                            
                            except Exception as container_error:
                                continue
                        
                        if len(test_urls) >= max_urls:
                            break
                except Exception as e:
                    logger.debug(f"Error with container selector {container_selector}: {str(e)[:50]}")
                    continue
            
            logger.info(f"✓ Found {len(test_urls)} wheel product URLs from initial page (stopped at {max_urls})")
            
        except Exception as e:
            logger.error(f"Error finding product URLs: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't return here - continue to check if any URLs were found before the error
        
        # Check if we found any URLs
        logger.info(f"DEBUG: Checking test_urls - length: {len(test_urls) if test_urls else 0}, type: {type(test_urls)}")
        if not test_urls or len(test_urls) == 0:
            logger.error("❌ No product URLs found!")
            logger.error("This could mean:")
            logger.error("  1. No product containers were found on the page")
            logger.error("  2. No product titles matched WHEEL_KEYWORDS and EXCLUDE_KEYWORDS")
            logger.error("  3. An error occurred before URLs could be extracted")
            return
        
        logger.info(f"✓ Successfully found {len(test_urls)} product URLs for testing")
        logger.info(f"DEBUG: Proceeding to next step (scraping products)...")
        logger.info(f"\nProduct URLs to scrape:")
        for i, url in enumerate(test_urls, 1):
            logger.info(f"  {i}. {url}")
        
        # Step 2: Scrape products
        logger.info("\n" + "="*70)
        logger.info("STEP 2: Scraping products...")
        logger.info("="*70)
        
        products = []
        for idx, url in enumerate(tqdm(test_urls, desc="Scraping products"), 1):
            logger.info(f"\n[{idx}/{len(test_urls)}] Scraping: {url}")
            try:
                product = scraper.scrape_product(url)
                if product:
                    products.append(product)
                    safe_title = scraper.safe_str(product.get('title', 'N/A')[:60])
                    logger.info(f"✓ Successfully scraped: {safe_title}")
                    
                    # Log extracted data to verify the updated extraction logic
                    logger.info("  Extracted data:")
                    logger.info(f"    Title: {scraper.safe_str(product.get('title', 'N/A')[:80])}")
                    logger.info(f"    SKU: {product.get('sku', 'N/A')}")
                    logger.info(f"    Part Number: {product.get('pn', 'N/A')}")
                    logger.info(f"    Price: {product.get('actual_price', 'N/A')}")
                    logger.info(f"    MSRP: {product.get('msrp', 'N/A')}")
                    logger.info(f"    Image URL: {product.get('image_url', 'N/A')[:80] if product.get('image_url') else 'N/A'}")
                    logger.info(f"    Replaces: {scraper.safe_str(product.get('replaces', 'N/A')[:100])}")
                    logger.info(f"    Fitments: {len(product.get('fitments', []))} fitment(s) found")
                    
                    # Validate that key fields were extracted
                    missing_fields = []
                    if not product.get('title'):
                        missing_fields.append('title')
                    if not product.get('sku') and not product.get('pn'):
                        missing_fields.append('sku/pn')
                    if not product.get('actual_price'):
                        missing_fields.append('price')
                    if not product.get('image_url'):
                        missing_fields.append('image_url')
                    
                    if missing_fields:
                        logger.warning(f"    ⚠️ Missing fields: {', '.join(missing_fields)}")
                    else:
                        logger.info(f"    ✅ All key fields extracted successfully")
                else:
                    logger.warning(f"⚠️ Failed to scrape product (returned None)")
            except Exception as e:
                logger.error(f"❌ Error scraping {url}: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
                continue
        
        logger.info(f"\n✓ Scraped {len(products)} out of {len(test_urls)} products")
        
        if not products:
            logger.error("❌ No products were successfully scraped!")
            return
        
        # Step 3: Process data
        logger.info("\n" + "="*70)
        logger.info("STEP 3: Processing data...")
        logger.info("="*70)
        
        processor = DataProcessor()
        df = processor.process_products(products)
        df = processor.clean_data(df)
        
        logger.info(f"✓ Processed {len(df)} rows (including fitments)")
        logger.info(f"✓ Unique products: {len(products)}")
        
        # Step 4: Export to Excel
        logger.info("\n" + "="*70)
        logger.info("STEP 4: Exporting to Excel...")
        logger.info("="*70)
        
        # Create output directory
        os.makedirs('data/processed', exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"data/processed/scuderiacarparts_test_{timestamp}.xlsx"
        
        exporter = ExcelExporter()
        exporter.export_to_excel(df, output_file, apply_formatting=True)
        
        logger.info(f"✓ Exported to: {output_file}")
        
        # Step 5: Summary
        logger.info("\n" + "="*70)
        logger.info("TEST COMPLETE!")
        logger.info("="*70)
        logger.info(f"✓ Products found: {len(test_urls)}")
        logger.info(f"✓ Products tested: {len(test_urls)}")
        logger.info(f"✓ Products scraped: {len(products)}")
        logger.info(f"✓ Total rows in Excel: {len(df)}")
        logger.info(f"✓ Output file: {output_file}")
        logger.info("="*70)
        
        # Print detailed sample data
        if len(df) > 0:
            logger.info("\n" + "="*70)
            logger.info("SAMPLE DATA (First Product):")
            logger.info("="*70)
            first_row = df.iloc[0]
            logger.info(f"  Title: {scraper.safe_str(first_row.get('title', 'N/A'))}")
            logger.info(f"  URL: {first_row.get('url', 'N/A')}")
            logger.info(f"  SKU: {first_row.get('sku', 'N/A')}")
            logger.info(f"  Part Number: {first_row.get('pn', 'N/A')}")
            logger.info(f"  Price: {first_row.get('actual_price', 'N/A')}")
            logger.info(f"  MSRP: {first_row.get('msrp', 'N/A')}")
            logger.info(f"  Image URL: {first_row.get('image_url', 'N/A')[:80] if first_row.get('image_url') else 'N/A'}")
            logger.info(f"  Replaces: {scraper.safe_str(first_row.get('replaces', 'N/A')[:100])}")
            logger.info(f"  Description: {scraper.safe_str(first_row.get('description', 'N/A')[:100])}")
            logger.info(f"  Year: {first_row.get('year', 'N/A')}")
            logger.info(f"  Make: {first_row.get('make', 'N/A')}")
            logger.info(f"  Model: {first_row.get('model', 'N/A')}")
            logger.info(f"  Trim: {first_row.get('trim', 'N/A')}")
            logger.info(f"  Engine: {first_row.get('engine', 'N/A')}")
            
            # Show extraction statistics
            logger.info("\n" + "="*70)
            logger.info("EXTRACTION STATISTICS:")
            logger.info("="*70)
            total_products = len(products)
            products_with_title = sum(1 for p in products if p.get('title'))
            products_with_sku = sum(1 for p in products if p.get('sku') or p.get('pn'))
            products_with_price = sum(1 for p in products if p.get('actual_price'))
            products_with_image = sum(1 for p in products if p.get('image_url'))
            products_with_replaces = sum(1 for p in products if p.get('replaces'))
            products_with_fitments = sum(1 for p in products if p.get('fitments') and len(p.get('fitments', [])) > 0)
            
            logger.info(f"  Products with Title: {products_with_title}/{total_products} ({100*products_with_title/total_products:.1f}%)")
            logger.info(f"  Products with SKU/PN: {products_with_sku}/{total_products} ({100*products_with_sku/total_products:.1f}%)")
            logger.info(f"  Products with Price: {products_with_price}/{total_products} ({100*products_with_price/total_products:.1f}%)")
            logger.info(f"  Products with Image: {products_with_image}/{total_products} ({100*products_with_image/total_products:.1f}%)")
            logger.info(f"  Products with Replaces: {products_with_replaces}/{total_products} ({100*products_with_replaces/total_products:.1f}%)")
            logger.info(f"  Products with Fitments: {products_with_fitments}/{total_products} ({100*products_with_fitments/total_products:.1f}%)")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Test interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Error during test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        if scraper:
            logger.info("\nCleaning up scraper...")
            try:
                scraper.close()
                logger.info("✓ Scraper closed")
            except Exception as cleanup_error:
                # Suppress cleanup errors (especially Chrome driver __del__ errors on Windows)
                error_str = str(cleanup_error).lower()
                if 'handle is invalid' in error_str or 'winerror 6' in error_str:
                    logger.debug(f"Driver cleanup warning (harmless): {str(cleanup_error)}")
                else:
                    logger.warning(f"Error during cleanup: {str(cleanup_error)}")


if __name__ == "__main__":
    main()

