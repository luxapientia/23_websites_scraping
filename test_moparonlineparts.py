"""Test script for MoparOnlineParts scraper - finds 5 products and exports to Excel"""
import sys
import logging
import os
from datetime import datetime
from tqdm import tqdm

from scrapers.moparonlineparts_scraper import MoparOnlinePartsScraper
from utils.data_processor import DataProcessor
from utils.excel_exporter import ExcelExporter


def setup_logging():
    """Setup logging with UTF-8 encoding to handle emojis"""
    logger = logging.getLogger('test_moparonlineparts')
    logger.setLevel(logging.INFO)
    logger.handlers = []  # Remove existing handlers
    
    # File handler with UTF-8 encoding
    log_file = f'logs/test_moparonlineparts_{datetime.now().strftime("%Y%m%d")}.log'
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
        logger.info("TEST: MoparOnlineParts Scraper - 5 Products")
        logger.info("="*70)
        
        # Create scraper
        logger.info("Initializing scraper...")
        scraper = MoparOnlinePartsScraper()
        logger.info("✓ Scraper initialized")
        
        # Step 1: Get only 5 product URLs (stop early)
        logger.info("\n" + "="*70)
        logger.info("STEP 1: Finding 5 product URLs (will stop after finding 5)...")
        logger.info("="*70)
        
        # Use a wrapper that stops after finding 5 URLs
        test_urls = []
        max_urls = 5
        
        # Monkey-patch the scraper's internal methods to stop early
        original_search = scraper._search_for_wheels
        original_browse = scraper._browse_wheel_categories
        
        def limited_search():
            """Limited search that stops after finding max_urls"""
            urls = []
            try:
                if not scraper.driver:
                    scraper.ensure_driver()
                
                search_url = f"{scraper.base_url}/search?search_str=wheel"
                scraper.logger.info(f"Searching: {search_url}")
                
                # Increase timeout
                original_timeout = scraper.page_load_timeout
                try:
                    scraper.page_load_timeout = 60
                    scraper.driver.set_page_load_timeout(60)
                    html = scraper.get_page(search_url, use_selenium=True, wait_time=2)
                    if not html:
                        return urls
                finally:
                    scraper.page_load_timeout = original_timeout
                    scraper.driver.set_page_load_timeout(original_timeout)
                
                # Wait for results
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                try:
                    WebDriverWait(scraper.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/oem-parts/'], a[href*='/parts/']"))
                    )
                except:
                    pass
                
                # Scroll a bit to load content
                scraper._scroll_to_load_content()
                
                # Extract URLs
                from bs4 import BeautifulSoup
                import re
                html = scraper.driver.page_source
                soup = BeautifulSoup(html, 'lxml')
                product_links = soup.find_all('a', href=re.compile(r'/oem-parts/|/parts/[^/]+/[^/]+'))
                
                for link in product_links:
                    if len(urls) >= max_urls:
                        break
                    href = link.get('href', '')
                    if href:
                        full_url = href if href.startswith('http') else f"{scraper.base_url}{href}"
                        if '?' in full_url:
                            full_url = full_url.split('?')[0]
                        if '#' in full_url:
                            full_url = full_url.split('#')[0]
                        full_url = full_url.rstrip('/')
                        
                        # Validate it's a product page
                        is_product = (
                            re.search(r'/oem-parts/', full_url, re.I) or
                            re.search(r'/parts/[^/]+/[^/]+', full_url, re.I)
                        )
                        is_category = (
                            re.search(r'/wheels$', full_url, re.I) or
                            re.search(r'/search', full_url, re.I) or
                            re.search(r'/v-', full_url, re.I)
                        )
                        
                        if is_product and not is_category and full_url not in urls:
                            urls.append(full_url)
                            if len(urls) >= max_urls:
                                break
                
            except Exception as e:
                scraper.logger.error(f"Error in limited search: {str(e)}")
            
            return urls
        
        # Try to find 5 URLs
        logger.info("Searching for wheel products...")
        test_urls = limited_search()
        logger.info(f"Found {len(test_urls)} URLs")
        
        if not test_urls:
            logger.error("❌ No product URLs found!")
            return
        
        logger.info(f"✓ Found {len(test_urls)} product URLs for testing")
        logger.info(f"\nProduct URLs to scrape:")
        for i, url in enumerate(test_urls, 1):
            logger.info(f"  {i}. {url}")
        
        # Step 2: Scrape products
        logger.info("\n" + "="*70)
        logger.info("STEP 2: Scraping products...")
        logger.info("="*70)
        
        products = []
        for idx, url in enumerate(tqdm(test_urls, desc="Scraping products"), 1):
            logger.info(f"\n[{idx}/5] Scraping: {url}")
            try:
                product = scraper.scrape_product(url)
                if product:
                    products.append(product)
                    logger.info(f"✓ Successfully scraped: {product.get('title', 'N/A')[:60]}")
                else:
                    logger.warning(f"⚠️ Failed to scrape product (returned None)")
            except Exception as e:
                logger.error(f"❌ Error scraping {url}: {str(e)}")
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
        output_file = f"data/processed/moparonlineparts_test_{timestamp}.xlsx"
        
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
        
        # Print some sample data
        if len(df) > 0:
            logger.info("\nSample data (first row):")
            logger.info(f"  Title: {df.iloc[0].get('title', 'N/A')}")
            logger.info(f"  SKU: {df.iloc[0].get('sku', 'N/A')}")
            logger.info(f"  Price: {df.iloc[0].get('actual_price', 'N/A')}")
            logger.info(f"  Year: {df.iloc[0].get('year', 'N/A')}")
            logger.info(f"  Make: {df.iloc[0].get('make', 'N/A')}")
            logger.info(f"  Model: {df.iloc[0].get('model', 'N/A')}")
        
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
            scraper.close()
            logger.info("✓ Scraper closed")


if __name__ == "__main__":
    main()

