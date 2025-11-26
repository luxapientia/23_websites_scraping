"""Run scraper for a single site (for testing)"""
import sys
import json
import logging
import atexit
import gc
from datetime import datetime

from scrapers.tascaparts_scraper import TascaPartsScraper
from scrapers.generic_scraper import GenericScraper
from utils.data_processor import DataProcessor
from utils.excel_exporter import ExcelExporter

# Global scraper reference for cleanup
_global_scraper = None

def _cleanup_on_exit():
    """Ensure scraper is closed before Python exits"""
    global _global_scraper
    if _global_scraper:
        try:
            _global_scraper.close()
        except:
            pass
        _global_scraper = None
    # Force garbage collection
    for _ in range(3):
        gc.collect()

# Register cleanup handler
atexit.register(_cleanup_on_exit)


def setup_logging():
    """Setup logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger('single_site')


def load_site_config(site_name):
    """Load configuration for a specific site"""
    try:
        with open('config/sites_config.json', 'r') as f:
            config = json.load(f)
            sites = config.get('sites', [])
            
            for site in sites:
                if site.get('name') == site_name:
                    return site
            
            return None
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        return None


def create_scraper(site_config):
    """Create scraper for the site"""
    site_name = site_config.get('name', '')
    
    if site_name == 'tascaparts':
        return TascaPartsScraper()
    else:
        return GenericScraper(site_config)


def main():
    logger = setup_logging()
    
    # Get site name from command line
    if len(sys.argv) < 2:
        print("\nUsage: python run_single_site.py <site_name>")
        print("\nAvailable sites:")
        print("  - tascaparts")
        print("  - acuraparts")
        print("  - honda")
        print("  - ford")
        print("  - toyota")
        print("  - etc. (see config/sites_config.json)")
        print("\nExample:")
        print("  python run_single_site.py tascaparts")
        sys.exit(1)
    
    site_name = sys.argv[1]
    
    # Optional: limit number of products for testing
    limit = None
    if len(sys.argv) >= 3:
        try:
            limit = int(sys.argv[2])
            logger.info(f"Limiting to {limit} products")
        except:
            pass
    
    logger.info("="*70)
    logger.info(f"SINGLE SITE SCRAPER - {site_name.upper()}")
    logger.info("="*70)
    
    # Load site config
    site_config = load_site_config(site_name)
    if not site_config:
        logger.error(f"Site '{site_name}' not found in configuration")
        sys.exit(1)
    
    logger.info(f"Loaded configuration for {site_name}")
    logger.info(f"Base URL: {site_config.get('base_url')}")
    
    # Create scraper
    scraper = None
    products = []
    
    try:
        scraper = create_scraper(site_config)
        # Register for cleanup on exit
        global _global_scraper
        _global_scraper = scraper
        logger.info("Scraper initialized")
        
        # Get product URLs
        logger.info("Fetching product URLs...")
        product_urls = scraper.get_product_urls()
        logger.info(f"Found {len(product_urls)} product URLs")
        
        if not product_urls:
            logger.warning("No product URLs found")
            return
        
        # Limit for testing if specified
        if limit:
            product_urls = product_urls[:limit]
            logger.info(f"Limited to first {limit} products")
        
        # Scrape products
        logger.info(f"Scraping {len(product_urls)} products...")
        
        from tqdm import tqdm
        import time
        
        for idx, url in enumerate(tqdm(product_urls, desc=f"Scraping {site_name}"), 1):
            try:
                product_data = scraper.scrape_product(url)
                
                if product_data:
                    products.append(product_data)
                    logger.info(f"[{idx}/{len(product_urls)}] ✓ {product_data.get('title', 'Unknown')[:50]}")
                else:
                    logger.info(f"[{idx}/{len(product_urls)}] ✗ Skipped")
                
                # Polite delay between products (increased to avoid Cloudflare detection)
                if idx < len(product_urls):
                    import random
                    delay = random.uniform(3, 5)  # Increased to 3-5 seconds for more human-like behavior
                    time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error scraping {url}: {str(e)}")
                continue
        
        logger.info(f"✓ Scraped {len(products)} wheel products from {site_name}")
        
        # Process and export
        if products:
            logger.info("\nProcessing data...")
            processor = DataProcessor()
            df = processor.process_products(products)
            df = processor.clean_data(df)
            
            # Validate
            validation = processor.validate_data(df)
            logger.info(f"\nValidation:")
            logger.info(f"  Total rows: {validation['total_rows']}")
            logger.info(f"  Unique parts: {validation['unique_parts']}")
            
            # Export
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"data/processed/{site_name}_{timestamp}.xlsx"
            
            logger.info(f"\nExporting to: {output_file}")
            exporter = ExcelExporter()
            exporter.export_to_excel(df, output_file, apply_formatting=True)
            
            logger.info("\n" + "="*70)
            logger.info("COMPLETE!")
            logger.info("="*70)
            logger.info(f"✓ Products scraped: {len(products)}")
            logger.info(f"✓ Total rows: {len(df)}")
            logger.info(f"✓ Output: {output_file}")
            logger.info("="*70)
        else:
            logger.warning("No products were scraped")
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user. Exiting...")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

