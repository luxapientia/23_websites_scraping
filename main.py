"""Main execution script for automotive wheels scraping project"""
import os
import json
import logging
from datetime import datetime
from tqdm import tqdm
import time

# Import scrapers
from scrapers.tascaparts_scraper import TascaPartsScraper
from scrapers.generic_scraper import GenericScraper

# Import utilities
from utils.data_processor import DataProcessor
from utils.excel_exporter import ExcelExporter


def setup_logging():
    """Setup logging configuration"""
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/main_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('main')


def load_site_configs():
    """Load site configurations from JSON file"""
    try:
        with open('config/sites_config.json', 'r') as f:
            config = json.load(f)
            return config.get('sites', [])
    except Exception as e:
        logging.error(f"Error loading site configs: {str(e)}")
        return []


def create_scraper(site_config):
    """
    Create appropriate scraper for the site
    
    Args:
        site_config: Site configuration dictionary
    
    Returns:
        Scraper instance
    """
    site_name = site_config.get('name', '')
    
    # Use specific scrapers if available
    if site_name == 'tascaparts':
        return TascaPartsScraper()
    # Add more specific scrapers here as implemented
    # elif site_name == 'acuraparts':
    #     return AcuraPartsScraper()
    else:
        # Use generic scraper for most sites
        return GenericScraper(site_config)


def scrape_site(site_config, logger, delay_between_products=3):
    """
    Scrape a single site
    
    Args:
        site_config: Site configuration dictionary
        logger: Logger instance
        delay_between_products: Delay in seconds between scraping products
    
    Returns:
        list: List of product data dictionaries
    """
    site_name = site_config.get('name', 'unknown')
    products = []
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Starting scrape of {site_name}")
    logger.info(f"{'='*70}")
    
    scraper = None
    
    try:
        # Create scraper
        scraper = create_scraper(site_config)
        logger.info(f"Scraper initialized for {site_name}")
        
        # Get product URLs
        logger.info("Fetching product URLs...")
        product_urls = scraper.get_product_urls()
        logger.info(f"Found {len(product_urls)} product URLs")
        
        if not product_urls:
            logger.warning(f"No product URLs found for {site_name}")
            return products
        
        # Limit for testing (remove in production)
        # Uncomment the line below to test with fewer products
        # product_urls = product_urls[:5]  # Test with first 5 products
        
        # Scrape each product
        logger.info(f"Scraping {len(product_urls)} products...")
        
        for idx, url in enumerate(tqdm(product_urls, desc=f"Scraping {site_name}"), 1):
            try:
                product_data = scraper.scrape_product(url)
                
                if product_data:
                    products.append(product_data)
                    logger.info(f"[{idx}/{len(product_urls)}] ✓ {product_data.get('title', 'Unknown')[:50]}")
                else:
                    logger.info(f"[{idx}/{len(product_urls)}] ✗ Skipped (not a wheel or error)")
                
                # Delay between requests to be polite
                if idx < len(product_urls):
                    time.sleep(delay_between_products)
                
            except Exception as e:
                logger.error(f"Error scraping {url}: {str(e)}")
                continue
        
        logger.info(f"✓ Completed {site_name}: {len(products)} wheel products scraped")
        
    except Exception as e:
        logger.error(f"Error scraping site {site_name}: {str(e)}")
    
    finally:
        if scraper:
            scraper.close()
    
    return products


def save_checkpoint(products, filename):
    """Save checkpoint of scraped data"""
    try:
        os.makedirs('data/checkpoints', exist_ok=True)
        checkpoint_file = f'data/checkpoints/{filename}'
        
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Checkpoint saved: {checkpoint_file}")
    except Exception as e:
        logging.error(f"Error saving checkpoint: {str(e)}")


def main():
    """Main execution function"""
    
    # Setup
    logger = setup_logging()
    logger.info("="*70)
    logger.info("AUTOMOTIVE WHEELS WEB SCRAPING PROJECT")
    logger.info("="*70)
    
    # Create directories
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    # Load site configurations
    logger.info("\nLoading site configurations...")
    site_configs = load_site_configs()
    logger.info(f"Loaded {len(site_configs)} site configurations")
    
    if not site_configs:
        logger.error("No site configurations found. Exiting.")
        return
    
    # Scrape all sites
    all_products = []
    successful_sites = 0
    failed_sites = []
    
    for idx, site_config in enumerate(site_configs, 1):
        site_name = site_config.get('name', f'site_{idx}')
        
        logger.info(f"\n[{idx}/{len(site_configs)}] Processing {site_name}...")
        
        try:
            products = scrape_site(site_config, logger, delay_between_products=2)
            
            if products:
                all_products.extend(products)
                successful_sites += 1
                
                # Save checkpoint after each site
                checkpoint_name = f'{site_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                save_checkpoint(products, checkpoint_name)
            else:
                failed_sites.append(site_name)
            
        except Exception as e:
            logger.error(f"Failed to scrape {site_name}: {str(e)}")
            failed_sites.append(site_name)
            continue
    
    # Process and export data
    logger.info(f"\n{'='*70}")
    logger.info("DATA PROCESSING AND EXPORT")
    logger.info(f"{'='*70}")
    logger.info(f"Total products scraped: {len(all_products)}")
    logger.info(f"Successful sites: {successful_sites}/{len(site_configs)}")
    
    if failed_sites:
        logger.warning(f"Failed sites: {', '.join(failed_sites)}")
    
    if not all_products:
        logger.error("No products scraped. Exiting.")
        return
    
    # Process data
    processor = DataProcessor()
    logger.info("\nProcessing scraped data...")
    df = processor.process_products(all_products)
    
    # Clean data
    df = processor.clean_data(df)
    
    # Validate data
    logger.info("\nValidating data...")
    validation_report = processor.validate_data(df)
    
    # Get summary statistics
    logger.info("\nGenerating summary statistics...")
    stats = processor.get_summary_statistics(df)
    
    logger.info("\nSummary Statistics:")
    logger.info(f"  Total rows: {stats['total_rows']}")
    logger.info(f"  Unique part numbers: {stats['unique_parts']}")
    logger.info(f"  Average price: ${stats['average_price']}")
    logger.info(f"  Price range: ${stats['price_range']['min']} - ${stats['price_range']['max']}")
    
    if stats['products_by_make']:
        logger.info("\n  Products by make:")
        for make, count in list(stats['products_by_make'].items())[:10]:
            logger.info(f"    {make}: {count}")
    
    # Export to Excel
    exporter = ExcelExporter()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"data/processed/wheels_data_{timestamp}.xlsx"
    
    logger.info(f"\nExporting to Excel: {output_file}")
    exporter.export_to_excel(df, output_file, apply_formatting=True)
    
    # Export summary
    summary_file = f"data/processed/summary_{timestamp}.xlsx"
    exporter.export_summary(stats, summary_file)
    
    # Optionally split by site
    # split_dir = f"data/processed/by_site_{timestamp}"
    # logger.info(f"\nSplitting data by site into: {split_dir}")
    # exporter.split_by_site(df.copy(), split_dir)
    
    # Final summary
    logger.info(f"\n{'='*70}")
    logger.info("SCRAPING COMPLETE!")
    logger.info(f"{'='*70}")
    logger.info(f"✓ Total products scraped: {len(all_products)}")
    logger.info(f"✓ Total rows in Excel: {len(df)}")
    logger.info(f"✓ Unique part numbers: {df['PN'].nunique()}")  # Using Excel header 'PN'
    logger.info(f"✓ Output file: {output_file}")
    logger.info(f"✓ File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user. Exiting...")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

