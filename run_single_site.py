"""Run scraper for a single site (for testing)"""
import sys
import json
import logging
import atexit
import gc
from datetime import datetime

from scrapers.audiusa_scraper import AudiUSAScraper
from scrapers.bmw_scraper import BMWScraper
from scrapers.gm_oemparts_scraper import GMOemPartsScraper
from scrapers.ford_scraper import FordScraper
from scrapers.honda_scraper import HondaScraper
from scrapers.hyundai_scraper import HyundaiScraper
from scrapers.infiniti_scraper import InfinitiScraper
from scrapers.jaguar_scraper import JaguarScraper
from scrapers.kia_scraper import KiaScraper
from scrapers.landrover_scraper import LandRoverScraper
from scrapers.lexus_scraper import LexusScraper
from scrapers.mazda_scraper import MazdaScraper
from scrapers.mercedes_scraper import MercedesScraper
from scrapers.mitsubishi_scraper import MitsubishiScraper
from scrapers.nissan_scraper import NissanScraper
from scrapers.porsche_scraper import PorscheScraper
from scrapers.subaru_scraper import SubaruScraper
from scrapers.toyota_scraper import ToyotaScraper
from scrapers.volkswagen_scraper import VolkswagenScraper
from scrapers.volvo_scraper import VolvoScraper
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
    elif site_name == 'acuraparts':
        return AcuraPartsWarehouseScraper()
    elif site_name == 'moparonline':
        return MoparOnlinePartsScraper()
    elif site_name == 'scuderiacarparts':
        return ScuderiaCarPartsScraper()
    elif site_name == 'audiusa':
        return AudiUSAScraper()
    elif site_name == 'bmw':
        return BMWScraper()
    elif site_name == 'gm_oemparts':
        return GMOemPartsScraper()
    elif site_name == 'ford':
        return FordScraper()
    elif site_name == 'honda':
        return HondaScraper()
    elif site_name == 'hyundai':
        return HyundaiScraper()
    elif site_name == 'infiniti':
        return InfinitiScraper()
    elif site_name == 'jaguar':
        return JaguarScraper()
    elif site_name == 'kia':
        return KiaScraper()
    elif site_name == 'landrover':
        return LandRoverScraper()
    elif site_name == 'lexus':
        return LexusScraper()
    elif site_name == 'mazda':
        return MazdaScraper()
    elif site_name == 'mercedes':
        return MercedesScraper()
    elif site_name == 'mitsubishi':
        return MitsubishiScraper()
    elif site_name == 'nissan':
        return NissanScraper()
    elif site_name == 'porsche':
        return PorscheScraper()
    elif site_name == 'subaru':
        return SubaruScraper()
    elif site_name == 'toyota':
        return ToyotaScraper()
    elif site_name == 'volkswagen':
        return VolkswagenScraper()
    elif site_name == 'volvo':
        return VolvoScraper()
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
        print("  - moparonline")
        print("  - scuderiacarparts")
        print("  - audiusa")
        print("  - bmw")
        print("  - gm_oemparts")
        print("  - ford")
        print("  - honda")
        print("  - hyundai")
        print("  - infiniti")
        print("  - jaguar")
        print("  - kia")
        print("  - landrover")
        print("  - lexus")
        print("  - mazda")
        print("  - mercedes")
        print("  - mitsubishi")
        print("  - nissan")
        print("  - porsche")
        print("  - subaru")
        print("  - toyota")
        print("  - volkswagen")
        print("  - volvo")
        print("  - etc. (see config/sites_config.json)")
        print("\nExample:")
        print("  python run_single_site.py tascaparts")
        print("  python run_single_site.py acuraparts")
        print("  python run_single_site.py moparonline")
        print("  python run_single_site.py scuderiacarparts")
        print("  python run_single_site.py audiusa")
        print("  python run_single_site.py bmw")
        print("  python run_single_site.py toyota")
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
        
        for idx, url in enumerate(tqdm(product_urls, desc=f"Scraping {site_name}"), start=1):
            try:
                product_data = scraper.scrape_product(url)
                
                if product_data:
                    if isinstance(product_data, list):
                        products.extend(product_data)
                        title = product_data[0].get('title', 'Unknown') if product_data else 'Unknown'
                    else:
                        products.append(product_data)
                        title = product_data.get('title', 'Unknown')
                    logger.info(f"[{idx}/{len(product_urls)}] ✓ {title[:50]}")
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

