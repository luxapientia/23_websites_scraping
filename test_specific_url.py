from scrapers.acurapartswarehouse_scraper import AcuraPartsWarehouseScraper
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_scraper():
    scraper = AcuraPartsWarehouseScraper()
    url = "https://www.acurapartswarehouse.com/oem/acura~wheel~al~20x9j~42800-tya-a20.html"
    
    print(f"Testing scraper with URL: {url}")
    results = scraper.scrape_product(url)
    
    if results:
        print(f"\nSuccessfully scraped {len(results)} rows")
        print("\nFirst row data:")
        for key, value in results[0].items():
            if value:  # Only show non-empty values
                print(f"  {key}: {value}")
        
        print("\n" + "="*80)
        print("Checking specific fields:")
        print(f"  Image URL: {results[0].get('image_url')}")
        print(f"  MSRP: {results[0].get('msrp')}")
        print(f"  SKU: {results[0].get('sku')}")
        print(f"  PN: {results[0].get('pn')}")
        print(f"  Description: {results[0].get('description')[:100] if results[0].get('description') else 'EMPTY'}...")
        
        # Check fitment data
        print(f"\nFitment count: {len(results)}")
        if len(results) > 0:
            print("Sample fitments:")
            for i in range(min(5, len(results))):
                r = results[i]
                print(f"  {r.get('year')} {r.get('make')} {r.get('model')} {r.get('trim')} {r.get('engine')}")
    else:
        print("Failed to scrape data - NO RESULTS RETURNED")

if __name__ == "__main__":
    test_scraper()
