
from scrapers.acurapartswarehouse_scraper import AcuraPartsWarehouseScraper
import json
import time

def test_original_url():
    scraper = AcuraPartsWarehouseScraper()
    url = "https://www.acurapartswarehouse.com/oem/acura~alloy~wheel~18~sbc~08w18-tk4-202.html"
    
    print(f"Testing URL: {url}")
    results = scraper.scrape_product(url)
    
    if not results:
        print("No results found!")
        return
        
    print(f"Found {len(results)} results")
    
    # Check first result
    first = results[0]
    print("\nFirst result details:")
    for k, v in first.items():
        if k != 'fitment':
            print(f"  {k}: {v}")
            
    print("\n" + "="*80)
    print("Checking specific fields:")
    print(f"  Image URL: {first.get('image_url')}")
    print(f"  MSRP: {first.get('msrp')}")
    print(f"  SKU: {first.get('sku')}")
    print(f"  PN: {first.get('pn')}")
    print(f"  Description: {first.get('description')[:50]}...")
    
    print(f"\nFitment count: {len(results)}")
    print("Sample fitments:")
    for r in results[:5]:
        print(f"  {r.get('year')} {r.get('make')} {r.get('model')} {r.get('trim')} {r.get('engine')}")

if __name__ == "__main__":
    test_original_url()
