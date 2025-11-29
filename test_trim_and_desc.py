import logging
import sys
from scrapers.acurapartswarehouse_scraper import AcuraPartsWarehouseScraper

# Setup logging - avoid Unicode issues on Windows
logging.basicConfig(
    level=logging.WARNING,  # Reduce logging to avoid emoji issues
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def test_trim_splitting():
    """Test URL with multiple comma-separated trims"""
    url = "https://www.acurapartswarehouse.com/oem/acura~wheel~19~alloy~08w19-tgv-200.html"
    scraper = AcuraPartsWarehouseScraper()
    
    print("\n" + "="*80)
    print(f"Testing TRIM SPLITTING for: {url}")
    print("="*80 + "\n")
    
    try:
        products = scraper.scrape_product(url)
        
        if not products:
            print("[ERROR] No products extracted!")
            return

        print(f"[SUCCESS] Extracted {len(products)} rows")
        
        # Check for 2021 TLX entries
        tlx_2021_rows = [p for p in products if p.get('year') == '2021' and p.get('model') == 'TLX']
        print(f"\n[INFO] Found {len(tlx_2021_rows)} rows for 2021 TLX")
        
        if tlx_2021_rows:
            print("\nSample 2021 TLX rows (each trim should be separate):")
            for i, row in enumerate(tlx_2021_rows, 1):
                print(f"  {i}. Trim: '{row.get('trim')}'")
        
        # Check description
        if products:
            desc = products[0].get('description', '')
            print(f"\n[INFO] Description ({len(desc)} chars): {desc[:150]}...")
            
        # Show first few rows
        print(f"\n[INFO] First 3 rows:")
        for i, p in enumerate(products[:3], 1):
            print(f"\n--- Row {i} ---")
            print(f"  Year: {p.get('year')}")
            print(f"  Make: {p.get('make')}")
            print(f"  Model: {p.get('model')}")
            print(f"  Trim: {p.get('trim')}")
            print(f"  Image: {p.get('image_url')[:60]}..." if p.get('image_url') else "  Image: N/A")
            print(f"  Price: ${p.get('actual_price')}")
            print(f"  SKU: {p.get('sku')}")
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper.driver:
            scraper.driver.quit()

def test_description_fallback():
    """Test URL that might not have description label"""
    url = "https://www.acurapartswarehouse.com/oem/acura~cap~assy~wheel~ct~44732-tz3-a10.html"
    scraper = AcuraPartsWarehouseScraper()
    
    print("\n" + "="*80)
    print(f"Testing DESCRIPTION FALLBACK for: {url}")
    print("="*80 + "\n")
    
    try:
        products = scraper.scrape_product(url)
        
        if not products:
            print("[ERROR] No products extracted!")
            return

        print(f"[SUCCESS] Extracted {len(products)} rows")
        
        if products:
            desc = products[0].get('description', '')
            print(f"\n[INFO] Description ({len(desc)} chars):")
            print(f"   {desc}")
            
            if desc:
                print("\n[SUCCESS] Description successfully extracted!")
            else:
                print("\n[WARNING] Description is empty!")
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper.driver:
            scraper.driver.quit()

if __name__ == "__main__":
    test_trim_splitting()
    print("\n" + "="*80 + "\n")
    test_description_fallback()
