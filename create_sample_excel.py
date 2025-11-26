# -*- coding: utf-8 -*-
"""Create a sample Excel file with scraped products"""
import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

from scrapers.tascaparts_scraper import TascaPartsScraper
from utils.data_processor import DataProcessor
from utils.excel_exporter import ExcelExporter

def main():
    """Scrape a few known good products and create Excel"""
    print("\n" + "="*70)
    print("CREATING SAMPLE EXCEL FILE")
    print("="*70)
    
    # Known working URLs (actual wheels, not accessories)
    test_urls = [
        "https://www.tascaparts.com/oem-parts/gm-24-x-9-inch-aluminum-6-split-spoke-wheel-86527277",
        "https://www.tascaparts.com/oem-parts/ford-aluminum-wheel-gl3z1015c",
        "https://www.tascaparts.com/oem-parts/ford-wheel-dl3z1015b",
    ]
    
    scraper = TascaPartsScraper()
    products = []
    
    print(f"\nScraping {len(test_urls)} products...")
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n[{i}/{len(test_urls)}] Scraping: {url[:80]}...")
        try:
            product = scraper.scrape_product(url)
            if product:
                products.append(product)
                print(f"  ✓ {product['title'][:60]}...")
                print(f"    SKU: {product['sku']}, Price: ${product['actual_price']}, Fitments: {len(product['fitments'])}")
            else:
                print(f"  ✗ Failed to scrape")
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
    
    scraper.close()
    
    if not products:
        print("\n✗ No products scraped!")
        return
    
    print(f"\n{'='*70}")
    print(f"Processing {len(products)} products...")
    print(f"{'='*70}")
    
    processor = DataProcessor()
    df = processor.process_products(products)
    
    print(f"\n✓ Total rows (with fitments): {len(df)}")
    print(f"✓ Unique part numbers: {df['PN'].nunique()}")  # Using Excel header 'PN'
    
    # Create output directory
    os.makedirs('data/processed', exist_ok=True)
    
    # Export to Excel
    from datetime import datetime
    output_file = f"data/processed/sample_wheels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    print(f"\nExporting to: {output_file}")
    exporter = ExcelExporter()
    exporter.export_to_excel(df, output_file)
    
    print(f"\n{'='*70}")
    print(f"✓ SAMPLE EXCEL CREATED!")
    print(f"✓ File: {output_file}")
    print(f"✓ Products: {len(products)}")
    print(f"✓ Total rows: {len(df)}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()

