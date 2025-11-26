# -*- coding: utf-8 -*-
"""Test script for individual scraper functionality"""
import logging
import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

from scrapers.tascaparts_scraper import TascaPartsScraper
from scrapers.generic_scraper import GenericScraper
from utils.data_processor import DataProcessor
from utils.excel_exporter import ExcelExporter


def setup_test_logging():
    """Setup logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )


def test_tascaparts_single_product():
    """Test scraping a single product from TascaParts"""
    print("\n" + "="*70)
    print("TEST: TascaParts Single Product")
    print("="*70)
    
    scraper = TascaPartsScraper()
    
    # Test URL from the example provided
    test_url = "https://www.tascaparts.com/oem-parts/gm-24-x-9-inch-aluminum-6-split-spoke-wheel-86527277"
    
    print(f"\nScraping: {test_url}")
    
    try:
        product = scraper.scrape_product(test_url)
        
        if product:
            print("\n✓ Product scraped successfully!")
            print(f"\nProduct Details:")
            print(f"  Title: {product['title']}")
            print(f"  SKU: {product['sku']}")
            print(f"  PN (cleaned): {product['pn']}")
            print(f"  Actual Price: ${product['actual_price']}")
            print(f"  MSRP: ${product['msrp']}")
            print(f"  Image URL: {product['image_url'][:50]}...")
            print(f"  Fitments: {len(product['fitments'])} combinations")
            
            if product['fitments']:
                print(f"\n  First Fitment:")
                first_fit = product['fitments'][0]
                print(f"    Year: {first_fit['year']}")
                print(f"    Make: {first_fit['make']}")
                print(f"    Model: {first_fit['model']}")
                print(f"    Trim: {first_fit['trim']}")
                print(f"    Engine: {first_fit['engine']}")
            
            # Validate expected values
            assert product['sku'] == '86527277', "SKU mismatch"
            assert product['pn'] == '86527277', "PN mismatch"
            assert product['actual_price'] == '787.31', "Price mismatch"
            assert len(product['fitments']) > 0, "No fitments found"
            
            print("\n✓ All assertions passed!")
            return True
        else:
            print("\n✗ Failed to scrape product")
            return False
            
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        return False
    finally:
        scraper.close()


def test_data_processing():
    """Test data processing with sample data"""
    print("\n" + "="*70)
    print("TEST: Data Processing")
    print("="*70)
    
    # Sample product data
    sample_products = [
        {
            'url': 'https://example.com/wheel1',
            'image_url': 'https://example.com/images/wheel1.jpg',
            'date': '2025-01-01 12:00:00',
            'sku': 'WHE-123-ABC',
            'pn': 'WHE123ABC',
            'actual_price': '299.99',
            'msrp': '399.99',
            'title': 'Test Wheel 17 inch',
            'also_known_as': 'Alternative Name',
            'positions': 'Front/Rear',
            'description': 'Test wheel description',
            'applications': 'Test applications',
            'replaces': 'OLD-PART-123',
            'fitments': [
                {'year': '2020', 'make': 'Honda', 'model': 'Accord', 'trim': 'EX', 'engine': '2.0L'},
                {'year': '2021', 'make': 'Honda', 'model': 'Accord', 'trim': 'EX', 'engine': '2.0L'},
            ]
        },
        {
            'url': 'https://example.com/wheel2',
            'image_url': 'https://example.com/images/wheel2.jpg',
            'date': '2025-01-01 12:05:00',
            'sku': 'WHE-456-DEF',
            'pn': 'WHE456DEF',
            'actual_price': '450.00',
            'msrp': '550.00',
            'title': 'Premium Alloy Wheel 18 inch',
            'also_known_as': '',
            'positions': '',
            'description': 'Premium wheel description',
            'applications': '',
            'replaces': '',
            'fitments': [
                {'year': '2022', 'make': 'Toyota', 'model': 'Camry', 'trim': 'XLE', 'engine': '2.5L'},
            ]
        }
    ]
    
    try:
        processor = DataProcessor()
        
        print("\nProcessing sample products...")
        df = processor.process_products(sample_products)
        
        print(f"\n✓ Processed {len(df)} rows")
        print(f"  Unique parts: {df['PN'].nunique()}")  # Using Excel header 'PN'
        
        # Validate
        assert len(df) == 3, "Expected 3 rows (2 fitments for first product, 1 for second)"
        assert df['PN'].nunique() == 2, "Expected 2 unique part numbers"  # Using Excel header 'PN'
        
        # Validate data
        validation = processor.validate_data(df)
        print(f"\nValidation Report:")
        for key, value in validation.items():
            print(f"  {key}: {value}")
        
        print("\n✓ Data processing test passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        return False


def test_excel_export():
    """Test Excel export functionality"""
    print("\n" + "="*70)
    print("TEST: Excel Export")
    print("="*70)
    
    # Sample DataFrame
    import pandas as pd
    
    sample_data = {
        'url': ['https://example.com/wheel1'],
        'image_url': ['https://example.com/images/wheel1.jpg'],
        'date': ['2025-01-01 12:00:00'],
        'sku': ['WHE-123-ABC'],
        'pn': ['WHE123ABC'],
        'col_f': [''], 'col_g': [''], 'col_h': [''], 'col_i': [''],
        'col_j': [''], 'col_k': [''], 'col_l': [''], 'col_m': [''],
        'actual_price': ['299.99'],
        'col_o': [''], 'col_p': [''], 'col_q': [''],
        'msrp': ['399.99'],
        'col_s': [''], 'col_t': [''], 'col_u': [''], 'col_v': [''], 'col_w': [''],
        'title': ['Test Wheel 17 inch'],
        'also_known_as': ['Alternative Name'],
        'positions': ['Front/Rear'],
        'description': ['Test wheel description'],
        'applications': ['Test applications'],
        'replaces': ['OLD-PART-123'],
        'year': ['2020'],
        'make': ['Honda'],
        'model': ['Accord'],
        'trims': ['EX'],
        'engines': ['2.0L']
    }
    
    df = pd.DataFrame(sample_data)
    
    try:
        exporter = ExcelExporter()
        
        test_file = 'data/test_output.xlsx'
        print(f"\nExporting to: {test_file}")
        
        exporter.export_to_excel(df, test_file, apply_formatting=True)
        
        # Check if file was created
        import os
        if os.path.exists(test_file):
            file_size = os.path.getsize(test_file)
            print(f"✓ File created successfully ({file_size} bytes)")
            
            # Clean up test file
            os.remove(test_file)
            print("✓ Test file cleaned up")
            
            print("\n✓ Excel export test passed!")
            return True
        else:
            print("\n✗ File was not created")
            return False
            
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        return False


def test_wheel_detection():
    """Test wheel product detection"""
    print("\n" + "="*70)
    print("TEST: Wheel Product Detection")
    print("="*70)
    
    from scrapers.base_scraper import BaseScraper
    
    class TestScraper(BaseScraper):
        def get_product_urls(self):
            return []
        def scrape_product(self, url):
            return None
    
    scraper = TestScraper('test', use_selenium=False)
    
    test_cases = [
        # Should be detected as wheels
        ("18 inch Alloy Wheel", True),
        ("Aluminum Wheel 17x8", True),
        ("Chrome Rim 20 inch", True),
        ("Wheel Cap Center Hub", True),
        ("Spoke Wheel Design", True),
        
        # Should NOT be detected as wheels
        ("Steering Wheel Cover", False),
        ("Wheel Bearing Assembly", False),
        ("Wheel Hub Bolt Set", False),
        ("Tire Pressure Sensor TPMS", False),
        ("Lug Nut Set", False),
        ("Wheel Speed Sensor", False),
    ]
    
    all_passed = True
    
    for title, expected in test_cases:
        result = scraper.is_wheel_product(title)
        status = "✓" if result == expected else "✗"
        
        if result != expected:
            all_passed = False
        
        print(f"{status} '{title}' -> {result} (expected {expected})")
    
    if all_passed:
        print("\n✓ All wheel detection tests passed!")
        return True
    else:
        print("\n✗ Some tests failed")
        return False


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print("RUNNING ALL TESTS")
    print("="*70)
    
    tests = [
        ("Wheel Detection", test_wheel_detection),
        ("Data Processing", test_data_processing),
        ("Excel Export", test_excel_export),
        ("TascaParts Single Product", test_tascaparts_single_product),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n✗ {test_name} failed with exception: {str(e)}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ ALL TESTS PASSED!")
        return True
    else:
        print(f"\n✗ {total - passed} test(s) failed")
        return False


if __name__ == "__main__":
    setup_test_logging()
    
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        
        if test_name == "tascaparts":
            test_tascaparts_single_product()
        elif test_name == "processing":
            test_data_processing()
        elif test_name == "excel":
            test_excel_export()
        elif test_name == "detection":
            test_wheel_detection()
        else:
            print(f"Unknown test: {test_name}")
            print("Available tests: tascaparts, processing, excel, detection")
    else:
        # Run all tests
        success = run_all_tests()
        sys.exit(0 if success else 1)

