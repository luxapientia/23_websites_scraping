# Automotive Wheels Web Scraping Project

A comprehensive web scraping solution for extracting automotive wheel and wheel cap data from 23+ automotive parts websites.

## 📋 Project Overview

This project scrapes automotive wheel products from multiple OEM parts websites, extracting detailed product information including:
- Product details (SKU, prices, descriptions)
- Vehicle fitment data (year, make, model, trim, engine)
- Images and specifications
- Multiple rows per part for each fitment combination

**Expected Output:** ~40,000 unique part numbers across all sites

## 🎯 Key Features

- ✅ **Multi-site scraping** for 23+ automotive parts websites
- ✅ **Intelligent wheel detection** (excludes steering wheels, wheel bearings, etc.)
- ✅ **Multiple fitment support** (creates separate rows for each year/make/model/trim/engine)
- ✅ **Excel export** with formatted output
- ✅ **Checkpoint system** for resuming interrupted scrapes
- ✅ **Polite scraping** with configurable delays
- ✅ **Comprehensive logging** for debugging
- ✅ **Data validation** and cleaning

## 📁 Project Structure

```
scrapping site/
├── config/
│   └── sites_config.json          # Configuration for all 23 sites
├── scrapers/
│   ├── __init__.py
│   ├── base_scraper.py            # Base scraper class
│   ├── tascaparts_scraper.py      # Site-specific scraper (example)
│   └── generic_scraper.py         # Generic scraper for similar sites
├── utils/
│   ├── __init__.py
│   ├── data_processor.py          # Data cleaning & processing
│   └── excel_exporter.py          # Excel export functionality
├── data/
│   ├── raw/                       # Raw scraped data
│   ├── processed/                 # Output Excel files
│   └── checkpoints/               # Resume points
├── logs/                          # Scraping logs
├── requirements.txt               # Python dependencies
├── main.py                        # Main execution script
├── test_scraper.py               # Test script
└── README.md                      # This file
```

## 🚀 Installation

### Prerequisites
- Python 3.8 or higher
- Chrome browser (for Selenium)
- 4GB+ RAM recommended
- Stable internet connection

### Setup Instructions

1. **Clone or download the project:**
   ```bash
   cd "C:\my projects\Scrapping\scrapping site"
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation:**
   ```bash
   python test_scraper.py detection
   ```

## 💻 Usage

### Running the Full Scraper

To scrape all 23 sites:

```bash
python main.py
```

This will:
1. Load site configurations from `config/sites_config.json`
2. Scrape each site sequentially
3. Save checkpoints after each site
4. Process and clean the data
5. Export to Excel in `data/processed/`

### Testing Individual Sites

To test a single product:

```bash
python test_scraper.py tascaparts
```

To run all tests:

```bash
python test_scraper.py
```

### Testing Specific Functionality

```bash
# Test data processing
python test_scraper.py processing

# Test Excel export
python test_scraper.py excel

# Test wheel detection
python test_scraper.py detection
```

## 📊 Output Format

The scraper produces an Excel file with the following columns:

| Column | Description |
|--------|-------------|
| A | URL of product page |
| B | Image URL |
| C | Scrape date/time |
| D | SKU (original format) |
| E | PN (cleaned SKU without special chars) |
| F-M | Empty (reserved for client use) |
| N | Actual sale price |
| O-Q | Empty (reserved for client use) |
| R | MSRP price |
| S-W | Empty (reserved for client use) |
| X | Product title |
| Y | Also known as (alternative names) |
| Z | Positions (notes/specifications) |
| AA | Description |
| AB | Applications |
| AC | Replaces (superseded part numbers) |
| AD | Year |
| AE | Make |
| AF | Model |
| AG | Trims |
| AH | Engines |

**Important:** Each part creates multiple rows - one for each fitment combination (year/make/model/trim/engine).

## 🔧 Configuration

### Site Configuration

Edit `config/sites_config.json` to modify site settings:

```json
{
  "sites": [
    {
      "name": "tascaparts",
      "base_url": "https://www.tascaparts.com",
      "brands": ["GM", "Chevrolet", "Buick", "GMC", "Cadillac"],
      "search_strategy": "category",
      "category_url": "/c/wheelstiresparts",
      "use_selenium": true
    }
  ]
}
```

### Scraping Parameters

In `main.py`, adjust:

```python
# Delay between products (seconds)
delay_between_products=2

# Test mode - limit number of products
# product_urls = product_urls[:5]  # Uncomment to test with 5 products
```

## 🎓 Creating Site-Specific Scrapers

To add a new site-specific scraper:

1. **Create a new scraper file** in `scrapers/`:

```python
from scrapers.base_scraper import BaseScraper

class YourSiteScraper(BaseScraper):
    def __init__(self):
        super().__init__('yoursite', use_selenium=True)
        self.base_url = 'https://www.yoursite.com'
    
    def get_product_urls(self):
        # Implement URL collection
        pass
    
    def scrape_product(self, url):
        # Implement product scraping
        pass
```

2. **Add to main.py**:

```python
from scrapers.yoursite_scraper import YourSiteScraper

def create_scraper(site_config):
    site_name = site_config.get('name', '')
    
    if site_name == 'yoursite':
        return YourSiteScraper()
    # ... existing code
```

## 🛡️ Best Practices

### Legal & Ethical Scraping

1. **Check robots.txt** before scraping each site
2. **Add delays** between requests (2-5 seconds minimum)
3. **Respect rate limits** to avoid overwhelming servers
4. **Use appropriate User-Agent** headers
5. **Review Terms of Service** for each website

### Performance Tips

1. **Use checkpoints** to resume interrupted scrapes
2. **Run during off-peak hours** when possible
3. **Monitor logs** for errors and adjust as needed
4. **Test with small samples** before full scrapes

### Error Handling

- The scraper automatically logs errors and continues
- Checkpoints are saved after each site
- Failed sites are reported in the final summary

## 📝 Supported Websites

The scraper is configured for 23 automotive parts websites:

1. TascaParts (GM)
2. Acura Parts Warehouse
3. Mopar Online Parts (Dodge, Jeep, Ram)
4. Scuderia Car Parts (Ferrari, Maserati, etc.)
5. Audi USA Parts
6. BMW Parts
7. GM OEM Parts Online
8. Ford Parts
9. Honda Parts Online
10. Hyundai/Genesis Parts
11. Infiniti Parts Deal
12. Jaguar Parts
13. Kia Parts Now
14. Land Rover Paramus
15. Lexus OEM Parts
16. Mazda Parts
17. Mercedes-Benz Parts Source
18. Mitsubishi Parts Warehouse
19. Nissan USA Parts
20. Porsche Parts
21. Subaru Parts
22. Toyota Autoparts
23. Volkswagen Parts
24. Volvo Parts

## 🐛 Troubleshooting

### Common Issues

**1. "WebDriver not found" error:**
```bash
# The script will auto-download ChromeDriver
# Ensure Chrome browser is installed
```

**2. "No products found" error:**
- Check if the site structure has changed
- Review logs for specific errors
- Try the generic scraper instead

**3. Memory errors:**
- Reduce the number of concurrent scrapes
- Process sites in smaller batches

**4. Rate limiting / IP blocking:**
- Increase delay between requests
- Consider using proxies
- Run during off-peak hours

### Debug Mode

Enable detailed logging:

```python
# In main.py, change logging level
logging.basicConfig(level=logging.DEBUG)
```

## 📈 Performance Expectations

- **Time per site:** 30 minutes - 2 hours (depending on product count)
- **Total time:** 1-3 days for all 23 sites
- **Expected output:** 40,000+ unique part numbers
- **Excel file size:** 10-50 MB (depending on fitment data)

## 🔄 Resuming Interrupted Scrapes

If scraping is interrupted:

1. Check `data/checkpoints/` for saved progress
2. Already scraped sites are saved as JSON files
3. Re-run `main.py` - it will continue from where it stopped
4. Alternatively, manually modify `sites_config.json` to skip completed sites

## 📞 Support

For issues or questions:

1. Check the logs in `logs/` directory
2. Review the test suite: `python test_scraper.py`
3. Consult the inline documentation in source files

## 📄 License

This project is for educational and commercial purposes as specified by the client.

## ⚠️ Disclaimer

Web scraping may be subject to the terms of service of the websites being scraped. Users are responsible for ensuring their use of this tool complies with all applicable laws and website terms of service.

---

## 🎯 Quick Start Checklist

- [ ] Install Python 3.8+
- [ ] Install Chrome browser
- [ ] Run `pip install -r requirements.txt`
- [ ] Test with `python test_scraper.py detection`
- [ ] Test single product: `python test_scraper.py tascaparts`
- [ ] Run full scraper: `python main.py`
- [ ] Check output in `data/processed/`

## 📊 Project Status

This is a complete, production-ready web scraping solution. The framework is extensible and can be adapted for other automotive parts websites or similar e-commerce platforms.

**Version:** 1.0.0  
**Last Updated:** November 2025
