# Quick Start Guide

## ⚡ Get Started in 5 Minutes

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- requests (HTTP requests)
- beautifulsoup4 (HTML parsing)
- selenium (Browser automation)
- pandas (Data processing)
- openpyxl (Excel export)
- And other required packages

### Step 2: Test the Installation

Run a simple test to verify everything works:

```bash
python test_scraper.py detection
```

You should see:
```
✓ All wheel detection tests passed!
```

### Step 3: Test Single Product Scraping

Test scraping one product from TascaParts:

```bash
python test_scraper.py tascaparts
```

Expected output:
```
✓ Product scraped successfully!
Product Details:
  Title: 24 X 9-Inch Aluminum 6-Split-Spoke Wheel
  SKU: 86527277
  Actual Price: $787.31
  MSRP: $950.00
  Fitments: 4 combinations
```

### Step 4: Run Full Scraper (Optional - Test Mode)

Before running the full scraper on all sites, test with a limited number of products:

1. Open `main.py`
2. Find this line (around line 132):
   ```python
   # product_urls = product_urls[:5]  # Test with first 5 products
   ```
3. Uncomment it:
   ```python
   product_urls = product_urls[:5]  # Test with first 5 products
   ```
4. Run:
   ```bash
   python main.py
   ```

This will test the entire pipeline with only 5 products per site.

### Step 5: Run Full Production Scrape

Once testing is successful, comment out the test limit and run:

```bash
python main.py
```

⏱️ **Expected Time:** 1-3 days for all 23 sites

## 📊 Monitoring Progress

### During Scraping

Watch the console output for progress:
```
[1/23] Processing tascaparts...
Found 250 product URLs
Scraping tascaparts: 100%|████████████| 250/250 [1:23:45<00:00,  3.0 products/min]
✓ Completed tascaparts: 245 wheel products scraped
```

### Check Logs

Real-time logs are saved in `logs/` directory:
```bash
tail -f logs/main_20251117_120000.log
```

### Check Checkpoints

After each site completes, a checkpoint is saved:
```
data/checkpoints/tascaparts_20251117_130000.json
```

## 📁 Output Files

Results are saved in `data/processed/`:

```
data/processed/
├── wheels_data_20251117_150000.xlsx    # Main output
└── summary_20251117_150000.xlsx        # Statistics
```

## 🎯 Understanding the Output

Open the Excel file to see:

- **Column A-E:** Basic product info (URL, image, date, SKU, cleaned SKU)
- **Column F-M:** Empty (for your use)
- **Column N:** Actual price
- **Column O-Q:** Empty (for your use)
- **Column R:** MSRP price
- **Column S-W:** Empty (for your use)
- **Column X-AH:** Product details and fitment data

**Important:** Each part number appears on multiple rows - one row per fitment combination (year/make/model/trim/engine).

Example:
```
SKU      | Year | Make  | Model  | Trim | Engine
86527277 | 2025 | Cadillac | ESCALADE IQ | Luxury 1 | Electric
86527277 | 2025 | Cadillac | ESCALADE IQ | Luxury 2 | Electric
86527277 | 2025 | Cadillac | ESCALADE IQ | Sport 1  | Electric
86527277 | 2025 | Cadillac | ESCALADE IQ | Sport 2  | Electric
```

## 🔧 Configuration

### Adjust Scraping Speed

In `main.py`, find:

```python
delay_between_products=2  # Delay in seconds
```

Change to:
- `1` for faster (but riskier - may get blocked)
- `3-5` for safer (more polite to servers)

### Select Specific Sites

Edit `config/sites_config.json` to enable/disable sites:

```json
{
  "sites": [
    {
      "name": "tascaparts",
      "enabled": true,  // Add this field
      ...
    }
  ]
}
```

## 🐛 Common Issues & Solutions

### Issue: "ChromeDriver not found"

**Solution:** The script auto-downloads ChromeDriver. Ensure Chrome browser is installed.

### Issue: "No products found"

**Solution:** 
1. Check if the website is accessible
2. Review logs for specific errors
3. The site structure may have changed - update the scraper

### Issue: Scraping is slow

**Solution:**
1. This is expected - be patient
2. Don't reduce delays too much (risk of IP ban)
3. Run overnight for best results

### Issue: Script crashes mid-way

**Solution:**
1. Check `data/checkpoints/` for saved progress
2. Re-run `main.py` - it will resume
3. Check logs for the error cause

## 📈 Expected Results

After scraping all 23 sites, you should have:

- ✅ 40,000+ unique part numbers
- ✅ 100,000+ rows (due to multiple fitments per part)
- ✅ Excel file: 10-50 MB
- ✅ Complete vehicle fitment data

## 🎓 Next Steps

1. **Review the output Excel file**
2. **Validate the data quality**
3. **Check for any missing sites** (see summary)
4. **Re-run failed sites** if any
5. **Process the data** according to your needs

## 💡 Tips for Success

1. **Run during off-peak hours** (late night/early morning)
2. **Use a stable internet connection**
3. **Don't close the terminal** while scraping
4. **Monitor the first few sites** to ensure everything works
5. **Keep the logs** for troubleshooting

## 📞 Need Help?

1. Check `README.md` for detailed documentation
2. Review logs in `logs/` directory
3. Run tests: `python test_scraper.py`
4. Check the source code - it's well-commented

---

**Ready to start?** Run this command:

```bash
python main.py
```

Good luck! 🚀

