# Installation Guide - Windows

## Step 1: Install Python

### Download Python

1. **Go to the official Python website:**
   - Visit: https://www.python.org/downloads/
   - Or direct link: https://www.python.org/ftp/python/3.11.7/python-3.11.7-amd64.exe

2. **Download Python 3.11 or higher** (recommended: Python 3.11.7)
   - Click the big yellow "Download Python" button
   - Save the installer file

### Install Python

1. **Run the installer** (double-click the downloaded file)

2. **IMPORTANT: Check these boxes:**
   - ✅ **"Add python.exe to PATH"** (VERY IMPORTANT!)
   - ✅ "Install launcher for all users"

3. **Click "Install Now"**
   - Wait for installation to complete (2-3 minutes)
   - You may need administrator privileges

4. **Click "Close"** when done

### Verify Python Installation

1. **Open Command Prompt:**
   - Press `Win + R`
   - Type: `cmd`
   - Press Enter

2. **Check Python version:**
   ```cmd
   python --version
   ```
   
   You should see:
   ```
   Python 3.11.7
   ```

3. **Check pip (Python package installer):**
   ```cmd
   pip --version
   ```
   
   You should see something like:
   ```
   pip 23.3.1 from C:\...\Python311\lib\site-packages\pip (python 3.11)
   ```

**If you see version numbers, Python is installed correctly! ✅**

---

## Step 2: Install Google Chrome

The scraper uses Chrome browser for Selenium.

1. **Download Chrome:**
   - Visit: https://www.google.com/chrome/
   - Click "Download Chrome"
   - Install it

2. **Verify Chrome is installed:**
   - Try opening Chrome from Start menu

---

## Step 3: Navigate to Project Folder

1. **Open Command Prompt** (if not already open)

2. **Navigate to project directory:**
   ```cmd
   cd "C:\my projects\Scrapping\scrapping site"
   ```

3. **Verify you're in the right folder:**
   ```cmd
   dir
   ```
   
   You should see these files:
   - main.py
   - requirements.txt
   - README.md
   - setup.bat
   - etc.

---

## Step 4: Run Setup Script

### Option A: Automatic Setup (Recommended)

Simply **double-click** on `setup.bat` in your project folder.

OR run from command prompt:
```cmd
setup.bat
```

This will:
- ✅ Check Python installation
- ✅ Install all required packages
- ✅ Create necessary folders
- ✅ Run a test to verify everything works

### Option B: Manual Setup

If automatic setup doesn't work:

1. **Install dependencies:**
   ```cmd
   pip install -r requirements.txt
   ```

2. **Create directories:**
   ```cmd
   mkdir data\raw
   mkdir data\processed
   mkdir data\checkpoints
   mkdir logs
   ```

3. **Run a test:**
   ```cmd
   python test_scraper.py detection
   ```

---

## Step 5: Verify Installation

Run the test suite:

```cmd
python test_scraper.py
```

You should see:
```
✓ PASS: Wheel Detection
✓ PASS: Data Processing
✓ PASS: Excel Export
✓ PASS: TascaParts Single Product

4/4 tests passed

✓ ALL TESTS PASSED!
```

---

## Step 6: You're Ready! 🎉

Now you can start scraping:

### Test with One Site (Recommended First)

```cmd
python run_single_site.py tascaparts 5
```

This will scrape only 5 products from TascaParts (for testing).

### Run Full Scraper

```cmd
python main.py
```

Or simply **double-click** `run.bat`

---

## Troubleshooting

### Problem: "python is not recognized"

**Solution:** Python is not in your PATH.

1. Uninstall Python
2. Reinstall Python
3. **Make sure to check "Add python.exe to PATH"** during installation

### Problem: "pip is not recognized"

**Solution:** Try using `python -m pip` instead:

```cmd
python -m pip install -r requirements.txt
```

### Problem: Installation fails with "error: Microsoft Visual C++ 14.0 is required"

**Solution:** Install Microsoft C++ Build Tools:

1. Download: https://visualstudio.microsoft.com/visual-cpp-build-tools/
2. Install "Desktop development with C++"
3. Restart computer
4. Run `pip install -r requirements.txt` again

### Problem: "Access Denied" or "Permission Error"

**Solution:** Run Command Prompt as Administrator:

1. Press `Win` key
2. Type "cmd"
3. Right-click "Command Prompt"
4. Select "Run as administrator"
5. Navigate to project folder and run setup again

### Problem: ChromeDriver issues

**Solution:** The scraper auto-downloads ChromeDriver, but ensure:

1. Chrome browser is installed
2. You have internet connection
3. Antivirus isn't blocking downloads

---

## What Gets Installed

When you run `pip install -r requirements.txt`, these packages are installed:

- **requests** - For making HTTP requests
- **beautifulsoup4** - For parsing HTML
- **selenium** - For browser automation
- **pandas** - For data processing
- **openpyxl** - For Excel export
- **lxml** - For faster HTML parsing
- **fake-useragent** - For randomized user agents
- **webdriver-manager** - For automatic ChromeDriver management
- **tqdm** - For progress bars

Total installation size: ~200-300 MB

---

## Quick Reference Commands

```cmd
# Check Python version
python --version

# Check pip version
pip --version

# Navigate to project
cd "C:\my projects\Scrapping\scrapping site"

# Install dependencies
pip install -r requirements.txt

# Run tests
python test_scraper.py

# Test single product
python test_scraper.py tascaparts

# Test one site (5 products)
python run_single_site.py tascaparts 5

# Run full scraper
python main.py
```

---

## Next Steps After Installation

1. ✅ **Verify installation:** Run `python test_scraper.py`
2. ✅ **Test single product:** Run `python test_scraper.py tascaparts`
3. ✅ **Test one site:** Run `python run_single_site.py tascaparts 5`
4. ✅ **Read documentation:** Open `QUICKSTART.md`
5. ✅ **Run full scraper:** Run `python main.py` (takes 1-3 days)

---

## System Requirements

- **OS:** Windows 10 or higher
- **Python:** 3.8 or higher (3.11 recommended)
- **RAM:** 4GB minimum, 8GB recommended
- **Disk Space:** 2GB free space
- **Internet:** Stable connection required
- **Browser:** Google Chrome (latest version)

---

## Getting Help

If you encounter issues:

1. **Check logs:** Look in the `logs/` folder
2. **Read documentation:** Check `README.md`
3. **Run tests:** `python test_scraper.py` to diagnose
4. **Check Python version:** Make sure it's 3.8 or higher

---

## Installation Complete Checklist

- [ ] Python 3.8+ installed
- [ ] Python added to PATH
- [ ] pip working
- [ ] Google Chrome installed
- [ ] Navigated to project folder
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Directories created
- [ ] Tests passing (`python test_scraper.py`)

**Once all items are checked, you're ready to start scraping!** ✅

---

## Quick Start

After installation, run this to start scraping all sites:

```cmd
python main.py
```

Or test with one site first:

```cmd
python run_single_site.py tascaparts 10
```

Good luck! 🚀

