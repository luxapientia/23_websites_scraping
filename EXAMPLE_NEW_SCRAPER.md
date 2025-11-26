# How to Add a New Site Scraper

This guide shows you how to add a scraper for a new automotive parts website.

## Example: Adding Honda Parts Scraper

Let's say you want to add a scraper for `https://www.hondapartsonline.net`

### Step 1: Analyze the Website

Visit the website and understand:

1. **How are wheels listed?**
   - Category page: `/c/wheels`
   - Search: `/search?q=wheel`
   - Browse by model: `/auto-parts/honda`

2. **What's the product page structure?**
   - URL pattern: `/oem-parts/honda-wheel-[name]-[sku]`
   - Where is the title? `<h1 class="product-title">`
   - Where is the price? `<span class="sale-price">`
   - Where is SKU? `<div class="part-number">`
   - Where is fitment? Check for JSON in `<script>` tags

3. **Does it use JavaScript?**
   - If yes, you need Selenium
   - If no, requests + BeautifulSoup is enough

### Step 2: Create Scraper File

Create `scrapers/honda_scraper.py`:

```python
"""Scraper for hondapartsonline.net"""
from scrapers.base_scraper import BaseScraper
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime


class HondaScraper(BaseScraper):
    """Scraper for Honda Parts Online"""
    
    def __init__(self):
        # Initialize with site name and whether to use Selenium
        super().__init__('honda', use_selenium=True)
        self.base_url = 'https://www.hondapartsonline.net'
    
    def get_product_urls(self):
        """Get all wheel product URLs"""
        product_urls = []
        
        try:
            # Method 1: Try category page
            category_url = f"{self.base_url}/c/wheels"
            self.logger.info(f"Fetching category: {category_url}")
            
            html = self.get_page(category_url, use_selenium=True, wait_time=3)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                
                # Find all product links
                links = soup.find_all('a', href=re.compile(r'/oem-parts/.*wheel'))
                
                for link in links:
                    href = link.get('href', '')
                    if href:
                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                        product_urls.append(full_url)
            
            # Method 2: If category doesn't work, try search
            if len(product_urls) < 10:
                search_urls = self._search_for_wheels()
                product_urls.extend(search_urls)
                product_urls = list(set(product_urls))  # Remove duplicates
            
            self.logger.info(f"Found {len(product_urls)} product URLs")
            
        except Exception as e:
            self.logger.error(f"Error getting product URLs: {str(e)}")
        
        return product_urls
    
    def _search_for_wheels(self):
        """Search for wheels using site search"""
        product_urls = []
        
        try:
            search_url = f"{self.base_url}/search?q=wheel"
            self.logger.info(f"Searching: {search_url}")
            
            html = self.get_page(search_url, use_selenium=True, wait_time=3)
            if html:
                soup = BeautifulSoup(html, 'lxml')
                
                # Find product links
                links = soup.find_all('a', class_='product-link')
                
                for link in links:
                    href = link.get('href', '')
                    if href and 'wheel' in href.lower():
                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                        product_urls.append(full_url)
        
        except Exception as e:
            self.logger.error(f"Error searching: {str(e)}")
        
        return product_urls
    
    def scrape_product(self, url):
        """Scrape single product"""
        html = self.get_page(url, use_selenium=True, wait_time=2)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Initialize product data
        product_data = {
            'url': url,
            'image_url': '',
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sku': '',
            'pn': '',
            'actual_price': '',
            'msrp': '',
            'title': '',
            'also_known_as': '',
            'positions': '',
            'description': '',
            'applications': '',
            'replaces': '',
            'fitments': []
        }
        
        try:
            # Extract title
            title_elem = soup.find('h1', class_='product-title')
            if title_elem:
                product_data['title'] = title_elem.get_text(strip=True)
            
            # Check if this is a wheel product
            if not self.is_wheel_product(product_data['title']):
                self.logger.info(f"Skipping non-wheel: {product_data['title']}")
                return None
            
            # Extract SKU
            sku_elem = soup.find('span', class_='part-number')
            if sku_elem:
                product_data['sku'] = sku_elem.get_text(strip=True)
                product_data['pn'] = self.clean_sku(product_data['sku'])
            
            # Extract sale price
            price_elem = soup.find('span', class_='sale-price')
            if price_elem:
                product_data['actual_price'] = self.extract_price(price_elem.get_text(strip=True))
            
            # Extract MSRP
            msrp_elem = soup.find('span', class_='list-price')
            if msrp_elem:
                product_data['msrp'] = self.extract_price(msrp_elem.get_text(strip=True))
            
            # Extract image
            img_elem = soup.find('img', class_='product-image')
            if img_elem and img_elem.get('src'):
                img_url = img_elem['src']
                product_data['image_url'] = f"https:{img_url}" if img_url.startswith('//') else img_url
            
            # Extract description
            desc_elem = soup.find('div', class_='product-description')
            if desc_elem:
                product_data['description'] = desc_elem.get_text(strip=True, separator=' ')
            
            # Extract fitment data
            self._extract_fitment(soup, product_data)
            
            # If no fitments, add empty fitment
            if not product_data['fitments']:
                product_data['fitments'].append({
                    'year': '',
                    'make': '',
                    'model': '',
                    'trim': '',
                    'engine': ''
                })
            
            self.logger.info(f"✓ Scraped: {product_data['title']}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return None
    
    def _extract_fitment(self, soup, product_data):
        """Extract fitment data from JSON or table"""
        
        # Try to find JSON fitment data
        script_tags = soup.find_all('script', type='application/json')
        
        for script in script_tags:
            try:
                if script.string:
                    data = json.loads(script.string)
                    fitments = data.get('fitment', [])
                    
                    if fitments:
                        for fitment in fitments:
                            year = str(fitment.get('year', ''))
                            make = fitment.get('make', '')
                            model = fitment.get('model', '')
                            trims = fitment.get('trims', [])
                            engines = fitment.get('engines', [])
                            
                            # Handle multiple trims and engines
                            if not trims:
                                trims = ['']
                            if not engines:
                                engines = ['']
                            
                            for trim in trims:
                                for engine in engines:
                                    product_data['fitments'].append({
                                        'year': year,
                                        'make': make,
                                        'model': model,
                                        'trim': trim,
                                        'engine': engine
                                    })
                        return
            except:
                continue
        
        # Fallback: Try to find fitment table
        fitment_table = soup.find('table', class_=re.compile(r'fitment', re.I))
        if fitment_table:
            rows = fitment_table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    product_data['fitments'].append({
                        'year': cols[0].get_text(strip=True) if len(cols) > 0 else '',
                        'make': cols[1].get_text(strip=True) if len(cols) > 1 else '',
                        'model': cols[2].get_text(strip=True) if len(cols) > 2 else '',
                        'trim': cols[3].get_text(strip=True) if len(cols) > 3 else '',
                        'engine': cols[4].get_text(strip=True) if len(cols) > 4 else ''
                    })
```

### Step 3: Add to Configuration

Edit `config/sites_config.json` and add:

```json
{
  "name": "honda",
  "base_url": "https://www.hondapartsonline.net",
  "brands": ["Honda"],
  "search_strategy": "category",
  "category_url": "/c/wheels",
  "use_selenium": true
}
```

### Step 4: Import in main.py

Edit `main.py` and add:

```python
# At the top with other imports
from scrapers.honda_scraper import HondaScraper

# In the create_scraper function
def create_scraper(site_config):
    site_name = site_config.get('name', '')
    
    if site_name == 'tascaparts':
        return TascaPartsScraper()
    elif site_name == 'honda':  # Add this
        return HondaScraper()
    else:
        return GenericScraper(site_config)
```

### Step 5: Test Your Scraper

Create a test file `test_honda.py`:

```python
from scrapers.honda_scraper import HondaScraper

def test_honda():
    scraper = HondaScraper()
    
    # Test getting URLs
    print("Getting product URLs...")
    urls = scraper.get_product_urls()
    print(f"Found {len(urls)} URLs")
    
    if urls:
        # Test scraping first product
        print(f"\nTesting first URL: {urls[0]}")
        product = scraper.scrape_product(urls[0])
        
        if product:
            print("\n✓ Success!")
            print(f"Title: {product['title']}")
            print(f"SKU: {product['sku']}")
            print(f"Price: ${product['actual_price']}")
            print(f"Fitments: {len(product['fitments'])}")
        else:
            print("\n✗ Failed to scrape product")
    
    scraper.close()

if __name__ == "__main__":
    test_honda()
```

Run it:
```bash
python test_honda.py
```

### Step 6: Run Full Scraper

Once testing is successful:

```bash
python main.py
```

## Tips for Site-Specific Scrapers

### 1. Handle Pagination

```python
def get_product_urls(self):
    product_urls = []
    page = 1
    max_pages = 100
    
    while page <= max_pages:
        page_url = f"{self.base_url}/wheels?page={page}"
        html = self.get_page(page_url)
        
        if not html:
            break
        
        soup = BeautifulSoup(html, 'lxml')
        links = soup.find_all('a', class_='product-link')
        
        if not links:
            break  # No more products
        
        for link in links:
            product_urls.append(...)
        
        page += 1
    
    return product_urls
```

### 2. Handle AJAX/Dynamic Content

```python
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_product_urls(self):
    self.driver.get(category_url)
    
    # Wait for products to load
    wait = WebDriverWait(self.driver, 10)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'product-link')))
    
    # Scroll to load more
    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    
    html = self.driver.page_source
    # Parse html...
```

### 3. Handle Different Price Formats

```python
def extract_price(self, price_text):
    """Handle various price formats"""
    if not price_text:
        return ''
    
    # Remove common patterns
    price = price_text.replace('$', '').replace(',', '').replace('USD', '')
    price = price.replace('Price:', '').strip()
    
    # Extract first number found
    match = re.search(r'(\d+\.?\d*)', price)
    if match:
        return match.group(1)
    
    return ''
```

### 4. Handle Authentication/Login

```python
def __init__(self):
    super().__init__('sitename', use_selenium=True)
    self.base_url = '...'
    self._login()

def _login(self):
    """Login to the site if required"""
    login_url = f"{self.base_url}/login"
    self.driver.get(login_url)
    
    # Fill in login form
    username_field = self.driver.find_element(By.ID, 'username')
    password_field = self.driver.find_element(By.ID, 'password')
    
    username_field.send_keys('your_username')
    password_field.send_keys('your_password')
    
    submit_button = self.driver.find_element(By.ID, 'submit')
    submit_button.click()
    
    time.sleep(3)  # Wait for login
```

## Common Patterns

### Pattern 1: Product Grid/List

```python
# Find all product cards
products = soup.find_all('div', class_='product-card')

for product in products:
    link = product.find('a', class_='product-link')
    if link:
        href = link.get('href')
        product_urls.append(...)
```

### Pattern 2: Search Results

```python
search_url = f"{self.base_url}/search?q=wheel&page={page}"
# Parse search results...
```

### Pattern 3: Category Navigation

```python
# Get all makes
makes_url = f"{self.base_url}/makes"
# For each make, get models
# For each model, get years
# For each combination, get wheels
```

## Debugging Tips

1. **Print HTML structure:**
   ```python
   print(soup.prettify()[:1000])
   ```

2. **Check specific elements:**
   ```python
   print("Title elem:", soup.find('h1'))
   print("Price elem:", soup.find(class_='price'))
   ```

3. **Save HTML for inspection:**
   ```python
   with open('debug.html', 'w', encoding='utf-8') as f:
       f.write(html)
   ```

4. **Use browser developer tools:**
   - Right-click element → Inspect
   - Copy selector
   - Use in your scraper

## Need Help?

- Check `base_scraper.py` for utility methods
- Look at `tascaparts_scraper.py` for a complete example
- Review `generic_scraper.py` for common patterns
- Check logs in `logs/` directory

Happy scraping! 🚀

