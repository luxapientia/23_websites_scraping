
import os

file_path = r'c:\my projects\Scrapping\scrapping site\scrapers\acurapartswarehouse_scraper.py'

new_method = r'''    def scrape_product(self, url):
        """
        Scrape single product from AcuraPartsWarehouse with refined extraction logic
        Returns a LIST of dictionaries (one for each fitment/trim combination)
        """
        import random
        import json
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        
        # Import TimeoutException - try to import it
        try:
            from selenium.common.exceptions import TimeoutException
        except ImportError:
            TimeoutException = type('TimeoutException', (Exception,), {})
        
        max_retries = 3
        retry_count = 0
        html = None
        
        while retry_count < max_retries:
            try:
                if not self.check_health():
                    self.logger.error("Scraper health check failed, stopping")
                    return []
                
                self.logger.info(f"Loading product page (attempt {retry_count + 1}/{max_retries}): {url}")
                
                try:
                    self.ensure_driver()
                except Exception as driver_error:
                    recovery = self.error_handler.handle_error(driver_error, retry_count)
                    if recovery['should_retry'] and retry_count < max_retries - 1:
                        time.sleep(random.uniform(recovery['wait_time'][0], recovery['wait_time'][1]))
                        retry_count += 1
                        continue
                    else:
                        return []
                
                try:
                    self.driver.get(url)
                    time.sleep(random.uniform(1.0, 2.0))
                    
                    # Cloudflare check
                    if ('challenges.cloudflare.com' in self.driver.current_url.lower() or len(self.driver.page_source) < 5000) and self.has_cloudflare_challenge():
                        self.logger.info("🛡️ Cloudflare challenge detected...")
                        if not self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1):
                            if len(self.driver.page_source) <= 5000:
                                retry_count += 1
                                time.sleep(random.uniform(10, 15))
                                continue
                        time.sleep(1)
                    
                    self.simulate_human_behavior()
                    
                    # Handle "View More" for fitment table specifically
                    try:
                        # Look for the specific "View More" button for the fitment table
                        view_more_btn = None
                        try:
                            # Try specific class first
                            view_more_btn = self.driver.find_element(By.CSS_SELECTOR, ".fit-vehicle-list-view-text")
                        except:
                            # Try generic text search if specific class fails
                            xpath_selectors = [
                                "//div[contains(@class, 'fit-vehicle-list-view-text')]",
                                "//div[contains(text(), 'View More') and contains(@class, 'fit-vehicle-list')]",
                                "//span[contains(text(), 'View More')]"
                            ]
                            for xpath in xpath_selectors:
                                try:
                                    btns = self.driver.find_elements(By.XPATH, xpath)
                                    for btn in btns:
                                        if btn.is_displayed():
                                            view_more_btn = btn
                                            break
                                    if view_more_btn: break
                                except: continue
                        
                        if view_more_btn and view_more_btn.is_displayed():
                            self.logger.info("Found 'View More' button for fitment, clicking...")
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_more_btn)
                            time.sleep(0.5)
                            self.driver.execute_script("arguments[0].click();", view_more_btn)
                            time.sleep(1.5)
                    except Exception as e:
                        self.logger.debug(f"Note: Could not click 'View More' (might not exist): {e}")
                    
                    html = self.driver.page_source
                    if not html or len(html) < 1000: raise Exception("Page content too small")
                    break
                    
                except TimeoutException:
                    retry_count += 1
                    continue
                except Exception as e:
                    self.logger.warning(f"⚠️ Error loading page: {e}")
                    retry_count += 1
                    time.sleep(random.uniform(5, 10))
                    continue
            except Exception as e:
                self.logger.error(f"❌ Critical error: {e}")
                return []

        if not html: return []

        soup = BeautifulSoup(html, 'lxml')
        
        base_data = {
            'url': url, 'image_url': '', 'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sku': '', 'pn': '', 'actual_price': '', 'msrp': '', 'title': '',
            'also_known_as': '', 'positions': '', 'description': '', 'applications': '', 'replaces': ''
        }
        
        try:
            # 1. Title
            title_elem = soup.find('h1', class_='product-title') or soup.find('h1', class_='pn-detail-h1') or soup.find('h1')
            if title_elem:
                base_data['title'] = title_elem.get_text(strip=True)
            else:
                title_tag = soup.find('title')
                if title_tag: base_data['title'] = title_tag.get_text(strip=True).split('|')[0].strip()

            if not base_data['title']: return []

            # 2. SKU/PN (Pattern: XXXXX-XXX-XXX)
            # Priority 1: Meta tags / JSON-LD
            try:
                # Check JSON-LD
                json_ld = soup.find('script', type='application/ld+json')
                if json_ld:
                    data = json.loads(json_ld.string)
                    if isinstance(data, list): data = data[0]
                    if 'sku' in data: base_data['sku'] = data['sku']
                    if 'mpn' in data: base_data['sku'] = data['mpn']
                
                # Check meta tags
                if not base_data['sku']:
                    meta_sku = soup.find('meta', itemprop='sku')
                    if meta_sku: base_data['sku'] = meta_sku.get('content')
            except: pass

            # Priority 2: Specific HTML element
            if not base_data['sku']:
                pn_div = soup.find('div', class_='acc-pn-detail-sub-title')
                if pn_div:
                    strong = pn_div.find('strong')
                    if strong: base_data['sku'] = strong.get_text(strip=True)

            # Priority 3: HTML (Parts Page)
            if not base_data['sku']:
                sub_desc = soup.find('p', class_='pn-detail-sub-desc')
                if sub_desc:
                    text = sub_desc.get_text(strip=True)
                    parts = text.split()
                    if parts:
                        potential_pn = parts[-1]
                        if any(c.isdigit() for c in potential_pn):
                            base_data['sku'] = potential_pn

            # Priority 4: Regex from Title/URL
            if not base_data['sku']:
                pattern = r'\b([A-Z0-9]{3,5}-[A-Z0-9]{2,3}-[A-Z0-9]{2,3})\b'
                match = re.search(pattern, base_data['title'], re.I)
                if match: base_data['sku'] = match.group(1).upper()
                else:
                    match = re.search(pattern, url.upper())
                    if match: base_data['sku'] = match.group(1)

            if base_data['sku']:
                base_data['sku'] = base_data['sku'].upper()
                base_data['pn'] = base_data['sku'].replace('-', '')

            # 3. Image URL
            # Priority 1: JSON-LD
            try:
                if not base_data['image_url']:
                    json_ld = soup.find('script', type='application/ld+json')
                    if json_ld:
                        data = json.loads(json_ld.string)
                        if isinstance(data, list): data = data[0]
                        if 'image' in data:
                            imgs = data['image']
                            if isinstance(imgs, list) and imgs: base_data['image_url'] = imgs[0]
                            elif isinstance(imgs, str): base_data['image_url'] = imgs
            except: pass

            # Priority 2: Specific img tag
            if not base_data['image_url']:
                # Accessory Page
                img_tag = soup.find('img', class_='pn-img-img')
                if not img_tag:
                    # Parts Page
                    img_div = soup.find('div', class_='pn-detail-img-area')
                    if img_div:
                        img_tag = img_div.find('img')
                
                if not img_tag:
                    # Fallback to src pattern
                    img_tag = soup.find('img', src=re.compile(r'/resources/.*accessory-image/'))
                    if not img_tag:
                        img_tag = soup.find('img', src=re.compile(r'/resources/.*part-picture/'))
                
                if img_tag:
                    base_data['image_url'] = img_tag.get('src')

            # Normalize URL
            if base_data['image_url']:
                if base_data['image_url'].startswith('//'): 
                    base_data['image_url'] = 'https:' + base_data['image_url']
                elif base_data['image_url'].startswith('/'): 
                    base_data['image_url'] = self.base_url + base_data['image_url']

            # 4. MSRP & Price
            # MSRP
            msrp_span = soup.find('span', class_='price-section-retail')
            if msrp_span:
                inner_span = msrp_span.find('span')
                if inner_span:
                    base_data['msrp'] = self.extract_price(inner_span.get_text(strip=True))
                else:
                    base_data['msrp'] = self.extract_price(msrp_span.get_text(strip=True))
            
            # Actual Price
            price_span = soup.find('span', class_='price-section-price')
            if price_span:
                base_data['actual_price'] = self.extract_price(price_span.get_text(strip=True))

            # 5. Description
            # Priority 1: Marketing description div (Accessory)
            desc_div = soup.find('div', class_=lambda c: c and 'acc-pn-detail-marketing' in c)
            if desc_div:
                ul = desc_div.find('ul')
                if ul:
                    items = [li.get_text(strip=True) for li in ul.find_all('li')]
                    base_data['description'] = ' | '.join(items)
            
            # Priority 2: Parts Page Description List
            if not base_data['description']:
                detail_list = soup.find('ul', class_='pn-detail-list')
                if detail_list:
                    for li in detail_list.find_all('li'):
                        span = li.find('span')
                        if span and 'Part Description' in span.get_text(strip=True):
                            div = li.find('div')
                            if div:
                                base_data['description'] = div.get_text(strip=True)
                                break

            # Priority 3: Meta description
            if not base_data['description']:
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc: base_data['description'] = meta_desc.get('content', '')

            # Priority 4: Product Specifications
            if not base_data['description']:
                spec_div = soup.find('div', class_=re.compile(r'product.*spec', re.I))
                if spec_div: base_data['description'] = spec_div.get_text(strip=True, separator=' ')

            # 6. Fitment Data extraction
            fitment_rows = []
            
            # Strategy 1: Parse the fitment table
            fitment_table = soup.find('div', class_='fit-vehicle-list')
            if not fitment_table:
                fitment_table = soup.find('div', class_='fit-vehicle-list-table')
            
            if fitment_table:
                table = fitment_table.find('table', class_='fit-vehicle-list-table')
                if table:
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_row = thead.find('tr')
                        if header_row:
                            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                    
                    col_map = {}
                    for i, h in enumerate(headers):
                        if 'year' in h: col_map['year'] = i
                        elif 'make' in h: col_map['make'] = i
                        elif 'model' in h: col_map['model'] = i
                        elif 'trim' in h or 'body' in h: col_map['trim'] = i
                        elif 'engine' in h: col_map['engine'] = i
                    
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if not cols: continue
                            
                            row_data = {
                                'year': '', 'make': 'Acura', 'model': '', 'trim': '', 'engine': ''
                            }
                            
                            # Handle combined Year Make Model column if needed
                            if 'year' not in col_map and len(cols) > 0:
                                # Check if first column is "Year Make Model"
                                first_col_text = cols[0].get_text(strip=True)
                                # Try to parse "2024 Acura MDX"
                                parts = first_col_text.split()
                                if len(parts) >= 3 and parts[0].isdigit():
                                    row_data['year'] = parts[0]
                                    row_data['make'] = parts[1]
                                    row_data['model'] = ' '.join(parts[2:])
                            
                            for key, idx in col_map.items():
                                if idx < len(cols):
                                    value = cols[idx].get_text(strip=True)
                                    if value and value.endswith('.'):
                                        value = value[:-1].strip()
                                    row_data[key] = value
                            
                            if not row_data['make']: row_data['make'] = 'Acura'
                            
                            if not row_data['year'] or not row_data['model']:
                                continue
                            
                            if ',' in row_data['trim']:
                                trims = [t.strip() for t in row_data['trim'].split(',') if t.strip()]
                                for trim in trims:
                                    new_row = row_data.copy()
                                    new_row['trim'] = trim
                                    fitment_rows.append(new_row)
                            else:
                                fitment_rows.append(row_data)

            # Strategy 2: Fallback to JSON data
            if not fitment_rows:
                try:
                    scripts = soup.find_all('script', type='application/json')
                    for script in scripts:
                        if script.string and 'fitment' in script.string.lower():
                            try:
                                data = json.loads(script.string)
                                fitments = []
                                if 'fitment' in data: fitments = data['fitment']
                                elif 'props' in data and 'fitment' in data['props']: fitments = data['props']['fitment']
                                
                                if fitments:
                                    for f in fitments:
                                        year = str(f.get('year', ''))
                                        make = f.get('make', 'Acura')
                                        model = f.get('model', '')
                                        trims = f.get('trims', [])
                                        if isinstance(trims, str): trims = [trims]
                                        if not trims: trims = ['']
                                        
                                        for trim in trims:
                                            fitment_rows.append({
                                                'year': year,
                                                'make': make,
                                                'model': model,
                                                'trim': trim,
                                                'engine': f.get('engine', '')
                                            })
                            except: continue
                except: pass

            # Strategy 3: Fallback to "This Part Fits" text parsing
            if not fitment_rows:
                try:
                    fits_text_elem = soup.find(string=re.compile(r'Fits\s+\d{4}', re.I))
                    if fits_text_elem:
                        text = fits_text_elem.parent.get_text() if fits_text_elem.parent else fits_text_elem
                        year_match = re.search(r'(\d{4})[-\s]+(\d{4})', text)
                        if year_match:
                            start, end = int(year_match.group(1)), int(year_match.group(2))
                            models_part = text.split('Acura')[-1]
                            models = [m.strip() for m in re.split(r'[,&]', models_part) if m.strip()]
                            
                            for year in range(start, end + 1):
                                for model in models:
                                    fitment_rows.append({
                                        'year': str(year), 'make': 'Acura', 'model': model, 'trim': '', 'engine': ''
                                    })
                except: pass

            # Construct final result list
            final_list = []
            if fitment_rows:
                self.logger.info(f"✅ Extracted {len(fitment_rows)} fitment rows")
                for f in fitment_rows:
                    row = base_data.copy()
                    row.update(f)
                    final_list.append(row)
            else:
                self.logger.warning("⚠️ No fitments found, returning single row")
                row = base_data.copy()
                row.update({'year': '', 'make': 'Acura', 'model': '', 'trim': '', 'engine': ''})
                final_list.append(row)

            return final_list

        except Exception as e:
            self.logger.error(f"❌ Error scraping product: {e}")
            return []
'''

# Read existing file
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Keep lines 0 to 1273 (since line numbers are 1-indexed in view_file, index 1273 is line 1274)
# Wait, line 1274 in view_file is index 1273.
# We want to keep up to line 1273 (index 1272).
# Line 1273 is empty in the view_file output.
# Line 1274 is "    def scrape_product(self, url):"
# So we want lines[:1273]

kept_lines = lines[:1273]

# Write back
with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(kept_lines)
    f.write('\n')
    f.write(new_method)

print("Successfully updated scraper file.")
