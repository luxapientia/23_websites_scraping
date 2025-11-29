"""Process scraped data and prepare for export"""
import pandas as pd
from datetime import datetime
import logging


class DataProcessor:
    """Process scraped data and convert to desired format"""
    
    def __init__(self):
        self.logger = logging.getLogger('data_processor')
        
        # Internal column names (used in code/data dictionaries)
        self.internal_columns = [
            'url', 'image_url', 'date', 'sku', 'pn',
            'col_f', 'col_g', 'col_h', 'col_i', 'col_j', 'col_k', 'col_l', 'col_m',
            'actual_price', 'col_o', 'col_p', 'col_q', 'msrp',
            'col_s', 'col_t', 'col_u', 'col_v', 'col_w',
            'title', 'also_known_as', 'positions', 'description', 'applications', 'replaces',
            'year', 'make', 'model', 'trims', 'engines'
        ]
        
        # Excel column headers (what appears in the exported file)
        self.excel_headers = [
            'url',          # A
            'Image',        # B (was 'image_url')
            'date',         # C
            'sku',          # D
            'PN',           # E (was 'pn')
            'CCC',          # F (was 'col_f')
            'VID',          # G (was 'col_g')
            'HOL',          # H (was 'col_h')
            'RVID',         # I (was 'col_i')
            'RHOL',         # J (was 'col_j')
            'IID',          # K (was 'col_k')
            'IDHol',        # L (was 'col_l')
            '',             # M (was 'col_m')
            'AC$',          # N (was 'actual_price')
            'Shipping',     # O (was 'col_o')
            'Price',        # P (was 'col_p')
            'Core',         # Q (was 'col_q')
            'msrp',         # R
            'STL',          # S (was 'col_s')
            'CC',           # T (was 'col_t')
            'RPN',          # U (was 'col_u')
            'PaintCode',    # V (was 'col_v')
            'ColDesc',      # W (was 'col_w')
            'title',        # X
            'also_known_as', # Y
            'positions',    # Z
            'description',  # AA
            'applications', # AB
            'replaces',     # AC
            'year',         # AD
            'make',         # AE
            'model',        # AF
            'trims',        # AG
            'engines'       # AH
        ]
        
        # Column mapping: internal_name -> excel_header
        self.column_mapping = dict(zip(self.internal_columns, self.excel_headers))
    
    def process_products(self, products_list):
        """
        Convert list of product dictionaries to DataFrame
        Creates multiple rows for each fitment combination
        
        Args:
            products_list: List of product dictionaries
        
        Returns:
            pandas.DataFrame: Processed data
        """
        rows = []
        
        self.logger.info(f"Processing {len(products_list)} products...")
        
        for product in products_list:
            if not product:
                continue
            
            try:
                # Base product data (same for all rows)
                base_data = {
                    'url': product.get('url', ''),
                    'image_url': product.get('image_url', ''),
                    'date': product.get('date', ''),
                    'sku': product.get('sku', ''),
                    'pn': product.get('pn', ''),
                    # Empty columns F-M for client use
                    'col_f': '', 'col_g': '', 'col_h': '', 'col_i': '',
                    'col_j': '', 'col_k': '', 'col_l': '', 'col_m': '',
                    'actual_price': product.get('actual_price', ''),
                    # Empty columns O-Q for client use
                    'col_o': '', 'col_p': '', 'col_q': '',
                    'msrp': product.get('msrp', ''),
                    # Empty columns S-W for client use
                    'col_s': '', 'col_t': '', 'col_u': '', 'col_v': '', 'col_w': '',
                    'title': product.get('title', ''),
                    'also_known_as': product.get('also_known_as', ''),
                    'positions': product.get('positions', ''),
                    'description': product.get('description', ''),
                    'applications': product.get('applications', ''),
                    'replaces': product.get('replaces', ''),
                }
                
                # Create a row for each fitment
                # This means multiple rows for each part number
                fitments = product.get('fitments', [])
                
                if fitments:
                    # Create multiple rows for each fitment (legacy format)
                    for fitment in fitments:
                        row = base_data.copy()
                        row.update({
                            'year': str(fitment.get('year', '')),
                            'make': fitment.get('make', ''),
                            'model': fitment.get('model', ''),
                            'trims': fitment.get('trim', ''),
                            'engines': fitment.get('engine', '')
                        })
                        rows.append(row)
                elif 'year' in product or 'make' in product:
                    # Fitment data already flattened in product (new format)
                    row = base_data.copy()
                    row.update({
                        'year': str(product.get('year', '')),
                        'make': product.get('make', ''),
                        'model': product.get('model', ''),
                        'trims': product.get('trim', ''),
                        'engines': product.get('engine', '')
                    })
                    rows.append(row)
                else:
                    # If no fitment data, create one row with empty fitment fields
                    row = base_data.copy()
                    row.update({
                        'year': '',
                        'make': '',
                        'model': '',
                        'trims': '',
                        'engines': ''
                    })
                    rows.append(row)
                
            except Exception as e:
                self.logger.error(f"Error processing product: {str(e)}")
                continue
        
        # Create DataFrame with internal column names first
        df = pd.DataFrame(rows, columns=self.internal_columns)
        
        # Rename columns to Excel headers
        df.columns = self.excel_headers
        
        self.logger.info(f"Processed {len(df)} rows from {len(products_list)} products")
        self.logger.info(f"Unique part numbers: {df['PN'].nunique()}")  # Using Excel header 'PN'
        
        return df
    
    def validate_data(self, df):
        """
        Validate the processed data
        
        Args:
            df: pandas DataFrame
        
        Returns:
            dict: Validation report
        """
        report = {
            'total_rows': len(df),
            'unique_parts': df['PN'].nunique(),  # Using Excel header 'PN'
            'missing_sku': df['sku'].isna().sum(),
            'missing_price': df['AC$'].isna().sum(),  # Using Excel header 'AC$' (was 'actual_price')
            'missing_msrp': df['msrp'].isna().sum(),
            'missing_fitment': ((df['year'] == '') & (df['make'] == '')).sum(),
            'products_with_multiple_fitments': 0
        }
        
        # Count products with multiple fitments
        if 'PN' in df.columns:  # Using Excel header 'PN'
            fitment_counts = df.groupby('PN').size()
            report['products_with_multiple_fitments'] = (fitment_counts > 1).sum()
        
        self.logger.info("Data Validation Report:")
        for key, value in report.items():
            self.logger.info(f"  {key}: {value}")
        
        return report
    
    def clean_data(self, df):
        """
        Clean and standardize the data
        
        Args:
            df: pandas DataFrame
        
        Returns:
            pandas.DataFrame: Cleaned data
        """
        self.logger.info("Cleaning data...")
        
        # Remove rows with missing critical data
        initial_count = len(df)
        df = df[df['sku'].notna() & (df['sku'] != '')]
        df = df[df['title'].notna() & (df['title'] != '')]
        
        removed = initial_count - len(df)
        if removed > 0:
            self.logger.info(f"Removed {removed} rows with missing critical data")
        
        # Standardize text fields
        text_columns = ['title', 'description', 'also_known_as', 'positions', 
                       'applications', 'replaces', 'make', 'model', 'trims', 'engines']
        
        for col in text_columns:
            if col in df.columns:
                # Remove extra whitespace
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                # Replace 'nan' string with empty string
                df[col] = df[col].replace('nan', '')
        
        # Ensure prices are numeric strings
        price_columns = ['AC$', 'msrp']  # Using Excel headers
        for col in price_columns:
            if col in df.columns:
                # Keep only valid numeric values
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('nan', '')
        
        self.logger.info("Data cleaning complete")
        
        return df
    
    def get_summary_statistics(self, df):
        """
        Get summary statistics about the scraped data
        
        Args:
            df: pandas DataFrame
        
        Returns:
            dict: Summary statistics
        """
        stats = {
            'total_rows': len(df),
            'unique_parts': df['PN'].nunique() if 'PN' in df.columns else 0,  # Using Excel header 'PN'
            'products_by_make': {},
            'average_price': 0,
            'price_range': {'min': 0, 'max': 0}
        }
        
        # Products by make
        if 'make' in df.columns:
            make_counts = df[df['make'] != '']['make'].value_counts()
            stats['products_by_make'] = make_counts.to_dict()
        
        # Price statistics
        if 'AC$' in df.columns:  # Using Excel header 'AC$'
            prices = pd.to_numeric(df['AC$'], errors='coerce')  # Using Excel header 'AC$'
            prices = prices.dropna()
            
            if len(prices) > 0:
                stats['average_price'] = round(prices.mean(), 2)
                stats['price_range'] = {
                    'min': round(prices.min(), 2),
                    'max': round(prices.max(), 2)
                }
        
        return stats

