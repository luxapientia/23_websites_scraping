"""Update date values across all Excel files while preserving order"""
import pandas as pd
from datetime import datetime, timedelta
import os
import glob

def update_dates_in_excel_files():
    """Update all date values in Excel files from 2026-01-08 08:45:02 to 2026-01-12 09:49:12"""
    
    # Directory containing Excel files
    data_dir = "data/processed"
    
    # Get all xlsx files (excluding temp files)
    excel_files = [f for f in glob.glob(os.path.join(data_dir, "*.xlsx")) 
                   if not os.path.basename(f).startswith("~$")]
    
    print(f"Found {len(excel_files)} Excel files")
    print(f"Files: {[os.path.basename(f) for f in excel_files[:5]]}...")
    
    # Step 1: Read all files and collect all dates with their file and row info
    all_dates_info = []
    
    for file_path in excel_files:
        print(f"Reading {os.path.basename(file_path)}...")
        try:
            df = pd.read_excel(file_path)
            
            if 'date' not in df.columns:
                print(f"  Warning: No 'date' column in {os.path.basename(file_path)}")
                continue
            
            # Collect all dates with their file and row index
            for idx, date_val in enumerate(df['date']):
                if pd.notna(date_val):
                    all_dates_info.append({
                        'file_path': file_path,
                        'row_index': idx,
                        'date': date_val
                    })
        except Exception as e:
            print(f"  Error reading {os.path.basename(file_path)}: {e}")
            continue
    
    print(f"\nTotal dates found: {len(all_dates_info)}")
    
    if len(all_dates_info) == 0:
        print("No dates found to update!")
        return
    
    # Step 2: Sort all dates while keeping track of original order
    # Convert dates to datetime for sorting
    date_objects = []
    for info in all_dates_info:
        try:
            if isinstance(info['date'], str):
                dt = datetime.strptime(info['date'], '%Y-%m-%d %H:%M:%S')
            else:
                dt = pd.to_datetime(info['date'])
            date_objects.append((dt, info))
        except Exception as e:
            print(f"  Warning: Could not parse date '{info['date']}': {e}")
            # Use a default date for sorting
            date_objects.append((datetime(2000, 1, 1), info))
    
    # Sort by date
    date_objects.sort(key=lambda x: x[0])
    
    # Step 3: Create new date range from 2026-01-08 08:45:02 to 2026-01-12 09:49:12
    start_date = datetime(2026, 1, 8, 8, 45, 2)
    end_date = datetime(2026, 1, 12, 9, 49, 12)
    
    total_seconds = (end_date - start_date).total_seconds()
    num_dates = len(date_objects)
    
    # Generate evenly distributed dates
    new_dates = []
    if num_dates == 1:
        new_dates = [start_date]
    else:
        for i in range(num_dates):
            # Distribute evenly across the time range
            fraction = i / (num_dates - 1) if num_dates > 1 else 0
            seconds_offset = fraction * total_seconds
            new_date = start_date + timedelta(seconds=seconds_offset)
            new_dates.append(new_date)
    
    # Step 4: Map new dates back to original files and rows
    # Create a mapping: (file_path, row_index) -> new_date
    date_mapping = {}
    for (old_dt, info), new_dt in zip(date_objects, new_dates):
        key = (info['file_path'], info['row_index'])
        date_mapping[key] = new_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Step 5: Update all files
    print(f"\nUpdating dates in {len(excel_files)} files...")
    
    for file_path in excel_files:
        try:
            print(f"  Updating {os.path.basename(file_path)}...")
            df = pd.read_excel(file_path)
            
            if 'date' not in df.columns:
                continue
            
            # Update dates in this file
            updated_count = 0
            for idx in range(len(df)):
                key = (file_path, idx)
                if key in date_mapping:
                    df.at[idx, 'date'] = date_mapping[key]
                    updated_count += 1
            
            # Save the updated file
            df.to_excel(file_path, index=False)
            print(f"    Updated {updated_count} dates")
            
        except Exception as e:
            print(f"    Error updating {os.path.basename(file_path)}: {e}")
    
    print(f"\n✓ Complete! Updated dates from {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    update_dates_in_excel_files()

