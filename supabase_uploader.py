import pandas as pd
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import time

# Load environment variables
load_dotenv()

# Get Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials. Please check your .env file.")

def get_supabase() -> Client:
    """Create and return a Supabase client with retry logic"""
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            # Create Supabase client
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            # Test the connection
            supabase.table('cattle_slaughter').select("*").limit(1).execute()
            return supabase
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise Exception(f"Failed to connect to Supabase after {max_retries} attempts: {str(e)}")
            print(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff

def transform_commodity_data(df):
    """
    Transform wide-format commodity data into long format suitable for database
    """
    # Reset index to make date a column
    df = df.reset_index()
    
    # Initialize list to store transformed records
    records = []
    
    # Iterate through each row
    for _, row in df.iterrows():
        date = row['date'].strftime('%Y-%m-%d')
        
        # Process each commodity
        for commodity in ['BEEF', 'FC00', 'GFU22', 'GF', 'LCAT', 'LC00', 'CORN', 'CZ25']:
            price_col = f'{commodity}_price'
            unit_col = f'{commodity}_unit'
            
            if price_col in row and not pd.isna(row[price_col]):
                record = {
                    'date': date,
                    'commodity_symbol': commodity,
                    'price': float(row[price_col]),
                    'unit': row[unit_col] if unit_col in row and not pd.isna(row[unit_col]) else None,
                    'created_at': datetime.now().isoformat()
                }
                records.append(record)
    
    return records

def transform_cattle_data(df):
    """
    Transform cattle slaughter data into the correct format for the database
    """
    # Filter for cattle data only (exclude calves/veal)
    df = df[
        (df['commodity'].str.contains('Cattle', na=False)) &
        (~df['commodity'].str.contains('Calves', na=False))
    ].copy()
    
    # Convert slaughter_date to datetime if it's not already
    df['slaughter_date'] = pd.to_datetime(df['slaughter_date'])
    
    # Create records list
    records = []
    
    # Process each row
    for _, row in df.iterrows():
        if pd.isna(row['slaughter_date']):
            continue
            
        # Skip rows that aren't the metrics we want
        if row['description'] not in ['Head Slaughtered', 'Live Weight', 'Dressed Weight', 'Total Red Meat']:
            continue
            
        # Handle the class field
        class_name = row['class']
        if pd.isna(class_name) or class_name == '':
            class_name = 'All'
            
        record = {
            'slaughter_date': row['slaughter_date'].strftime('%Y-%m-%d'),
            'class': class_name,
            'description': row['description'],
            'volume': float(row['volume']),
            'unit': row['unit'],
            'created_at': datetime.now().isoformat()
        }
        records.append(record)
    
    print(f"Found {len(records)} valid cattle records")
    if records:
        # Print some statistics
        df_records = pd.DataFrame(records)
        print("\nRecords by description:")
        print(df_records.groupby('description').size())
        print("\nRecords by class:")
        print(df_records.groupby('class').size())
        print("\nSample record for each description:")
        for desc in df_records['description'].unique():
            print(f"\n{desc}:", df_records[df_records['description'] == desc].iloc[0].to_dict())
    
    return records

def upload_to_supabase(records, supabase):
    """
    Upload records to Supabase with retry logic
    """
    batch_size = 50  # Smaller batch size for better reliability
    max_retries = 3
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_num = i//batch_size + 1
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                result = supabase.table('commodity_prices').insert(batch).execute()
                print(f"Uploaded batch {batch_num} successfully ({len(batch)} records)")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed to upload batch {batch_num} after {max_retries} attempts: {str(e)}")
                    continue
                print(f"Upload attempt {attempt + 1} for batch {batch_num} failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

def upload_cattle_data(records, supabase):
    """
    Upload cattle records to Supabase with retry logic
    """
    # First, clear existing data to avoid duplicates
    try:
        print("Clearing existing cattle data...")
        supabase.table('cattle_slaughter').delete().neq('id', 0).execute()
    except Exception as e:
        print(f"Warning: Could not clear existing data: {str(e)}")
    
    batch_size = 50  # Smaller batch size for better reliability
    max_retries = 3
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_num = i//batch_size + 1
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                result = supabase.table('cattle_slaughter').insert(batch).execute()
                print(f"Uploaded batch {batch_num} successfully ({len(batch)} records)")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed to upload batch {batch_num} after {max_retries} attempts: {str(e)}")
                    continue
                print(f"Upload attempt {attempt + 1} for batch {batch_num} failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

def main():
    try:
        # Initialize Supabase client
        print("Connecting to Supabase...")
        supabase = get_supabase()
        print("Successfully connected to Supabase!")
        
        # Process cattle data first
        print("\nProcessing cattle data...")
        cattle_df = pd.read_csv('cattle_slaughter_oct16_dec27_2024.csv', parse_dates=['slaughter_date'])
        print(f"Read {len(cattle_df)} rows from CSV")
        cattle_records = transform_cattle_data(cattle_df)
        
        if cattle_records:
            print(f"Uploading {len(cattle_records)} cattle records to Supabase...")
            upload_cattle_data(cattle_records, supabase)
        else:
            print("No valid cattle records found to upload")
        
        # Process commodity data
        print("\nProcessing commodity data...")
        commodity_df = pd.read_csv('commodity_prices_2024-12-25_to_2025-01-23.csv', parse_dates=['date'])
        commodity_records = transform_commodity_data(commodity_df)
        
        if commodity_records:
            print(f"Uploading {len(commodity_records)} commodity records to Supabase...")
            upload_to_supabase(commodity_records, supabase)
        else:
            print("No valid commodity records found to upload")
        
        print("All uploads completed!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
