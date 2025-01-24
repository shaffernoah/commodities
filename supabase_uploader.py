import pandas as pd
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import time
import httpx

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = "https://iqccsqfkxzoptjqhxrht.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlxY2NzcWZreHpvcHRqcWhocmh0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDU5NTU1NjgsImV4cCI6MjAyMTUzMTU2OH0.h0AMmUBF2EXXriLEXM5-cCUGhRtDCkR9JtbhkgpWQzA"

def get_supabase() -> Client:
    """Create and return a Supabase client with retry logic"""
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            # Create a custom transport with specific timeouts
            transport = httpx.HTTPTransport(retries=3)
            client = httpx.Client(
                transport=transport,
                timeout=30.0,  # 30 seconds timeout
                verify=True,  # Verify SSL certificates
            )
            
            # Create Supabase client with custom HTTP client
            supabase = create_client(supabase_url, supabase_key, options={
                'http_client': client
            })
            
            # Test the connection
            supabase.table('commodity_prices').select("*").limit(1).execute()
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

def main():
    try:
        # Initialize Supabase client
        print("Connecting to Supabase...")
        supabase = get_supabase()
        
        # Read the CSV file
        print("Reading commodity data...")
        df = pd.read_csv('commodity_prices_2024-12-25_to_2025-01-23.csv', parse_dates=['date'])
        
        # Transform the data
        print("Transforming data...")
        records = transform_commodity_data(df)
        
        # Upload to Supabase
        print(f"Uploading {len(records)} records to Supabase...")
        upload_to_supabase(records, supabase)
        
        print("Upload completed!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
