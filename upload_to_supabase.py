import os
import pandas as pd
from supabase import create_client
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Supabase configuration
SUPABASE_URL = "https://qarnlsfwgnjgunsmemea.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcm5sc2Z3Z25qZ3Vuc21lbWVhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mzc2Njg4MjAsImV4cCI6MjA1MzI0NDgyMH0.FgUUh9aNMPicG3iqI3XnG9tiFvT-LHfRDebYb4U9T2U"

def convert_date(date_str):
    """Convert date string to ISO format."""
    if pd.isna(date_str):
        return None
    try:
        return datetime.strptime(date_str, '%m/%d/%y').strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

def clean_data_for_json(value):
    """Clean data to ensure it's JSON serializable."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return value
    return str(value)

def process_csv_data(df):
    """Process and clean the CSV data."""
    # Select only the columns we need for the cattle_slaughter table
    columns_needed = [
        'office_name', 'office_code', 'office_city', 'office_state',
        'description', 'class', 'slaughter_date', 'volume', 'unit',
        'section', 'type', 'region'
    ]
    
    df = df[columns_needed]
    
    # Convert date columns
    df['slaughter_date'] = df['slaughter_date'].apply(convert_date)
    
    # Convert volume to numeric
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    
    # Clean all columns to ensure JSON serializable
    for column in df.columns:
        df[column] = df[column].apply(clean_data_for_json)
    
    return df

def upload_to_supabase(csv_file):
    """Upload CSV data to Supabase."""
    try:
        # Initialize Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Read CSV file
        logger.info(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        
        # Process the data
        df = process_csv_data(df)
        
        # Convert DataFrame to list of dictionaries and ensure JSON serializable
        records = json.loads(df.to_json(orient='records'))
        
        # Upload data in batches
        batch_size = 100  # Reduced batch size
        total_records = len(records)
        
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            logger.info(f"Uploading batch {i//batch_size + 1} of {(total_records + batch_size - 1)//batch_size}")
            
            try:
                # Log the first record of each batch for debugging
                logger.debug(f"Sample record: {json.dumps(batch[0], indent=2)}")
                
                response = supabase.table('cattle_slaughter').insert(batch).execute()
                
                if hasattr(response, 'error') and response.error is not None:
                    logger.error(f"Error uploading batch: {response.error}")
                    raise Exception(f"Supabase upload error: {response.error}")
                
                logger.info(f"Successfully uploaded batch of {len(batch)} records")
                
            except Exception as batch_error:
                logger.error(f"Error uploading batch: {str(batch_error)}")
                logger.error(f"Problematic batch data: {json.dumps(batch[0], indent=2)}")
                raise
        
        logger.info(f"Successfully uploaded {total_records} records to Supabase")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

def main():
    """Main execution function."""
    # Get the most recent CSV file in the current directory
    csv_files = [f for f in os.listdir('.') if f.startswith('cattle_slaughter_') and f.endswith('.csv')]
    if not csv_files:
        logger.error("No cattle slaughter CSV files found")
        return
    
    latest_csv = max(csv_files)
    logger.info(f"Found latest CSV file: {latest_csv}")
    
    try:
        upload_to_supabase(latest_csv)
        logger.info("Data upload completed successfully")
    except Exception as e:
        logger.error(f"Failed to upload data: {str(e)}")

if __name__ == "__main__":
    main()
