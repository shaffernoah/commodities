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
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcm5sc2Z3Z25qZ3Vuc21lbWVhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczNzY2ODgyMCwiZXhwIjoyMDUzMjQ0ODIwfQ.xM0ASxXK7IkrOdt_4VP_dTLJj5kpedBP3KKJSc1SS2k"

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
    """Process and clean the CSV data into separate dataframes for different tables."""
    # Create separate dataframes for each section
    dfs = {}
    
    # Process FIS Species data (main cattle_slaughter table)
    species_df = df[df['section'] == 'Report FIS Species'].copy()
    species_df = species_df[
        (species_df['slaughter_date'] != 'All') & 
        (species_df['slaughter_date'].notna()) &
        (species_df['unit'] != 'Pct')
    ]
    
    # Process Meat Production data
    meat_prod_df = df[df['section'] == 'Report FIS Meat Production'].copy()
    
    # Process Head Percent data
    head_pct_df = df[df['section'] == 'Report FIS Head Percent'].copy()
    
    # Process Region data
    region_df = df[df['section'] == 'Report FIS Region'].copy()
    
    # Convert date columns for each dataframe
    date_columns = ['report_date', 'report_begin_date', 'report_end_date', 'published_date', 'slaughter_date']
    
    # Process each dataframe
    for name, df_subset in {
        'cattle_slaughter': species_df,
        'meat_production': meat_prod_df,
        'head_percent': head_pct_df,
        'region_data': region_df
    }.items():
        if not df_subset.empty:
            # Convert date columns
            for date_col in date_columns:
                if date_col in df_subset.columns:
                    df_subset[date_col] = pd.to_datetime(df_subset[date_col], errors='coerce')
            
            # Convert volume to numeric
            df_subset['volume'] = pd.to_numeric(df_subset['volume'], errors='coerce')
            
            # Clean all columns to ensure JSON serializable
            for column in df_subset.columns:
                df_subset[column] = df_subset[column].apply(clean_data_for_json)
            
            dfs[name] = df_subset
    
    return dfs

def upload_to_supabase(csv_file):
    """Upload CSV data to Supabase."""
    try:
        # Initialize Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Read CSV file
        logger.info(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        
        # Process the data into separate dataframes
        dataframes = process_csv_data(df)
        
        # Upload each dataframe to its respective table
        for table_name, df in dataframes.items():
            if df is not None and not df.empty:
                logger.info(f"Processing {table_name} data...")
                
                # Convert DataFrame to list of dictionaries and ensure JSON serializable
                records = json.loads(df.to_json(orient='records'))
                
                # Upload data in batches
                batch_size = 100
                total_records = len(records)
                
                for i in range(0, total_records, batch_size):
                    batch = records[i:i + batch_size]
                    logger.info(f"Uploading batch {i//batch_size + 1} of {(total_records + batch_size - 1)//batch_size} to {table_name}")
                    
                    try:
                        data = supabase.table(table_name).insert(batch).execute()
                        logger.info(f"Successfully uploaded batch to {table_name}")
                    except Exception as e:
                        logger.error(f"Error uploading batch to {table_name}: {str(e)}")
                        raise
                        
        logger.info("Upload completed successfully")
        
    except Exception as e:
        logger.error(f"Error in upload process: {str(e)}")
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
