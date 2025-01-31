import pandas as pd
from upload_to_supabase import upload_to_supabase, process_csv_data
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_upload():
    """Test the upload process with a small subset of data."""
    try:
        # Read the CSV file
        csv_file = 'cattle_slaughter_oct16_dec27_2024.csv'
        logger.info(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        
        # Take a small sample from each section
        sections = [
            'Report FIS Species',
            'Report FIS Meat Production',
            'Report FIS Head Percent',
            'Report FIS Region'
        ]
        
        # Create a sample dataframe with 5 rows from each section
        sample_dfs = []
        for section in sections:
            section_df = df[df['section'] == section].head(5)
            sample_dfs.append(section_df)
        
        sample_df = pd.concat(sample_dfs)
        
        # Save sample to a temporary CSV
        temp_csv = 'test_sample.csv'
        sample_df.to_csv(temp_csv, index=False)
        logger.info(f"Created test sample with {len(sample_df)} rows")
        
        # Process and upload the sample data
        logger.info("Starting test upload...")
        upload_to_supabase(temp_csv)
        logger.info("Test upload completed successfully!")
        
        # Print sample counts
        processed_dfs = process_csv_data(sample_df)
        for table_name, processed_df in processed_dfs.items():
            if processed_df is not None and not processed_df.empty:
                logger.info(f"{table_name}: {len(processed_df)} rows processed")
        
    except Exception as e:
        logger.error(f"Error in test upload: {str(e)}")
        raise

if __name__ == "__main__":
    test_upload()
