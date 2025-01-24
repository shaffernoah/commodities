import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from typing import Dict, List, Optional, Union
import os
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class USDAHistoricalFetcher:
    """Fetches historical data from the USDA MARS API."""
    
    BASE_URL = 'https://marsapi.ams.usda.gov'
    API_VERSION = 'v1.2'
    
    def __init__(self, api_key: str):
        """Initialize the fetcher with API credentials."""
        self.api_key = api_key
        self.session = requests.Session()
        self.session.auth = (api_key, '')
        self.session.headers.update({
            'Accept': 'application/json'
        })

    def _handle_rate_limit(self, response: requests.Response) -> None:
        """Handle rate limiting with exponential backoff."""
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logger.warning(f"Rate limit hit. Waiting {retry_after} seconds...")
            time.sleep(retry_after)

    def _make_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> Dict:
        """Make an API request with retry logic."""
        url = urljoin(self.BASE_URL, endpoint)
        retries = 0
        
        while retries < max_retries:
            try:
                response = self.session.get(url, params=params)
                logger.debug(f"Making request to: {response.url}")
                
                if response.status_code == 200:
                    return response.json()
                
                if response.status_code == 429:
                    self._handle_rate_limit(response)
                    retries += 1
                    continue
                
                response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                retries += 1
                if retries == max_retries:
                    raise
                time.sleep(2 ** retries)  # Exponential backoff
        
        raise Exception("Max retries exceeded")

    def fetch_historical_data(self, slug_id: str, start_date: str, end_date: str, output_file: str = None) -> pd.DataFrame:
        """
        Fetch historical data for a given date range.
        
        Args:
            slug_id: Report ID to fetch
            start_date: Start date in format 'MM/DD/YYYY'
            end_date: End date in format 'MM/DD/YYYY'
            output_file: Optional filename to save the data
            
        Returns:
            pandas DataFrame containing the data
        """
        all_data = []
        
        try:
            # First get the report metadata to get all available dates
            metadata_endpoint = f'/services/{self.API_VERSION}/reports/{slug_id}'
            metadata = self._make_request(metadata_endpoint)
            
            if not metadata.get('results'):
                logger.warning("No metadata found for report")
                return pd.DataFrame()
            
            # Get all reports within our date range
            start_datetime = datetime.strptime(start_date, '%m/%d/%Y')
            end_datetime = datetime.strptime(end_date, '%m/%d/%Y')
            
            matching_reports = []
            for report in metadata['results']:
                report_end = datetime.strptime(report['report_end_date'], '%m/%d/%Y')
                if start_datetime <= report_end <= end_datetime:
                    matching_reports.append(report)
            
            if not matching_reports:
                logger.warning(f"No reports found between {start_date} and {end_date}")
                return pd.DataFrame()
            
            logger.info(f"Found {len(matching_reports)} reports in date range")
            
            sections = [
                "Report FIS Species",
                "Report FIS Cattle",
                "Report FIS Meat Production",
                "Report FIS Head Percent",
                "Report FIS Region"
            ]
            
            # Process each report
            for report in matching_reports:
                logger.info(f"Processing report for end date: {report['report_end_date']}")
                
                # Get data from each section
                for section in sections:
                    endpoint = f'/services/{self.API_VERSION}/reports/{slug_id}/{section}'
                    params = {'q': f'report_end_date={report["report_end_date"]}'}
                    
                    try:
                        section_data = self._make_request(endpoint, params=params)
                        
                        if section_data.get('results'):
                            # Add metadata and section info to each record
                            for item in section_data['results']:
                                item.update({
                                    'office_name': report.get('office_name'),
                                    'office_code': report.get('office_code'),
                                    'office_city': report.get('office_city'),
                                    'office_state': report.get('office_state'),
                                    'report_date': report.get('report_date'),
                                    'report_begin_date': report.get('report_begin_date'),
                                    'report_end_date': report.get('report_end_date'),
                                    'published_date': report.get('published_Date'),
                                    'market_type': report.get('market_type'),
                                    'slug_id': slug_id,
                                    'slug_name': f'AMS_{slug_id}',
                                    'report_title': report.get('report_title'),
                                    'section': section
                                })
                            all_data.extend(section_data['results'])
                            logger.info(f"Retrieved {len(section_data['results'])} records from {section}")
                    except Exception as e:
                        logger.warning(f"Error fetching section {section}: {str(e)}")
                        continue
                
                # Wait 1 second between reports to avoid rate limiting
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Save to file if specified
        if output_file and not df.empty:
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            
            # Print a summary of the data
            logger.info("\nData Summary:")
            logger.info(f"Total records: {len(df)}")
            logger.info("\nColumns available:")
            for col in df.columns:
                logger.info(f"- {col}")
            
            if 'section' in df.columns:
                section_counts = df['section'].value_counts()
                logger.info("\nRecords by section:")
                for section, count in section_counts.items():
                    logger.info(f"- {section}: {count} records")
        
        return df

def main():
    """Main execution function."""
    # Use the provided API key
    api_key = "LIm1Mr7tz2NzD4WkYqfv/AsGFAqQevNgoldYbrjdpbs="
    
    try:
        # Initialize fetcher
        fetcher = USDAHistoricalFetcher(api_key)
        
        # Set up logging
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Fetch data for specified date range
        df = fetcher.fetch_historical_data(
            slug_id="3658",  # Actual Slaughter Under Federal Inspection
            start_date="10/16/2024",
            end_date="12/27/2024",
            output_file="cattle_slaughter_oct16_dec27_2024.csv"
        )
        
        if not df.empty:
            logger.info(f"Successfully fetched {len(df)} records")
            # Display first few records with available columns
            logger.info("\nFirst few records:")
            logger.info(df[['report_date', 'report_begin_date', 'report_end_date', 'market_type']].head())
        else:
            logger.warning("No data found")
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
