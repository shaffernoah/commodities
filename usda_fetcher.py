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

class USDAReportFetcher:
    """Fetches data from the USDA MARS API."""
    
    BASE_URL = 'https://marsapi.ams.usda.gov'
    API_VERSION = 'v1.2'  # Updated API version from 1.1 to 1.2
    
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

    def get_report_metadata(self, slug_id: str) -> Dict:
        """Get metadata for a specific report."""
        endpoint = f'services/{self.API_VERSION}/reports/{slug_id}'
        return self._make_request(endpoint)

    def get_report_details(self, slug_id: str, report_date: str, section: str = None) -> Dict:
        """Get detailed data for a specific report and date."""
        endpoint = f'services/{self.API_VERSION}/reports/{slug_id}/details'
        params = {
            'report_date': report_date,
            'format': 'json'
        }
        if section:
            params['section'] = section
        return self._make_request(endpoint, params)

    def get_report_prices(self, slug_id: str, report_date: str) -> Dict:
        """Get price data for a specific report and date."""
        endpoint = f'services/{self.API_VERSION}/reports/{slug_id}/prices'
        params = {
            'report_date': report_date,
            'format': 'json'
        }
        return self._make_request(endpoint, params)

    def get_available_reports(self) -> Dict:
        """Get a list of all available reports."""
        endpoint = f'services/{self.API_VERSION}/reports'
        return self._make_request(endpoint)

    def fetch_and_save_report(self, slug_id: str = "2460") -> None:
        """Fetch a specific report and save it to CSV."""
        try:
            # Get report metadata first
            logger.info(f"Fetching metadata for report {slug_id}...")
            metadata = self.get_report_metadata(slug_id)
            
            # Log the full metadata response for debugging
            logger.info("Full metadata response:")
            logger.info(json.dumps(metadata, indent=2))
            
            if not metadata.get('results'):
                logger.error("No report metadata found")
                logger.info("Available fields in metadata response:")
                for key in metadata.keys():
                    logger.info(f"- {key}")
                return
            
            # Get the most recent report
            latest_report = metadata['results'][0]
            report_date = latest_report['report_date']
            
            logger.info(f"Latest report info:")
            logger.info(f"Title: {latest_report['report_title']}")
            logger.info(f"Date: {report_date}")
            logger.info(f"Status: {latest_report['final_ind']}")
            
            # Get both details and prices
            logger.info(f"Fetching report details for date {report_date}...")
            details = self.get_report_details(slug_id, report_date)
            
            logger.info(f"Fetching price data for date {report_date}...")
            prices = self.get_report_prices(slug_id, report_date)
            
            logger.debug("Raw Details Response:")
            logger.debug(json.dumps(details, indent=2))
            
            logger.debug("Raw Prices Response:")
            logger.debug(json.dumps(prices, indent=2))
            
            # Process the data
            rows = []
            
            # Try to get data from details first
            if details.get('results'):
                for item in details['results']:
                    if isinstance(item, dict) and 'report_data' in item:
                        for data in item['report_data']:
                            row = {
                                'slug_id': slug_id,
                                'report_date': report_date,
                                'office_name': item.get('office_name'),
                                'office_code': item.get('office_code'),
                                'commodity_desc': data.get('commodity_desc'),
                                'price': data.get('price'),
                                'price_range': data.get('price_range'),
                                'unit_of_measure': data.get('unit_of_measure'),
                                'comments': data.get('comments', '')
                            }
                            rows.append(row)
            
            # If no data from details, try prices
            if not rows and prices.get('results'):
                for item in prices['results']:
                    if isinstance(item, dict):
                        row = {
                            'slug_id': slug_id,
                            'report_date': report_date,
                            'office_name': item.get('office_name'),
                            'office_code': item.get('office_code'),
                            'commodity_desc': item.get('commodity_desc'),
                            'price': item.get('price'),
                            'price_range': item.get('price_range'),
                            'unit_of_measure': item.get('unit_of_measure'),
                            'comments': item.get('comments', '')
                        }
                        rows.append(row)
            
            if not rows:
                logger.error("No price data found in either details or prices endpoints")
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(rows)
            
            # Save to CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'usda_report_{slug_id}_{timestamp}.csv'
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
            
            # Display first few rows
            logger.info("\nFirst few rows of the data:")
            print(df.head())
            
        except Exception as e:
            logger.error(f"Error fetching report: {str(e)}")
            raise

    def get_cattle_reports(self) -> List[Dict]:
        """Get all available cattle-related reports."""
        all_reports = self._make_request(f'services/{self.API_VERSION}/reports')
        
        # Filter for cattle-related reports
        cattle_reports = []
        cattle_keywords = ['cattle', 'beef', 'feeder', 'slaughter', 'replacement cattle', 'yearling']
        
        for report in all_reports:
            title = report.get('report_title', '').lower()
            if any(keyword in title for keyword in cattle_keywords):
                cattle_reports.append(report)
        
        return cattle_reports

    def fetch_multiple_reports(self, reports: List[Dict]) -> pd.DataFrame:
        """Fetch data for multiple reports and combine into a single DataFrame."""
        all_data = []
        
        for report in reports:
            slug_id = report['slug_id']
            report_date = report['report_date']
            
            try:
                # Get both details and prices
                details = self.get_report_details(slug_id, report_date)
                prices = self.get_report_prices(slug_id, report_date)
                
                # Process details data
                if details.get('results'):
                    for item in details['results']:
                        if isinstance(item, dict) and 'report_data' in item:
                            for data in item['report_data']:
                                row = {
                                    'slug_id': slug_id,
                                    'report_title': report['report_title'],
                                    'report_date': report_date,
                                    'office_name': item.get('office_name'),
                                    'office_code': item.get('office_code'),
                                    'commodity_desc': data.get('commodity_desc'),
                                    'price': data.get('price'),
                                    'price_range': data.get('price_range'),
                                    'unit_of_measure': data.get('unit_of_measure'),
                                    'comments': data.get('comments', '')
                                }
                                all_data.append(row)
                
                # Process prices data
                if prices.get('results'):
                    for item in prices['results']:
                        if isinstance(item, dict):
                            row = {
                                'slug_id': slug_id,
                                'report_title': report['report_title'],
                                'report_date': report_date,
                                'office_name': item.get('office_name'),
                                'office_code': item.get('office_code'),
                                'commodity_desc': item.get('commodity_desc'),
                                'price': item.get('price'),
                                'price_range': item.get('price_range'),
                                'unit_of_measure': item.get('unit_of_measure'),
                                'comments': item.get('comments', '')
                            }
                            all_data.append(row)
                            
            except Exception as e:
                logger.error(f"Error fetching report {slug_id}: {str(e)}")
                continue
        
        if not all_data:
            logger.error("No data found in any of the reports")
            return pd.DataFrame()
        
        return pd.DataFrame(all_data)

    def list_report_sections(self, slug_id: str) -> List[str]:
        """List available sections for a report."""
        endpoint = f'/services/{self.API_VERSION}/reports/{slug_id}'
        try:
            response = self._make_request(endpoint)
            if response.get('results') and response['results']:
                report = response['results'][0]
                sections = []
                # Try to find sections in the response
                for key in report.keys():
                    if key.startswith('section_'):
                        section_name = report[key]
                        if section_name:
                            sections.append(section_name)
                return sections
        except Exception as e:
            logger.error(f"Error listing report sections: {str(e)}")
            raise
        return []

    def fetch_federal_inspection_slaughter(self, report_date: Optional[str] = None) -> pd.DataFrame:
        """
        Fetches the Actual Slaughter Under Federal Inspection report (Slug ID: 3658).
        
        Args:
            report_date: Optional date string in format 'MM/DD/YYYY'. If None, fetches most recent report.
        
        Returns:
            pandas DataFrame containing the slaughter report data
        """
        slug_id = "3658"  # Actual Slaughter Under Federal Inspection report
        
        try:
            # First get the report metadata to get the latest date if not specified
            metadata_endpoint = f'/services/{self.API_VERSION}/reports/{slug_id}'
            metadata = self._make_request(metadata_endpoint)
            
            if not metadata.get('results'):
                logger.warning("No metadata found for Federal Inspection Slaughter report")
                return pd.DataFrame()
            
            # Get the report date and dates
            if report_date:
                target_date = report_date
                # Find the report that matches this date
                matching_report = None
                for report in metadata['results']:
                    if report['report_date'] == target_date:
                        matching_report = report
                        break
                
                if matching_report is None:
                    logger.warning(f"No report found for date: {target_date}")
                    return pd.DataFrame()
                
                begin_date = matching_report['report_begin_date']
                end_date = matching_report['report_end_date']
            else:
                # Use the most recent report's dates
                latest_report = metadata['results'][0]
                target_date = latest_report['report_date']
                begin_date = latest_report['report_begin_date']
                end_date = latest_report['report_end_date']
            
            logger.info(f"Fetching data for report date: {target_date} (begin: {begin_date}, end: {end_date})")
            
            # Use the correct report sections
            sections = [
                "Report FIS Species",
                "Report FIS Cattle",
                "Report FIS Meat Production",
                "Report FIS Head Percent",
                "Report FIS Region"
            ]
            
            all_data = []
            for section in sections:
                endpoint = f'/services/{self.API_VERSION}/reports/{slug_id}/{section}'
                params = {'q': f'report_end_date={end_date}'}
                
                try:
                    section_data = self._make_request(endpoint, params=params)
                    logger.debug(f"Response for section {section}:")
                    logger.debug(json.dumps(section_data, indent=2))
                    
                    if section_data.get('results'):
                        # Add section name and dates to each record for better organization
                        for item in section_data['results']:
                            item['section'] = section
                            item['report_date'] = target_date
                            item['report_begin_date'] = begin_date
                            item['report_end_date'] = end_date
                        all_data.extend(section_data['results'])
                        logger.info(f"Retrieved {len(section_data['results'])} records from {section}")
                    else:
                        logger.warning(f"No data found in section {section}")
                except Exception as e:
                    logger.warning(f"Error fetching section {section}: {str(e)}")
                    continue
            
            if not all_data:
                logger.warning(f"No data found for date: {target_date}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(all_data)
            
            # Save to CSV with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'federal_inspection_slaughter_{timestamp}.csv'
            df.to_csv(csv_filename, index=False)
            logger.info(f"Saved Federal Inspection Slaughter data to {csv_filename}")
            
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
            
        except Exception as e:
            logger.error(f"Error fetching Federal Inspection Slaughter report: {str(e)}")
            raise

def main():
    """Main execution function."""
    # Use the provided API key
    api_key = "LIm1Mr7tz2NzD4WkYqfv/AsGFAqQevNgoldYbrjdpbs="
    
    try:
        # Initialize the fetcher
        fetcher = USDAReportFetcher(api_key)
        
        # Fetch data for a specific date
        logger.info("Fetching Federal Inspection Slaughter report...")
        slaughter_data = fetcher.fetch_federal_inspection_slaughter("12/30/2024")
        
        if not slaughter_data.empty:
            # Filter for cattle data
            cattle_data = slaughter_data[
                slaughter_data['commodity'].str.contains('Cattle', na=False)
            ].copy()
            
            # Get daily slaughter numbers (All category)
            daily_slaughter = cattle_data[
                (cattle_data['class'] == 'All') & 
                (cattle_data['slaughter_date'].notna()) &
                (cattle_data['region'].isna())  # Exclude regional breakdowns
            ][['slaughter_date', 'volume', 'unit']].copy()
            
            # Get weight and production metrics
            weight_production = cattle_data[
                (cattle_data['class'] == 'All') &
                (cattle_data['description'].isin([
                    'Average Live Weight',
                    'Average Dressed Weight',
                    'Total Red Meat Production',
                    'Head Slaughtered'
                ]))
            ][['description', 'volume', 'unit']].copy()
            
            # Display results
            print("\nDaily Cattle Slaughter (All Categories):")
            print(daily_slaughter.sort_values('slaughter_date'))
            
            print("\nWeight and Production Metrics:")
            print(weight_production)
            
            # Save to CSV with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save daily slaughter data
            daily_csv = f'cattle_daily_slaughter_{timestamp}.csv'
            daily_slaughter.to_csv(daily_csv, index=False)
            logger.info(f"\nSaved daily cattle slaughter data to {daily_csv}")
            
            # Save weight and production data
            metrics_csv = f'cattle_metrics_{timestamp}.csv'
            weight_production.to_csv(metrics_csv, index=False)
            logger.info(f"Saved cattle weight and production metrics to {metrics_csv}")
            
        else:
            logger.warning("No Federal Inspection Slaughter data found")
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()