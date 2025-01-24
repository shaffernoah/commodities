import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json
import time

# Load environment variables
load_dotenv()

class CommodityFetcher:
    def __init__(self, api_key=None):
        """Initialize the CommodityFetcher with API credentials"""
        self.api_key = api_key or os.getenv('COMMODITIES_API_KEY')
        self.base_url = "https://commodities-api.com/api"
        self.max_symbols_per_request = 2  # API limitation
        
        if not self.api_key:
            raise ValueError("API key is required. Set it in .env file or pass it to the constructor.")

    def fetch_latest_prices_batch(self, symbols):
        """
        Fetch latest prices for a batch of symbols
        
        Args:
            symbols (list): List of commodity symbols to fetch
            
        Returns:
            tuple: (prices_dict, units_dict)
        """
        # Convert symbols list to comma-separated string
        symbols_str = ','.join(symbols)

        # Prepare request parameters
        params = {
            'access_key': self.api_key,
            'base': 'USD',
            'symbols': symbols_str
        }

        try:
            # Make API request
            response = requests.get(f"{self.base_url}/latest", params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Print full response for debugging
            print(f"Latest Prices Response for {symbols_str}:", json.dumps(data, indent=2))
            
            # Extract the nested data structure
            if 'data' in data and data['data'].get('success', False):
                return data['data'].get('rates', {}), data['data'].get('unit', {})
            else:
                error_msg = data.get('error', {}).get('info', 'Unknown error occurred')
                raise Exception(f"API request failed: {error_msg}")
            
        except Exception as e:
            print(f"Error fetching latest prices for {symbols_str}: {str(e)}")
            return {}, {}

    def fetch_latest_prices(self, symbols):
        """
        Fetch latest prices for all symbols in batches
        
        Args:
            symbols (list): List of commodity symbols to fetch
            
        Returns:
            tuple: (prices_dict, units_dict)
        """
        all_prices = {}
        all_units = {}
        
        # Process symbols in batches
        for i in range(0, len(symbols), self.max_symbols_per_request):
            batch = symbols[i:i + self.max_symbols_per_request]
            prices, units = self.fetch_latest_prices_batch(batch)
            all_prices.update(prices)
            all_units.update(units)
            time.sleep(1)  # Add delay between batch requests
        
        return all_prices, all_units

    def fetch_commodity_prices(self, start_date, end_date, symbol, max_retries=3):
        """
        Fetch commodity prices for a single symbol over a given date range
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            symbol (str): Commodity symbol to fetch
            max_retries (int): Maximum number of retry attempts
            
        Returns:
            pandas.DataFrame: DataFrame with dates and prices for the commodity
        """
        # Prepare request parameters
        params = {
            'access_key': self.api_key,
            'start_date': start_date,
            'end_date': end_date,
            'base': 'USD',
            'symbols': symbol
        }

        for attempt in range(max_retries):
            try:
                # Make API request with increased timeout
                response = requests.get(f"{self.base_url}/timeseries", params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Print full response for debugging
                print(f"Timeseries Response for {symbol}:", json.dumps(data, indent=2))
                
                # Check if the API request was successful and handle nested structure
                if 'data' in data and data['data'].get('success', False):
                    rates_data = data['data'].get('rates', {})
                    unit = data['data'].get('unit', {}).get(symbol)
                else:
                    error_msg = data.get('error', {}).get('info', 'Unknown error occurred')
                    raise Exception(f"API request failed: {error_msg}")
                
                # Convert to DataFrame
                df = pd.DataFrame.from_dict(rates_data, orient='index')
                df.index = pd.to_datetime(df.index)
                df.index.name = 'date'
                
                # Rename the column to include the symbol
                if symbol in df.columns:
                    df = df[[symbol]]  # Keep only the symbol column
                    df.columns = [f"{symbol}_price"]
                    df[f"{symbol}_unit"] = unit
                
                # Sort by date
                df = df.sort_index()
                
                return df
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    print(f"Timeout occurred. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                raise Exception("API request timed out after multiple retries")
            except requests.exceptions.RequestException as e:
                raise Exception(f"Error fetching data from API: {str(e)}")
            except json.JSONDecodeError:
                raise Exception("Error decoding API response")
            except Exception as e:
                raise Exception(f"Unexpected error: {str(e)}")

    def fetch_all_commodities(self, start_date, end_date, symbols):
        """
        Fetch historical data for multiple commodities by making separate requests for each
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            symbols (list): List of commodity symbols to fetch
            
        Returns:
            pandas.DataFrame: Combined DataFrame with all commodity prices
        """
        all_data = []
        
        for symbol in symbols:
            try:
                print(f"\nFetching data for {symbol}...")
                df = self.fetch_commodity_prices(start_date, end_date, symbol)
                all_data.append(df)
                time.sleep(1)  # Add a small delay between requests
            except Exception as e:
                print(f"Error fetching {symbol}: {str(e)}")
                continue
        
        if not all_data:
            raise Exception("Failed to fetch data for any symbols")
        
        # Combine all DataFrames
        combined_df = pd.concat(all_data, axis=1)
        return combined_df

    def save_to_csv(self, df, filename):
        """Save the DataFrame to a CSV file"""
        try:
            df.to_csv(filename)
            print(f"Data saved to {filename}")
        except Exception as e:
            raise Exception(f"Error saving data to CSV: {str(e)}")

def main():
    # Initialize fetcher with API key
    api_key = "8y4x3c0dha78p74l97pv4ka3mw08bm3buhjrxxziorznk3bp0oh52gxci3qk"
    fetcher = CommodityFetcher(api_key)
    
    try:
        # First try fetching latest prices to verify symbols
        print("Fetching latest prices to verify available symbols...")
        requested_symbols = ['BEEF', 'FC00', 'GFU22', 'GF', 'LCAT', 'LC00', 'CORN', 'CZ25']
        latest_prices, units = fetcher.fetch_latest_prices(requested_symbols)
        
        print("\nAvailable symbols and their units:")
        for symbol in requested_symbols:
            if symbol in latest_prices:
                print(f"{symbol}: {latest_prices[symbol]} {units.get(symbol, '')}")
        
        # Set date range
        start_date = "2024-12-25"
        end_date = "2025-01-23"
        
        # Use symbols that we confirmed are available
        available_symbols = [sym for sym in requested_symbols if sym in latest_prices]
        if not available_symbols:
            raise Exception("None of the requested symbols are available in the API")
        
        print(f"\nFetching historical data for symbols: {', '.join(available_symbols)}")
        print(f"Date range: {start_date} to {end_date}")
        
        # Fetch data for all commodities
        df = fetcher.fetch_all_commodities(start_date, end_date, available_symbols)
        
        # Display the data
        print("\nCommodity Prices:")
        print(df)
        
        # Save to CSV
        output_file = f"commodity_prices_{start_date}_to_{end_date}.csv"
        fetcher.save_to_csv(df, output_file)
        
        # Print summary of available and unavailable symbols
        print("\nSummary:")
        print("Available symbols:", ", ".join(available_symbols))
        unavailable = [sym for sym in requested_symbols if sym not in available_symbols]
        if unavailable:
            print("Unavailable symbols:", ", ".join(unavailable))
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
