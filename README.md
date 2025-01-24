# Commodities Dashboard

A comprehensive dashboard for analyzing commodity prices and cattle slaughter data using Streamlit and Supabase.

## Features

- **Cattle Slaughter Analysis**
  - Daily trends visualization
  - Composition analysis by class
  - Weight distribution analysis
  - Key metrics and insights

- **Commodity Price Analysis**
  - Interactive price trend visualization
  - Multi-commodity comparison
  - Price statistics and change analysis
  - Correlation analysis between commodities

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/commodities.git
cd commodities
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file with your Supabase credentials:
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

4. Run the application:
```bash
streamlit run app.py
```

## Data Sources

- Cattle slaughter data from USDA APIs
- Commodity price data from various market sources
- Data stored and managed in Supabase

## Project Structure

- `app.py`: Main Streamlit dashboard application
- `commodity_fetcher.py`: Script for fetching commodity price data
- `usda_historical_fetcher.py`: Script for fetching historical USDA data
- `supabase_uploader.py`: Script for uploading data to Supabase

## Dependencies

- Python 3.8+
- Streamlit
- Pandas
- Plotly
- Supabase Python Client
- python-dotenv

## Contributing

Feel free to open issues or submit pull requests for any improvements.

## License

MIT License
