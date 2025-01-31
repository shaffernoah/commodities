import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client
import requests  # Add this at the top with other imports

# Configure Streamlit page
st.set_page_config(layout="wide", page_title="USDA Agricultural Data Analysis")

# Load environment variables
load_dotenv()

# Initialize Supabase client
try:
    st.write("Attempting to access secrets...")
    SUPABASE_URL = st.secrets["SUPABASE_URL"].strip()  # Remove any whitespace
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"].strip()
    st.write("Successfully loaded secrets!")
    st.write(f"Debug: Supabase URL: {SUPABASE_URL}")
except Exception as e:
    st.error(f"Error accessing secrets: {str(e)}")
    st.write("Please make sure secrets are configured in Streamlit Cloud in this format:")
    st.code("""
[secrets]
SUPABASE_URL = "your-url-here"
SUPABASE_KEY = "your-key-here"
    """)

# Create Supabase client at the module level
try:
    st.write("Debug: Creating global Supabase client...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    st.write("Debug: Successfully created Supabase client")
except Exception as e:
    st.error(f"Error creating Supabase client: {str(e)}")

# Custom CSS
st.markdown("""
    <style>
        .stApp {
            max-width: 1200px;
            margin: 0 auto;
        }
        .metric-card {
            background-color: #f0f2f6;
            border-radius: 10px;
            padding: 20px;
            text-align: center;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_cattle_data():
    try:
        st.write("Debug: Testing basic HTTP connectivity...")
        try:
            test_url = SUPABASE_URL.replace('https://', 'https://ping.')
            st.write(f"Debug: Testing connection to: {test_url}")
            test_response = requests.get(test_url, timeout=5)
            st.write(f"Debug: HTTP test response: {test_response.status_code}")
        except Exception as http_e:
            st.write(f"Debug: HTTP test failed: {type(http_e).__name__}: {str(http_e)}")
        
        st.write("Debug: Creating Supabase client...")
        st.write(f"Debug: Using URL: {SUPABASE_URL}")  # Show full URL for debugging
        
        st.write("Debug: Attempting to query cattle_slaughter table...")
        response = supabase.table('cattle_slaughter').select("*").execute()
        
        if response.data:
            st.write(f"Debug: Successfully retrieved {len(response.data)} records")
            df = pd.DataFrame(response.data)
            
            # Debug information about the data
            st.write("Debug: Cattle data columns:")
            st.write(df.columns.tolist())
            st.write("\nDebug: Sample data:")
            st.write(df.head())
            
            df['slaughter_date'] = pd.to_datetime(df['slaughter_date'])
            
            # Ensure numeric columns are properly typed
            numeric_columns = ['total_cattle', 'avg_live_weight', 'avg_dressed_weight', 'total_meat_production']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        st.write("Debug: No data found in the response")
        return None
    except Exception as e:
        st.error(f"Error loading cattle data: {str(e)}")
        st.write(f"Debug: Error type: {type(e).__name__}")
        return None

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_commodity_data():
    """Load commodity data from Supabase"""
    try:
        st.write("Debug: Creating Supabase client for commodities...")
        
        st.write("Debug: Attempting to query commodities_data table...")
        response = supabase.table('commodities_data').select('*').execute()
        
        if response.data:
            st.write(f"Debug: Successfully retrieved {len(response.data)} commodity records")
            df = pd.DataFrame(response.data)
            
            # Convert date column
            df['date'] = pd.to_datetime(df['date'])
            
            # Convert price columns to numeric
            price_columns = [col for col in df.columns if col.endswith('_price')]
            for col in price_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            st.write("Debug: Commodity data columns:", df.columns.tolist())
            return df
            
        st.write("Debug: No data in Supabase")
        return None
        
    except Exception as e:
        st.error(f"Error loading commodity data: {str(e)}")
        st.write(f"Debug: Error type: {type(e).__name__}")
        return None

def display_commodity_analysis(df, date_range):
    if df is None or df.empty:
        st.warning("No commodity data available for analysis.")
        return
        
    try:
        # Convert date_range to pandas timestamps
        start_date = pd.Timestamp(date_range[0])
        end_date = pd.Timestamp(date_range[1])
        
        # Create a date filter using datetime objects
        mask = (df['date'].dt.date >= start_date.date()) & (df['date'].dt.date <= end_date.date())
        filtered_df = df[mask]
        
        if filtered_df.empty:
            st.warning("No data available for the selected date range.")
            return
            
        # Get all commodities
        commodities = [col.split('_')[0] for col in df.columns if col.endswith('_price')]
        
        # Create metrics for each commodity with price changes
        st.markdown("### 📊 Commodity Prices and Changes")
        
        # Calculate price changes
        metrics = []
        for commodity in commodities:
            price_col = f"{commodity}_price"
            unit_col = f"{commodity}_unit"
            
            if price_col in filtered_df.columns:
                latest_price = filtered_df[price_col].iloc[-1]
                prev_price = filtered_df[price_col].iloc[-2] if len(filtered_df) > 1 else latest_price
                price_change = ((latest_price - prev_price) / prev_price) * 100
                unit = filtered_df[unit_col].iloc[-1] if unit_col in filtered_df.columns else ''
                
                metrics.append({
                    'commodity': commodity,
                    'price': latest_price,
                    'change': price_change,
                    'unit': unit
                })
        
        # Display metrics in a grid
        cols = st.columns(len(metrics))
        for idx, metric in enumerate(metrics):
            with cols[idx]:
                st.button(
                    f"{metric['commodity']}\n${metric['price']:.2f}\n{metric['change']:+.1f}%",
                    key=f"btn_{metric['commodity']}",
                    help=f"Click to show/hide {metric['commodity']} in the chart"
                )
        
        # Multi-select for commodities to display
        selected_commodities = st.multiselect(
            "Select commodities to compare",
            commodities,
            default=[commodities[0]]
        )
        
        if selected_commodities:
            # Create combined price trend chart
            fig = go.Figure()
            
            for commodity in selected_commodities:
                price_col = f"{commodity}_price"
                unit_col = f"{commodity}_unit"
                unit = filtered_df[unit_col].iloc[-1] if unit_col in filtered_df.columns else ''
                
                fig.add_trace(go.Scatter(
                    x=filtered_df['date'],
                    y=filtered_df[price_col],
                    name=f"{commodity} ({unit})",
                    mode='lines'
                ))
            
            fig.update_layout(
                title="Commodity Price Trends",
                plot_bgcolor='white',
                xaxis=dict(
                    title="Date",
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    tickformat="%Y-%m-%d"
                ),
                yaxis=dict(
                    title="Price ($)",
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    tickprefix="$"
                ),
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Correlation Analysis
            if len(selected_commodities) > 1:
                st.markdown("### 📊 Correlation Analysis")
                
                # Create correlation matrix
                price_cols = [f"{commodity}_price" for commodity in selected_commodities]
                corr_df = filtered_df[price_cols].corr()
                
                # Create heatmap
                fig = px.imshow(
                    corr_df,
                    labels=dict(color="Correlation"),
                    x=selected_commodities,
                    y=selected_commodities,
                    color_continuous_scale="RdBu",
                    aspect="auto"
                )
                fig.update_layout(
                    title="Price Correlation Matrix",
                    plot_bgcolor='white'
                )
                st.plotly_chart(fig, use_container_width=True)
                
    except Exception as e:
        st.error(f"Error in commodity analysis: {str(e)}")
        st.write("Debug Info:")
        st.write(f"Date range type: {type(date_range)}")
        st.write(f"Date range values: {date_range}")
        if 'date' in df.columns:
            st.write(f"DataFrame date column type: {df['date'].dtype}")
            st.write("Sample dates from DataFrame:")
            st.write(df['date'].head())

def display_cattle_metrics(df, selected_class):
    """Display metrics and charts for cattle data."""
    if df.empty:
        st.warning("No data available for the selected date range and class.")
        return

    # Filter for weight-related records
    weight_data = df[
        (df['description'].str.contains('Weight', case=False, na=False)) &
        (df['volume'].notna())
    ].copy()

    if weight_data.empty:
        st.warning("No weight data available for the selected criteria.")
        return

    # Create pivot table with dates as index and weight types as columns
    pivot_data = weight_data.pivot_table(
        index='slaughter_date',
        columns='description',
        values='volume',
        aggfunc='mean'
    ).reset_index()

    # Ensure pivot_data has data
    if pivot_data.empty or len(pivot_data.columns) < 2:
        st.warning("Insufficient data to calculate metrics.")
        return

    # Calculate rolling averages for numeric columns only
    numeric_cols = pivot_data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        pivot_data[f'{col}_7day_avg'] = pivot_data[col].rolling(window=7, min_periods=1).mean()

    # Prepare data for plotting
    plot_data = pd.melt(
        pivot_data,
        id_vars=['slaughter_date'],
        value_vars=[col for col in pivot_data.columns if col != 'slaughter_date']
    )

    # Clean up series names for display
    plot_data['series'] = plot_data['variable'].str.replace('_7day_avg', ' (7-day avg)')

    # Create the plot
    fig = px.line(
        plot_data,
        x='slaughter_date',
        y='value',
        color='series',
        title=f'Cattle Weights Over Time - {selected_class}',
        labels={'value': 'Weight', 'slaughter_date': 'Date'}
    )

    st.plotly_chart(fig)

def main():
    st.title("🌾 USDA Agricultural Data Analysis Dashboard")
    st.markdown("---")
    
    # Create tabs for different data types
    tab1, tab2, tab3 = st.tabs(["Cattle Slaughter Analysis", "Regional Analysis", "Commodity Analysis"])
    
    # Load data
    cattle_df = load_cattle_data()
    commodity_df = load_commodity_data()
    region_df = None
    
    # Initialize date range variables
    today = datetime.today().date()
    min_date = today
    max_date = today
    
    # Update date range based on available data
    if cattle_df is not None and not cattle_df.empty:
        if 'date' in cattle_df.columns:
            cattle_min = cattle_df['date'].min().date()
            cattle_max = cattle_df['date'].max().date()
            min_date = min(min_date, cattle_min)
            max_date = max(max_date, cattle_max)
    
    if commodity_df is not None and not commodity_df.empty:
        if 'date' in commodity_df.columns:
            commodity_min = commodity_df['date'].min().date()
            commodity_max = commodity_df['date'].max().date()
            min_date = min(min_date, commodity_min)
            max_date = max(max_date, commodity_max)
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Species/Class filter
    available_classes = ['All']
    if cattle_df is not None and not cattle_df.empty and 'class' in cattle_df.columns:
        class_options = sorted([c for c in cattle_df['class'].unique() if c != 'All'])
        available_classes.extend(class_options)
    
    selected_class = st.sidebar.selectbox("Select Species/Class", available_classes)
    
    # Date range selector
    st.sidebar.markdown("---")
    st.sidebar.header("📅 Date Range")
    
    # Add option to show all data
    use_date_filter = st.sidebar.checkbox("Filter by Date Range", value=False)
    
    if use_date_filter:
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Ensure we have both start and end dates
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range
    else:
        start_date, end_date = min_date, max_date
    
    # Process cattle data
    with tab1:
        if cattle_df is None or cattle_df.empty:
            st.error("Unable to load cattle data. Please check your database connection.")
        else:
            # Filter cattle data
            filtered_cattle_df = cattle_df.copy()
            
            # Apply date filter
            filtered_cattle_df = filtered_cattle_df[
                (filtered_cattle_df['slaughter_date'].dt.date >= start_date) &
                (filtered_cattle_df['slaughter_date'].dt.date <= end_date)
            ]
            
            # Display metrics and charts
            display_cattle_metrics(filtered_cattle_df, selected_class)
    
    # Process commodity data
    with tab3:
        if commodity_df is not None and not commodity_df.empty:
            display_commodity_analysis(commodity_df, (start_date, end_date))
        else:
            st.error("No commodity data available for analysis.")

if __name__ == "__main__":
    main()
