import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://qarnlsfwgnjgunsmemea.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Configure Streamlit page
st.set_page_config(
    page_title="USDA Agricultural Data Analysis",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    """Load cattle data from Supabase"""
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table('cattle_slaughter').select('*').execute()
        df = pd.DataFrame(response.data)
        
        # Convert dates - handle potential missing or invalid dates
        df['slaughter_date'] = pd.to_datetime(df['slaughter_date'], errors='coerce')
        
        # Drop rows where slaughter_date is null
        df = df.dropna(subset=['slaughter_date'])
        
        # Convert volume to numeric, handling percentage values
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Error loading cattle data: {str(e)}")
        return None

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_commodity_data():
    """Load commodity data from Supabase"""
    try:
        # First try loading from Supabase
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = supabase.table('commodities_data').select('*').execute()
        df = pd.DataFrame(response.data)
        
        # If Supabase data is empty, try loading from CSV
        if df.empty:
            print("No data in Supabase, loading from CSV...")
            df = pd.read_csv('commodity_prices_2024-12-25_to_2025-01-23.csv')
            
            # Convert wide format to long format
            commodity_symbols = ['BEEF', 'FC00', 'GFU22', 'GF', 'LCAT', 'LC00', 'CORN', 'CZ25']
            
            # Initialize lists to store transformed data
            records = []
            
            for _, row in df.iterrows():
                date = pd.to_datetime(row['date'])
                
                for symbol in commodity_symbols:
                    price_col = f'{symbol}_price'
                    unit_col = f'{symbol}_unit'
                    
                    if price_col in row and not pd.isna(row[price_col]):
                        records.append({
                            'date': date,
                            'commodity_symbol': symbol,
                            'price': float(row[price_col]),
                            'unit': row[unit_col] if unit_col in row and not pd.isna(row[unit_col]) else None
                        })
            
            df = pd.DataFrame(records)
        
        if df.empty:
            print("No data available from either source")
            return None
            
        # Print the columns we received
        print("Commodity data columns:", df.columns.tolist())
        
        # Convert dates if needed
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'created_at' in df.columns:
            df['date'] = pd.to_datetime(df['created_at'])
            
        # Ensure we have the required columns
        required_columns = ['date', 'commodity_symbol', 'price', 'unit']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            return None
        
        # Convert price to numeric
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Drop any rows with missing values
        df = df.dropna(subset=['date', 'price', 'commodity_symbol'])
        
        return df
    except Exception as e:
        import traceback
        print(f"Error loading commodity data: {str(e)}")
        print("Traceback:", traceback.format_exc())
        st.error(f"Error loading commodity data: {str(e)}")
        return None

def display_commodity_analysis(df, date_range):
    """Display commodity analysis section"""
    if df is None or df.empty:
        st.error("No commodity data available")
        return
    
    # Filter data by date range
    mask = (df['date'].dt.date >= date_range[0]) & (df['date'].dt.date <= date_range[1])
    filtered_df = df[mask]
    
    # Get unique commodities
    commodities = sorted(filtered_df['commodity_symbol'].unique())
    
    # Commodity selector
    selected_commodities = st.multiselect(
        "Select Commodities to Compare",
        options=commodities,
        default=commodities[:3] if len(commodities) > 0 else None
    )
    
    if not selected_commodities:
        st.warning("Please select at least one commodity")
        return
    
    # Filter for selected commodities
    commodity_data = filtered_df[filtered_df['commodity_symbol'].isin(selected_commodities)]
    
    # Display metrics
    st.subheader("💹 Current Prices")
    latest_date = commodity_data['date'].max()
    latest_prices = commodity_data[commodity_data['date'] == latest_date]
    
    # Create metric columns dynamically based on number of selected commodities
    cols = st.columns(min(len(selected_commodities), 4))
    for i, (_, row) in enumerate(latest_prices.iterrows()):
        col_idx = i % len(cols)
        with cols[col_idx]:
            st.metric(
                f"{row['commodity_symbol']}",
                f"{row['price']:.4f} {row['unit'] if pd.notna(row['unit']) else ''}"
            )
    
    # Price trends
    st.subheader("📈 Price Trends")
    
    # Create price trend chart
    fig = px.line(commodity_data, 
                  x='date', 
                  y='price',
                  color='commodity_symbol',
                  title="Commodity Price Trends",
                  labels={"price": "Price", "date": "Date", "commodity_symbol": "Commodity"})
    
    # Add units to hover text
    fig.update_traces(
        hovertemplate="<b>%{customdata}</b><br>" +
                      "Date: %{x}<br>" +
                      "Price: %{y:.4f} %{customdata[1]}<extra></extra>",
        customdata=commodity_data[['commodity_symbol', 'unit']].values
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Price statistics
    st.subheader("📊 Price Statistics")
    
    # Calculate statistics for each commodity
    stats_df = commodity_data.groupby('commodity_symbol').agg({
        'price': ['mean', 'min', 'max', 'std']
    }).round(4)
    
    stats_df.columns = ['Average Price', 'Minimum Price', 'Maximum Price', 'Standard Deviation']
    stats_df = stats_df.reset_index()
    
    # Add units to the display
    units_dict = dict(zip(commodity_data['commodity_symbol'], commodity_data['unit']))
    stats_df['Unit'] = stats_df['commodity_symbol'].map(units_dict)
    
    # Calculate percent change from first to last date for each commodity
    price_changes = []
    for symbol in selected_commodities:
        symbol_data = commodity_data[commodity_data['commodity_symbol'] == symbol].sort_values('date')
        if len(symbol_data) >= 2:
            first_price = symbol_data.iloc[0]['price']
            last_price = symbol_data.iloc[-1]['price']
            pct_change = ((last_price - first_price) / first_price) * 100
            price_changes.append({
                'commodity_symbol': symbol,
                'percent_change': pct_change
            })
    
    if price_changes:
        price_changes_df = pd.DataFrame(price_changes)
        stats_df = stats_df.merge(price_changes_df, on='commodity_symbol')
        stats_df = stats_df.rename(columns={'percent_change': 'Price Change %'})
        stats_df['Price Change %'] = stats_df['Price Change %'].round(2)
    
    st.write(stats_df)
    
    # Price correlation analysis
    if len(selected_commodities) > 1:
        st.subheader("🔄 Price Correlation Analysis")
        
        # Create pivot table for correlation
        pivot_df = commodity_data.pivot(index='date', 
                                      columns='commodity_symbol', 
                                      values='price')
        
        # Calculate correlation matrix
        corr_matrix = pivot_df.corr()
        
        # Create heatmap
        fig = px.imshow(corr_matrix,
                       labels=dict(color="Correlation"),
                       title="Price Correlation Matrix")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add correlation interpretation
        st.write("#### Correlation Interpretation")
        st.write("""
        - Values close to 1 indicate strong positive correlation (prices move together)
        - Values close to -1 indicate strong negative correlation (prices move in opposite directions)
        - Values close to 0 indicate little to no correlation
        """)
        
        # Find strongest correlations
        correlations = []
        for i in range(len(selected_commodities)):
            for j in range(i + 1, len(selected_commodities)):
                corr = corr_matrix.iloc[i, j]
                correlations.append({
                    'pair': f"{selected_commodities[i]} - {selected_commodities[j]}",
                    'correlation': corr
                })
        
        if correlations:
            correlations_df = pd.DataFrame(correlations)
            correlations_df = correlations_df.sort_values('correlation', ascending=False)
            
            st.write("#### Strongest Correlations")
            for _, row in correlations_df.iterrows():
                corr = row['correlation']
                if abs(corr) > 0.5:  # Only show meaningful correlations
                    st.write(f"- {row['pair']}: {corr:.2f}")

def main():
    st.title("🌾 USDA Agricultural Data Analysis Dashboard")
    
    # Create tabs for different data types
    tab1, tab2 = st.tabs(["Cattle Slaughter Analysis", "Commodity Analysis"])
    
    # Load data
    cattle_df = load_cattle_data()
    commodity_df = load_commodity_data()
    
    # Date range selector in sidebar
    st.sidebar.header("Filters")
    
    # Get the overall date range from both datasets
    min_date = min(
        cattle_df['slaughter_date'].min().date() if cattle_df is not None else datetime.today().date(),
        commodity_df['date'].min().date() if commodity_df is not None else datetime.today().date()
    )
    max_date = max(
        cattle_df['slaughter_date'].max().date() if cattle_df is not None else datetime.today().date(),
        commodity_df['date'].max().date() if commodity_df is not None else datetime.today().date()
    )
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    with tab1:
        if cattle_df is None or cattle_df.empty:
            st.error("Unable to load cattle data from Supabase. Please check your connection and credentials.")
        else:
            # Display sample of the cattle data
            st.write("Sample of the cattle data:")
            columns_to_show = ['description', 'class', 'slaughter_date', 'volume', 'unit']
            st.write(cattle_df[columns_to_show].head())
            
            # Filter cattle data by date range
            mask = (cattle_df['slaughter_date'].dt.date >= date_range[0]) & (cattle_df['slaughter_date'].dt.date <= date_range[1])
            filtered_cattle_df = cattle_df[mask]
            
            # Display cattle metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_cattle = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Head Slaughtered') & 
                    (filtered_cattle_df['class'] == 'All')
                ]['volume'].sum()
                st.metric("Total Cattle Slaughtered", f"{total_cattle:,.0f}")
            
            with col2:
                avg_live_weight = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Live Weight')
                ]['volume'].mean()
                st.metric("Average Live Weight (lbs)", f"{avg_live_weight:,.0f}" if not pd.isna(avg_live_weight) else "N/A")
            
            with col3:
                avg_dressed_weight = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Dressed Weight')
                ]['volume'].mean()
                st.metric("Average Dressed Weight (lbs)", f"{avg_dressed_weight:,.0f}" if not pd.isna(avg_dressed_weight) else "N/A")
            
            with col4:
                meat_prod = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Meat Production') &
                    (filtered_cattle_df['unit'] == 'Million lbs')
                ]['volume'].sum()
                st.metric("Total Meat Production (M lbs)", f"{meat_prod:,.1f}" if not pd.isna(meat_prod) else "N/A")
            
            # Create subtabs for cattle analysis
            subtab1, subtab2 = st.tabs(["Daily Trends", "Composition Analysis"])
            
            with subtab1:
                # Daily slaughter trends
                daily_data = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Head Slaughtered') & 
                    (filtered_cattle_df['class'].isin(['Steers', 'Heifers', 'Dairy Cows', 'Other Cows']))
                ].copy()
                
                if not daily_data.empty:
                    daily_slaughter = daily_data.pivot_table(
                        index='slaughter_date',
                        columns='class',
                        values='volume',
                        aggfunc='sum'
                    ).fillna(0)
                    
                    fig = px.line(daily_slaughter, 
                                title="Daily Slaughter by Class",
                                labels={"value": "Head Count", "slaughter_date": "Date"},
                                height=500)
                    fig.update_layout(legend_title="Class")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No daily trend data available for the selected date range.")
            
            with subtab2:
                # Composition analysis
                col1, col2 = st.columns(2)
                
                with col1:
                    class_data = filtered_cattle_df[
                        (filtered_cattle_df['description'] == 'Head Slaughtered') & 
                        (filtered_cattle_df['class'].isin(['Steers', 'Heifers', 'Dairy Cows', 'Other Cows']))
                    ]
                    
                    if not class_data.empty:
                        class_total = class_data.groupby('class')['volume'].sum()
                        
                        fig = px.pie(values=class_total.values, 
                                   names=class_total.index,
                                   title="Composition by Class",
                                   hole=0.4)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No composition data available for the selected date range.")
                
                with col2:
                    weight_data = filtered_cattle_df[
                        (filtered_cattle_df['description'] == 'Live Weight') & 
                        (filtered_cattle_df['class'].isin(['Steers', 'Heifers', 'Dairy Cows']))
                    ]
                    
                    if not weight_data.empty:
                        avg_weights = weight_data.groupby('class')['volume'].mean()
                        
                        fig = px.bar(x=avg_weights.index, 
                                   y=avg_weights.values,
                                   title="Average Live Weight by Class",
                                   labels={"x": "Class", "y": "Weight (lbs)"})
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No weight data available for the selected date range.")
    
    with tab2:
        display_commodity_analysis(commodity_df, date_range)

if __name__ == "__main__":
    main()
