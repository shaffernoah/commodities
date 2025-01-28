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
            df['slaughter_date'] = pd.to_datetime(df['slaughter_date'])
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
        
        if not response.data:
            st.write("Debug: No data in Supabase, attempting to load from CSV...")
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
        st.write(f"Debug: Error type: {type(e).__name__}")
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
    st.markdown("---")
    
    # Create tabs for different data types
    tab1, tab2, tab3 = st.tabs(["Cattle Slaughter Analysis", "Regional Analysis", "Commodity Analysis"])
    
    # Load data
    cattle_df = load_cattle_data()
    commodity_df = load_commodity_data()
    region_df = None
    
    # Check if data is loaded
    if cattle_df is None or cattle_df.empty:
        st.error("Unable to load cattle data. Please check your database connection.")
        return
        
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Species/Class filter - with error handling
    try:
        available_classes = ['All'] + sorted([c for c in cattle_df['class'].unique() if c != 'All'])
    except:
        available_classes = ['All']
    selected_class = st.sidebar.selectbox("Select Species/Class", available_classes)
    
    # Date range selector
    st.sidebar.markdown("---")
    st.sidebar.header("📅 Date Range")
    
    # Get the overall date range from both datasets
    min_date = datetime.today().date()
    max_date = datetime.today().date()
    
    if cattle_df is not None and not cattle_df.empty and 'slaughter_date' in cattle_df.columns:
        min_date = min(min_date, cattle_df['slaughter_date'].min().date())
        max_date = max(max_date, cattle_df['slaughter_date'].max().date())
    
    if commodity_df is not None and not commodity_df.empty and 'date' in commodity_df.columns:
        min_date = min(min_date, commodity_df['date'].min().date())
        max_date = max(max_date, commodity_df['date'].max().date())
    
    # Add option to show all data
    use_date_filter = st.sidebar.checkbox("Filter by Date Range", value=False)
    
    if use_date_filter:
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    else:
        date_range = (min_date, max_date)
    
    with tab1:
        if cattle_df is None or cattle_df.empty:
            st.error("Unable to load cattle data from Supabase. Please check your connection and credentials.")
        else:
            st.markdown("### 📊 Cattle Slaughter Metrics")
            
            # Filter cattle data by date range and class
            filtered_cattle_df = cattle_df.copy()
            
            if use_date_filter:
                start_date = pd.Timestamp(date_range[0])
                end_date = pd.Timestamp(date_range[1])
                filtered_cattle_df = filtered_cattle_df[
                    (filtered_cattle_df['slaughter_date'] >= start_date) & 
                    (filtered_cattle_df['slaughter_date'] <= end_date)
                ]
            
            # Apply class filter if not 'All'
            if selected_class != 'All':
                filtered_cattle_df = filtered_cattle_df[filtered_cattle_df['class'] == selected_class]
            
            # Display cattle metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                head_data = filtered_cattle_df[filtered_cattle_df['description'] == 'Head Slaughtered']
                if selected_class == 'All':
                    head_data = head_data[head_data['class'] == 'All']
                total_cattle = head_data['volume'].sum()
                st.metric("Total Cattle Slaughtered", f"{total_cattle:,.0f}")
            
            with col2:
                weight_data = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Live Weight') &
                    (filtered_cattle_df['unit'] == 'lbs')
                ]
                avg_live_weight = weight_data['volume'].mean()
                st.metric("Average Live Weight (lbs)", f"{avg_live_weight:,.0f}" if not pd.isna(avg_live_weight) else "N/A")
            
            with col3:
                dressed_data = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Dressed Weight') &
                    (filtered_cattle_df['unit'] == 'lbs')
                ]
                avg_dressed_weight = dressed_data['volume'].mean()
                st.metric("Average Dressed Weight (lbs)", f"{avg_dressed_weight:,.0f}" if not pd.isna(avg_dressed_weight) else "N/A")
            
            with col4:
                meat_data = filtered_cattle_df[
                    (filtered_cattle_df['description'] == 'Meat Production') &
                    (filtered_cattle_df['unit'] == 'Million lbs')
                ]
                meat_prod = meat_data['volume'].sum()
                st.metric("Total Meat Production (M lbs)", f"{meat_prod:,.1f}" if not pd.isna(meat_prod) else "N/A")
            
            st.markdown("---")
            
            # Create subtabs for cattle analysis
            subtab1, subtab2 = st.tabs(["📈 Daily Trends", "📊 Composition Analysis"])
            
            with subtab1:
                # Daily slaughter trends - keep it simple
                daily_data = filtered_cattle_df.copy()
                
                # Debug info
                st.write("Initial data sample:")
                st.write(daily_data[['slaughter_date', 'class', 'volume']].head())
                
                if selected_class == 'All':
                    # When 'All' is selected, show data for 'All' class
                    daily_data = daily_data[daily_data['class'] == 'All']
                else:
                    # When a specific class is selected, only show that class
                    daily_data = daily_data[daily_data['class'] == selected_class]
                
                # Debug info
                st.write("\nFiltered data sample:")
                st.write(daily_data[['slaughter_date', 'class', 'volume']].head())
                st.write(f"Total rows in filtered data: {len(daily_data)}")
                
                if not daily_data.empty:
                    # Sort by date
                    daily_data = daily_data.sort_values('slaughter_date')
                    
                    # Create the line chart
                    fig = px.line(
                        daily_data,
                        x='slaughter_date',
                        y='volume',
                        title="Daily Slaughter Trends",
                        labels={
                            "slaughter_date": "Date",
                            "volume": "Head Count"
                        },
                        height=500
                    )
                    
                    # Update layout
                    fig.update_layout(
                        plot_bgcolor='white',
                        xaxis=dict(
                            showgrid=True, 
                            gridwidth=1, 
                            gridcolor='LightGray',
                            title="Date",
                            tickformat="%Y-%m-%d"  # Format date ticks
                        ),
                        yaxis=dict(
                            showgrid=True, 
                            gridwidth=1, 
                            gridcolor='LightGray',
                            title="Head Count",
                            rangemode='tozero'  # Start y-axis at 0
                        ),
                        hovermode='x unified'  # Show all points at same x position
                    )
                    
                    # Display the chart
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No daily trend data available for the selected filters.")

            with subtab2:
                if selected_class == 'All':
                    # Composition analysis
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        class_data = filtered_cattle_df[
                            (filtered_cattle_df['description'] == 'Head Slaughtered') & 
                            (filtered_cattle_df['class'] != 'All')
                        ]
                        
                        if not class_data.empty:
                            class_total = class_data.groupby('class')['volume'].sum()
                            
                            fig = px.pie(values=class_total.values, 
                                       names=class_total.index,
                                       title="Composition by Class",
                                       hole=0.4)
                            fig.update_layout(
                                showlegend=True,
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("No composition data available for the selected filters.")
                    
                    with col2:
                        weight_data = filtered_cattle_df[
                            (filtered_cattle_df['description'] == 'Live Weight') & 
                            (filtered_cattle_df['class'] != 'All') &
                            (filtered_cattle_df['unit'] == 'lbs')
                        ]
                        
                        if not weight_data.empty:
                            avg_weights = weight_data.groupby('class')['volume'].mean()
                            
                            fig = px.bar(x=avg_weights.index, 
                                       y=avg_weights.values,
                                       title="Average Live Weight by Class",
                                       labels={"x": "Class", "y": "Weight (lbs)"})
                            fig.update_layout(
                                plot_bgcolor='white',
                                xaxis=dict(showgrid=False),
                                yaxis=dict(showgrid=True, gridwidth=1, gridcolor='LightGray')
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("No weight data available for the selected filters.")
                else:
                    st.info("Composition analysis is only available when viewing all classes. Please select 'All' in the sidebar to view composition charts.")

    with tab2:
        if region_df is None or region_df.empty:
            st.error("No regional data available")
        else:
            st.subheader("Regional Analysis")
            
            # Filter regional data by date range
            if use_date_filter:
                start_date = pd.Timestamp(date_range[0])
                end_date = pd.Timestamp(date_range[1])
                mask = (region_df['slaughter_date'] >= start_date) & (region_df['slaughter_date'] <= end_date)
                filtered_region_df = region_df[mask]
            else:
                filtered_region_df = region_df
            
            if not filtered_region_df.empty:
                # Group by region and calculate total volume
                region_totals = filtered_region_df.groupby('region')['volume'].sum().sort_values(ascending=True)
                
                # Create bar chart
                fig = px.bar(
                    x=region_totals.values,
                    y=region_totals.index,
                    orientation='h',
                    title="Total Slaughter by Region",
                    labels={"x": "Total Volume", "y": "Region"}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Show regional trends over time
                region_trends = filtered_region_df.pivot_table(
                    index='slaughter_date',
                    columns='region',
                    values='volume',
                    aggfunc='sum'
                ).fillna(0)
                
                fig = px.line(
                    region_trends,
                    title="Regional Trends Over Time",
                    labels={"value": "Volume", "slaughter_date": "Date"}
                )
                fig.update_layout(legend_title="Region")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No regional data available for the selected date range.")
    
    with tab3:
        display_commodity_analysis(commodity_df, date_range)

if __name__ == "__main__":
    main()
