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

def display_cattle_metrics(filtered_df, selected_class):
    """Display cattle metrics and charts"""
    st.markdown("### 📊 Cattle Slaughter Metrics")
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Filter for selected class
    class_data = filtered_df[filtered_df['class'] == selected_class].copy()
    
    with col1:
        head_data = class_data[class_data['description'] == 'Head Slaughtered']
        total_cattle = head_data['volume'].sum() if not head_data.empty else 0
        st.metric("Total Cattle Slaughtered", f"{total_cattle:,.0f}")
    
    with col2:
        live_data = class_data[
            (class_data['description'] == 'Live Weight') & 
            (class_data['unit'] == 'lbs')
        ]
        avg_live = live_data['volume'].mean() if not live_data.empty else None
        st.metric("Average Live Weight (lbs)", f"{avg_live:,.0f}" if pd.notna(avg_live) else "N/A")
    
    with col3:
        dressed_data = class_data[
            (class_data['description'] == 'Dressed Weight') & 
            (class_data['unit'] == 'lbs')
        ]
        avg_dressed = dressed_data['volume'].mean() if not dressed_data.empty else None
        st.metric("Average Dressed Weight (lbs)", f"{avg_dressed:,.0f}" if pd.notna(avg_dressed) else "N/A")
    
    with col4:
        meat_data = class_data[
            (class_data['description'] == 'Total Red Meat') & 
            (class_data['unit'] == 'lbs')
        ]
        total_meat = meat_data['volume'].sum() if not meat_data.empty else None
        st.metric(
            "Total Meat Production (M lbs)", 
            f"{(total_meat/1_000_000):,.1f}" if pd.notna(total_meat) else "N/A"
        )
    
    # Create tabs for detailed analysis
    tab1, tab2, tab3 = st.tabs(["📈 Slaughter Trends", "⚖️ Weight Analysis", "🥩 Meat Production"])
    
    with tab1:
        # Daily slaughter trends
        slaughter_data = filtered_df[filtered_df['description'] == 'Head Slaughtered'].copy()
        
        if not slaughter_data.empty:
            # Create a pivot table for easier rolling average calculation
            pivot_data = slaughter_data.pivot(
                index='slaughter_date',
                columns='class',
                values='volume'
            ).reset_index()
            
            # Calculate 7-day rolling average for each class
            for col in pivot_data.columns:
                if col != 'slaughter_date':
                    pivot_data[f'{col}_7day_avg'] = pivot_data[col].rolling(window=7, min_periods=1).mean()
            
            # Melt the data back for plotting
            plot_data = pd.melt(
                pivot_data,
                id_vars=['slaughter_date'],
                value_vars=[col for col in pivot_data.columns if col not in ['slaughter_date']],
                var_name='series',
                value_name='volume'
            )
            
            # Separate daily and rolling average data
            daily_data = plot_data[~plot_data['series'].str.contains('_7day_avg')]
            rolling_data = plot_data[plot_data['series'].str.contains('_7day_avg')]
            rolling_data['series'] = rolling_data['series'].str.replace('_7day_avg', ' (7-day avg)')
            
            # Create figure with both daily values and rolling averages
            fig = go.Figure()
            
            # Add daily values as scatter plots with low opacity
            for class_name in daily_data['series'].unique():
                mask = daily_data['series'] == class_name
                fig.add_trace(go.Scatter(
                    x=daily_data[mask]['slaughter_date'],
                    y=daily_data[mask]['volume'],
                    name=class_name,
                    mode='markers',
                    opacity=0.3,
                    showlegend=False
                ))
            
            # Add rolling averages as solid lines
            for class_name in rolling_data['series'].unique():
                mask = rolling_data['series'] == class_name
                fig.add_trace(go.Scatter(
                    x=rolling_data[mask]['slaughter_date'],
                    y=rolling_data[mask]['volume'],
                    name=class_name,
                    mode='lines',
                    line=dict(width=3)
                ))
            
            fig.update_layout(
                title="Daily Slaughter Trends (with 7-day moving average)",
                plot_bgcolor='white',
                xaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    title="Date"
                ),
                yaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    title="Head Count",
                    rangemode='tozero'
                ),
                hovermode='x unified',
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                )
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        # Weight comparison
        weight_data = filtered_df[
            (filtered_df['description'].isin(['Live Weight', 'Dressed Weight'])) &
            (filtered_df['unit'] == 'lbs')
        ].copy()
        
        if not weight_data.empty:
            st.markdown("### 📊 Weight Analysis")
            
            # Create pivot table for rolling averages
            pivot_data = weight_data.pivot(
                index='slaughter_date',
                columns=['description', 'class'],
                values='volume'
            ).reset_index()
            
            # Calculate 7-day rolling averages
            for col in pivot_data.columns:
                if col != 'slaughter_date':
                    pivot_data[f'{col}_7day_avg'] = pivot_data[col].rolling(window=7, min_periods=1).mean()
            
            # Melt the data back for plotting
            plot_data = pd.melt(
                pivot_data,
                id_vars=['slaughter_date'],
                value_vars=[col for col in pivot_data.columns if col not in ['slaughter_date']],
                var_name='series',
                value_name='volume'
            )
            
            # Split the series column into description and class
            plot_data[['description', 'class', 'rolling']] = plot_data['series'].apply(
                lambda x: pd.Series([
                    x[0] if isinstance(x, tuple) else None,  # Description
                    x[1] if isinstance(x, tuple) else None,  # Class
                    '_7day_avg' in str(x)  # Rolling average flag
                ])
            )
            
            # Create figure
            fig = go.Figure()
            
            # Plot daily values and rolling averages for each weight type and class
            for desc in ['Live Weight', 'Dressed Weight']:
                for class_name in weight_data['class'].unique():
                    # Daily values as scatter
                    daily_mask = (
                        (plot_data['description'] == desc) &
                        (plot_data['class'] == class_name) &
                        (~plot_data['rolling'])
                    )
                    if daily_mask.any():
                        fig.add_trace(go.Scatter(
                            x=plot_data[daily_mask]['slaughter_date'],
                            y=plot_data[daily_mask]['volume'],
                            name=f"{desc} - {class_name}",
                            mode='markers',
                            opacity=0.3,
                            showlegend=True,
                            marker=dict(
                                symbol='circle' if desc == 'Live Weight' else 'square',
                                size=8
                            )
                        ))
                    
                    # Rolling average as line
                    rolling_mask = (
                        (plot_data['description'] == desc) &
                        (plot_data['class'] == class_name) &
                        (plot_data['rolling'])
                    )
                    if rolling_mask.any():
                        fig.add_trace(go.Scatter(
                            x=plot_data[rolling_mask]['slaughter_date'],
                            y=plot_data[rolling_mask]['volume'],
                            name=f"{desc} - {class_name} (7-day avg)",
                            mode='lines',
                            line=dict(
                                width=3,
                                dash='solid' if desc == 'Live Weight' else 'dash'
                            )
                        ))
            
            # Calculate and display dressing percentage
            st.markdown("### 🔄 Dressing Percentage")
            
            # Get the latest weight data
            latest_date = weight_data['slaughter_date'].max()
            latest_weights = weight_data[weight_data['slaughter_date'] == latest_date]
            
            # Calculate dressing percentages for each class
            col1, col2, col3 = st.columns(3)
            
            for i, class_name in enumerate(['Cattle', 'Steers', 'Heifers']):
                live = latest_weights[
                    (latest_weights['description'] == 'Live Weight') &
                    (latest_weights['class'] == class_name)
                ]['volume'].iloc[0] if len(latest_weights) > 0 else None
                
                dressed = latest_weights[
                    (latest_weights['description'] == 'Dressed Weight') &
                    (latest_weights['class'] == class_name)
                ]['volume'].iloc[0] if len(latest_weights) > 0 else None
                
                if live and dressed:
                    dressing_pct = (dressed / live) * 100
                    col = [col1, col2, col3][i]
                    with col:
                        st.metric(
                            f"Dressing % ({class_name})",
                            f"{dressing_pct:.1f}%",
                            help=f"Live: {live:,.0f} lbs\nDressed: {dressed:,.0f} lbs"
                        )
            
            # Update layout
            fig.update_layout(
                title="Live vs Dressed Weight by Class (with 7-day moving average)",
                plot_bgcolor='white',
                xaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    title="Date"
                ),
                yaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    title="Weight (lbs)",
                    rangemode='tozero'
                ),
                hovermode='x unified',
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01,
                    bgcolor='rgba(255,255,255,0.8)'
                )
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Meat production trends
        meat_data = filtered_df[
            (filtered_df['description'] == 'Total Red Meat') &
            (filtered_df['unit'] == 'lbs')
        ].copy()
        
        if not meat_data.empty:
            # Convert to millions of pounds
            meat_data['volume'] = meat_data['volume'] / 1_000_000
            
            # Create line chart
            fig = px.line(
                meat_data,
                x='slaughter_date',
                y='volume',
                color='class',
                title="Daily Meat Production by Class",
                labels={
                    "slaughter_date": "Date",
                    "volume": "Production (M lbs)",
                    "class": "Class"
                }
            )
            fig.update_layout(
                plot_bgcolor='white',
                xaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    title="Date"
                ),
                yaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray',
                    title="Production (M lbs)",
                    rangemode='tozero'
                ),
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)

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
        if 'slaughter_date' in cattle_df.columns:
            cattle_min = cattle_df['slaughter_date'].min().date()
            cattle_max = cattle_df['slaughter_date'].max().date()
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
