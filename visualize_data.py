import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set style
sns.set_theme(style="whitegrid")
sns.set_palette("husl")

def clean_dataframe(df):
    df.columns = df.columns.str.strip().str.lower()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    return df

def load_and_prepare_data():
    # Load the most recent data files
    daily_slaughter = pd.read_csv('cattle_daily_slaughter_20250123_221454.csv')
    full_data = pd.read_csv('federal_inspection_slaughter_20250123_221454.csv')

    # Clean dataframes
    daily_slaughter = clean_dataframe(daily_slaughter)
    full_data = clean_dataframe(full_data)

    # Convert slaughter_date to datetime with error coercion
    if 'slaughter_date' in daily_slaughter.columns:
        daily_slaughter['slaughter_date'] = pd.to_datetime(daily_slaughter['slaughter_date'], format='%m/%d/%y', errors='coerce')

    return daily_slaughter, full_data

def plot_daily_slaughter(daily_slaughter):
    plt.figure(figsize=(12, 6))
    plt.plot(daily_slaughter['slaughter_date'], daily_slaughter['volume'], 
             marker='o', linewidth=2, markersize=8)
    
    plt.title('Daily Cattle Slaughter Volume', fontsize=14, pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Number of Head', fontsize=12)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    # Add value labels
    for x, y in zip(daily_slaughter['slaughter_date'], daily_slaughter['volume']):
        plt.annotate(f'{y:,.0f}', 
                    (x, y), 
                    textcoords="offset points", 
                    xytext=(0,10), 
                    ha='center')
    
    plt.tight_layout()
    plt.savefig('daily_slaughter_trend.png')
    plt.close()

def plot_commodity_distribution(full_data):
    # Filter for total values by commodity
    commodity_data = full_data[
        (full_data['commodity'].str.contains('Total', na=False)) & 
        (full_data['description'] == 'Head Slaughtered')
    ].copy()
    
    # Convert volume to numeric, removing commas
    commodity_data['volume'] = pd.to_numeric(commodity_data['volume'].replace('-', '0'))
    
    # Clean up commodity names
    commodity_data['commodity'] = commodity_data['commodity'].str.replace(' Total', '')
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='commodity', y='volume', data=commodity_data)
    
    plt.title('Distribution of Slaughter by Animal Type', fontsize=14, pad=20)
    plt.xlabel('Animal Type', fontsize=12)
    plt.ylabel('Number of Head', fontsize=12)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    # Add value labels
    for i, v in enumerate(commodity_data['volume']):
        plt.text(i, v, f'{v:,.0f}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('commodity_distribution.png')
    plt.close()

def plot_weight_metrics(full_data):
    # Filter for beef weight and production metrics
    metrics_data = full_data[
        (full_data['section'] == 'Report FIS Meat Production') &
        (full_data['type'] == 'Beef')
    ].copy()
    
    # Convert volume to numeric, replacing '-' with 0
    metrics_data['volume'] = pd.to_numeric(metrics_data['volume'].replace('-', '0'))
    
    # Create figure
    plt.figure(figsize=(10, 6))
    
    # Plot all metrics
    sns.barplot(data=metrics_data, x='description', y='volume')
    
    plt.title('Beef Processing Metrics', fontsize=14, pad=20)
    plt.xlabel('Metric', fontsize=12)
    plt.ylabel('Weight/Production (lbs)', fontsize=12)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    # Add value labels
    for i, v in enumerate(metrics_data['volume']):
        plt.text(i, v, f'{v:,.0f}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('beef_metrics.png', dpi=300)
    plt.close()

def main():
    # Load data
    daily_slaughter, full_data = load_and_prepare_data()
    
    # Create visualizations
    plot_daily_slaughter(daily_slaughter)
    plot_commodity_distribution(full_data)
    plot_weight_metrics(full_data)
    
    print("Visualizations have been created and saved as:")
    print("1. daily_slaughter_trend.png")
    print("2. commodity_distribution.png")
    print("3. beef_metrics.png")

if __name__ == "__main__":
    main()
