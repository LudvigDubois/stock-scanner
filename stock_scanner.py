# stock_scanner.py
# Description: Scans the local stock database based on user-defined criteria,
# allows for excluding specific sectors/industries, and exports the results.
# Author: Gemini
# Version: 4.0

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# --- Scan Parameters (Edit these values) ---
DB_FILE = "stock_market_data.db"
MIN_DOLLAR_VOLUME = 3_000_000
MIN_ADR_PERCENT = 8.0
MIN_CLOSE_PRICE = 3.0

AVG_VOL_DAYS = 20
ADR_DAYS = 20
GAIN_DAYS_1M = 21
GAIN_DAYS_3M = 63

GAIN_PERCENTILE_1M = 0.80
GAIN_PERCENTILE_3M = 0.80

# --- NEW: Exclusion Lists (Add sectors or industries to exclude) ---
# Note: These are case-sensitive. Check the database for exact names.
EXCLUDED_SECTORS = [
]
EXCLUDED_INDUSTRIES = [
    'Biotechnology'
]

def generate_tradingview_watchlist(results_df):
    """
    Generates a comma-separated .txt file for TradingView import,
    grouped by asset class (e.g., Stocks, Funds).
    """
    if results_df.empty:
        print("No tickers to export.")
        return

    print("\nGenerating TradingView watchlist file...")
    try:
        # The results_df already contains the exchange and asset class info
        grouped = results_df.groupby('asset_class')
        
        watchlist_parts = []
        for asset_class, group_df in grouped:
            delimiter = f"###{asset_class}s"
            formatted_tickers = [
                f"{row['exchange']}:{row['ticker']}"
                for index, row in group_df.iterrows()
            ]
            watchlist_parts.append(f"{delimiter},{','.join(formatted_tickers)}")

        watchlist_string = ",".join(watchlist_parts)
        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = f"TradingView_Watchlist_{date_str}.txt"
        
        with open(filename, 'w') as f:
            f.write(watchlist_string)
            
        print(f"Successfully created watchlist: {filename}")

    except Exception as e:
        print(f"An error occurred while generating the watchlist file: {e}")


def run_scan():
    """
    Connects to the database, loads data, and runs the scan based on defined criteria.
    """
    print("Starting stock scan...")
    
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query("SELECT * FROM daily_data", conn)
        # Also fetch the ticker info for filtering
        ticker_info_df = pd.read_sql_query("SELECT ticker, exchange, asset_class, sector, industry FROM tickers_exchange", conn)
        conn.close()
    except Exception as e:
        print(f"Error loading data from database: {e}")
        return

    if df.empty:
        print("Database is empty. Please run data_fetcher.py first.")
        return

    print("Calculating metrics for all stocks...")

    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by=['ticker', 'date'], inplace=True)

    df['dollar_volume'] = df['close'] * df['volume']
    df['avg_dollar_volume'] = df.groupby('ticker')['dollar_volume'].transform(
        lambda x: x.rolling(window=AVG_VOL_DAYS).mean()
    )
    df['daily_range_factor'] = df['high'] / (df['low'] + 1e-9)
    df['adr_percent'] = df.groupby('ticker')['daily_range_factor'].transform(
        lambda x: (x.rolling(window=ADR_DAYS).sum() / ADR_DAYS - 1) * 100
    )
    min_low_1m = df.groupby('ticker')['low'].transform(
        lambda x: x.rolling(window=GAIN_DAYS_1M).min()
    )
    df['gain_1m'] = (df['close'] / min_low_1m - 1) * 100
    min_low_3m = df.groupby('ticker')['low'].transform(
        lambda x: x.rolling(window=GAIN_DAYS_3M).min()
    )
    df['gain_3m'] = (df['close'] / min_low_3m - 1) * 100

    latest_data = df.groupby('ticker').last().reset_index()
    
    # Merge financial data with ticker info
    merged_data = pd.merge(latest_data, ticker_info_df, on='ticker')

    # Apply initial screening filters
    filtered_stocks = merged_data[
        (merged_data['avg_dollar_volume'] >= MIN_DOLLAR_VOLUME) &
        (merged_data['adr_percent'] >= MIN_ADR_PERCENT) &
        (merged_data['close'] >= MIN_CLOSE_PRICE)
    ]

    if filtered_stocks.empty:
        print("No stocks passed the initial filters (Volume, ADR, Price).")
        return
        
    # --- NEW: Apply Sector and Industry Exclusions ---
    initial_count = len(filtered_stocks)
    
    if EXCLUDED_SECTORS:
        filtered_stocks = filtered_stocks[~filtered_stocks['sector'].isin(EXCLUDED_SECTORS)]
    
    if EXCLUDED_INDUSTRIES:
        filtered_stocks = filtered_stocks[~filtered_stocks['industry'].isin(EXCLUDED_INDUSTRIES)]
        
    excluded_count = initial_count - len(filtered_stocks)
    if excluded_count > 0:
        print(f"Excluded {excluded_count} stocks based on sector/industry criteria.")
    # --- End of New Section ---

    gain_1m_threshold = filtered_stocks['gain_1m'].quantile(GAIN_PERCENTILE_1M)
    gain_3m_threshold = filtered_stocks['gain_3m'].quantile(GAIN_PERCENTILE_3M)

    print(f"\n1-Month Gainer Threshold: {gain_1m_threshold:.2f}%")
    print(f"3-Month Gainer Threshold: {gain_3m_threshold:.2f}%")

    top_gainers_1m = filtered_stocks[filtered_stocks['gain_1m'] >= gain_1m_threshold]
    top_gainers_3m = filtered_stocks[filtered_stocks['gain_3m'] >= gain_3m_threshold]

    final_results = pd.concat([top_gainers_1m, top_gainers_3m]).drop_duplicates(subset=['ticker'])
    final_results = final_results.sort_values(by='gain_1m', ascending=False)

    print("\n--- Scan Results ---")
    if final_results.empty:
        print("No stocks met all the specified criteria.")
    else:
        output_df = final_results[['ticker', 'close', 'adr_percent', 'avg_dollar_volume', 'gain_1m', 'gain_3m', 'sector', 'industry']].copy()
        output_df['close'] = output_df['close'].map('${:,.2f}'.format)
        output_df['adr_percent'] = output_df['adr_percent'].map('{:.2f}%'.format)
        output_df['avg_dollar_volume'] = output_df['avg_dollar_volume'].map('${:,.0f}'.format)
        output_df['gain_1m'] = output_df['gain_1m'].map('{:.2f}%'.format)
        output_df['gain_3m'] = output_df['gain_3m'].map('{:.2f}%'.format)
        print(output_df.to_string(index=False))

    print(f"\nScan complete. Found {len(final_results)} stocks.")

    generate_tradingview_watchlist(final_results)


if __name__ == "__main__":
    run_scan()
