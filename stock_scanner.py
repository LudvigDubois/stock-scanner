# stock_scanner.py
# Description: Scans the local stock database based on user-defined criteria.
# This version is "IPO-friendly", meaning it will not exclude new stocks
# that lack sufficient history for long-term metrics like the 200-day MA.
# Author: Gemini
# Version: 7.0

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# --- Scan Parameters (Edit these values) ---
DB_FILE = "stock_market_data.db"
MIN_DOLLAR_VOLUME = 5_000_000
MIN_ADR_PERCENT = 7.0
MIN_CLOSE_PRICE = 2.0

AVG_VOL_DAYS = 1
ADR_DAYS = 20
GAIN_DAYS_1M = 21
GAIN_DAYS_3M = 63
GAIN_DAYS_6M = 126
MA_DAYS_LONG = 200

GAIN_PERCENTILE_1M = 0.50
GAIN_PERCENTILE_3M = 0.60
GAIN_PERCENTILE_6M = 0.60

# --- Moving Average Filter ---
# Set to True to only keep stocks trading above their 200-day moving average.
# New stocks without a 200-day MA will be included if this is True.
USE_MA200_FILTER = True

# --- Exclusion Lists (Add sectors or industries to exclude) ---
EXCLUDED_SECTORS = [
    #'Healthcare'
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
        filename = f"scan_results/scan_results_{date_str}.txt"
        
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

    df['ma200'] = df.groupby('ticker')['close'].transform(
        lambda x: x.rolling(window=MA_DAYS_LONG, min_periods=MA_DAYS_LONG).mean()
    )
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
    
    min_low_6m = df.groupby('ticker')['low'].transform(
        lambda x: x.rolling(window=GAIN_DAYS_6M, min_periods=GAIN_DAYS_6M).min()
    )
    df['gain_6m'] = (df['close'] / min_low_6m - 1) * 100

    latest_data = df.groupby('ticker').last().reset_index()
    
    merged_data = pd.merge(latest_data, ticker_info_df, on='ticker')

    filtered_stocks = merged_data[
        (merged_data['avg_dollar_volume'] >= MIN_DOLLAR_VOLUME) &
        (merged_data['adr_percent'] >= MIN_ADR_PERCENT) &
        (merged_data['close'] >= MIN_CLOSE_PRICE)
    ]

    if filtered_stocks.empty:
        print("No stocks passed the initial filters (Volume, ADR, Price).")
        return
        
    initial_count = len(filtered_stocks)
    
    if EXCLUDED_SECTORS:
        filtered_stocks = filtered_stocks[~filtered_stocks['sector'].isin(EXCLUDED_SECTORS)]
    if EXCLUDED_INDUSTRIES:
        filtered_stocks = filtered_stocks[~filtered_stocks['industry'].isin(EXCLUDED_INDUSTRIES)]
        
    # --- UPDATED: MA200 Filter now includes new stocks ---
    if USE_MA200_FILTER:
        # Keep stocks that are above their MA200 OR stocks that don't have an MA200 yet (new issues)
        filtered_stocks = filtered_stocks[
            (filtered_stocks['close'] >= filtered_stocks['ma200']) | (filtered_stocks['ma200'].isna())
        ]
    
    excluded_count = initial_count - len(filtered_stocks)
    if excluded_count > 0:
        print(f"Excluded {excluded_count} stocks based on sector and/or MA200 criteria.")

    # Quantile calculations will automatically ignore NaNs for new stocks
    gain_1m_threshold = filtered_stocks['gain_1m'].quantile(GAIN_PERCENTILE_1M)
    gain_3m_threshold = filtered_stocks['gain_3m'].quantile(GAIN_PERCENTILE_3M)
    gain_6m_threshold = filtered_stocks['gain_6m'].quantile(GAIN_PERCENTILE_6M)

    print(f"\n1-Month Gainer Threshold: {gain_1m_threshold:.2f}%")
    print(f"3-Month Gainer Threshold: {gain_3m_threshold:.2f}%")
    print(f"6-Month Gainer Threshold: {gain_6m_threshold:.2f}%")

    top_gainers_1m = filtered_stocks[filtered_stocks['gain_1m'] >= gain_1m_threshold]
    top_gainers_3m = filtered_stocks[filtered_stocks['gain_3m'] >= gain_3m_threshold]
    # This filter correctly ignores new stocks with NaN for gain_6m
    top_gainers_6m = filtered_stocks[filtered_stocks['gain_6m'] >= gain_6m_threshold]

    final_results = pd.concat([top_gainers_1m, top_gainers_3m, top_gainers_6m]).drop_duplicates(subset=['ticker'])
    final_results = final_results.sort_values(by='gain_1m', ascending=False)

    print("\n--- Scan Results ---")
    if final_results.empty:
        print("No stocks met all the specified criteria.")
    else:
        output_cols = ['ticker', 'date', 'close', 'adr_percent', 'avg_dollar_volume', 'gain_1m', 'gain_3m', 'gain_6m', 'sector', 'industry']
        output_df = final_results[output_cols].copy()
        output_df['close'] = output_df['close'].map('${:,.2f}'.format)
        output_df['adr_percent'] = output_df['adr_percent'].map('{:.2f}%'.format)
        output_df['avg_dollar_volume'] = output_df['avg_dollar_volume'].map('${:,.0f}'.format)
        output_df['gain_1m'] = output_df['gain_1m'].map('{:.2f}%'.format)
        output_df['gain_3m'] = output_df['gain_3m'].map('{:.2f}%'.format)
        # --- UPDATED: Handle potential missing gain_6m values for new stocks ---
        output_df['gain_6m'] = output_df['gain_6m'].apply(lambda x: '{:.2f}%'.format(x) if pd.notna(x) else 'N/A')
        
        print(output_df.to_string(index=False))

    print(f"\nScan complete. Found {len(final_results)} stocks.")

    generate_tradingview_watchlist(final_results)


if __name__ == "__main__":
    run_scan()
