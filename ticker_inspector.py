# ticker_inspector.py
# Description: Calculates and displays scan metrics for a single, specified ticker
# to help debug why it may not be appearing in the main scan results.
# Author: Gemini
# Version: 1.0

import sqlite3
import pandas as pd
import numpy as np

from stock_scanner import MIN_DOLLAR_VOLUME, MIN_ADR_PERCENT, MIN_CLOSE_PRICE, AVG_VOL_DAYS, ADR_DAYS, GAIN_DAYS_1M, GAIN_DAYS_3M, GAIN_DAYS_6M

# --- Ticker to Inspect (Edit this value) ---
TICKER_TO_INSPECT = "UUUU"  # <--- CHANGE THIS TO THE TICKER YOU WANT TO CHECK

# --- Scan Parameters (Copied from stock_scanner.py for consistency) ---
DB_FILE = "stock_market_data.db"

def inspect_ticker(ticker):
    """
    Loads data for a single ticker, calculates all scan metrics,
    and prints a detailed report comparing them to the scan criteria.
    """
    print(f"--- Inspecting Ticker: {ticker} ---")
    
    try:
        conn = sqlite3.connect(DB_FILE)
        # Use a parameterized query to fetch data for only the specified ticker
        query = "SELECT * FROM daily_data WHERE ticker = ?"
        df = pd.read_sql_query(query, conn, params=(ticker,))
        conn.close()
    except Exception as e:
        print(f"Error loading data from database for {ticker}: {e}")
        return

    if df.empty:
        print(f"No data found for ticker '{ticker}' in the database.")
        return

    # --- Data Preparation and Calculations ---
    # The logic here is identical to stock_scanner.py

    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by='date', inplace=True)

    # Check if there's enough data for the longest calculation period
    if len(df) < GAIN_DAYS_6M:
        print(f"Warning: Not enough historical data for {ticker} to perform all calculations.")
        print(f"Required: {GAIN_DAYS_6M} days, Found: {len(df)} days.")
        return

    df['dollar_volume'] = df['close'] * df['volume']
    df['avg_dollar_volume'] = df['dollar_volume'].rolling(window=AVG_VOL_DAYS).mean()
    df['daily_range_factor'] = df['high'] / (df['low'] + 1e-9)
    df['adr_percent'] = (df['daily_range_factor'].rolling(window=ADR_DAYS).sum() / ADR_DAYS - 1) * 100
    
    min_low_1m = df['low'].rolling(window=GAIN_DAYS_1M).min()
    df['gain_1m'] = (df['close'] / min_low_1m - 1) * 100

    min_low_3m = df['low'].rolling(window=GAIN_DAYS_3M).min()
    df['gain_3m'] = (df['close'] / min_low_3m - 1) * 100

    min_low_6m = df['low'].rolling(window=GAIN_DAYS_6M).min()
    df['gain_6m'] = (df['close'] / min_low_3m - 1) * 100

    # Get the most recent row of data
    latest_metrics = df.iloc[-1]

    # --- Display Results and Criteria Check ---
    print("\n--- Calculated Metrics (as of latest data) ---")
    
    price = latest_metrics['close']
    avg_vol = latest_metrics['avg_dollar_volume']
    adr = latest_metrics['adr_percent']
    gain1m = latest_metrics['gain_1m']
    gain3m = latest_metrics['gain_3m']
    gain6m = latest_metrics['gain_6m']

    print(f"Latest Close Price: ${price:,.2f}")
    print(f"20-Day Avg Dollar Volume: ${avg_vol:,.0f}")
    print(f"20-Day ADR: {adr:.2f}%")
    print(f"1-Month Gain (21 days): {gain1m:.2f}%")
    print(f"3-Month Gain (63 days): {gain3m:.2f}%")
    print(f"6-Month Gain (126 days): {gain6m:.2f}%")

    print("\n--- Criteria Check ---")
    
    # Check each criterion and print a pass/fail message
    pass_price = price >= MIN_CLOSE_PRICE
    print(f"1. Min Price >= ${MIN_CLOSE_PRICE:,.2f}: {'PASS' if pass_price else 'FAIL'} (Value: ${price:,.2f})")

    pass_volume = avg_vol >= MIN_DOLLAR_VOLUME
    print(f"2. Min Avg Volume >= ${MIN_DOLLAR_VOLUME:,.0f}: {'PASS' if pass_volume else 'FAIL'} (Value: ${avg_vol:,.0f})")

    pass_adr = adr >= MIN_ADR_PERCENT
    print(f"3. Min ADR >= {MIN_ADR_PERCENT:.2f}%: {'PASS' if pass_adr else 'FAIL'} (Value: {adr:.2f}%)")
    
    if not all([pass_price, pass_volume, pass_adr]):
        print("\nThis ticker fails the initial screening criteria and would not be considered for the gainer rankings.")
    else:
        print("\nThis ticker passes all initial screening criteria and would be included in the gainer rankings.")


if __name__ == "__main__":
    if TICKER_TO_INSPECT == "AAPL": # A default example
        print("Inspecting the default ticker 'AAPL'. Please edit the script to change the ticker.")
    inspect_ticker(TICKER_TO_INSPECT)
