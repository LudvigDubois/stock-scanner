# ticker_inspector.py
# Description: Calculates and displays scan metrics for a single, specified ticker
# to help debug why it may not be appearing in the main scan results.
# Author: Gemini
# Version: 1.0

import sqlite3
import pandas as pd
import numpy as np

from stock_scanner import MIN_DOLLAR_VOLUME, MIN_ADR_PERCENT, MIN_CLOSE_PRICE, AVG_VOL_DAYS, ADR_DAYS, GAIN_DAYS_1M, GAIN_DAYS_3M, GAIN_DAYS_6M, EXCLUDED_INDUSTRIES, EXCLUDED_SECTORS, USE_MA200_FILTER, MA_DAYS_LONG

# --- Ticker to Inspect (Edit this value) ---
TICKER_TO_INSPECT = "CRWV"  # <--- CHANGE THIS TO THE TICKER YOU WANT TO CHECK

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
        # Fetch price data
        price_query = "SELECT * FROM daily_data WHERE ticker = ?"
        df = pd.read_sql_query(price_query, conn, params=(ticker,))
        
        # Fetch ticker info (sector, industry, etc.)
        info_query = "SELECT sector, industry FROM tickers_exchange WHERE ticker = ?"
        ticker_info = pd.read_sql_query(info_query, conn, params=(ticker,)).iloc[0]
        
        conn.close()
    except Exception as e:
        print(f"Error loading data from database for {ticker}: {e}")
        return

    if df.empty:
        print(f"No price data found for ticker '{ticker}' in the database.")
        return

    # --- NEW: Print the total number of days found ---
    print(f"\nFound {len(df)} days of historical data in the database.")

    # --- Data Preparation and Calculations ---
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by='date', inplace=True)

    # Check if there's enough data for the basic calculations
    if len(df) < AVG_VOL_DAYS:
        print(f"Warning: Not enough historical data for {ticker} to perform basic calculations.")
        print(f"Required: {AVG_VOL_DAYS} days, Found: {len(df)} days.")
        return

    df['ma200'] = df['close'].rolling(window=MA_DAYS_LONG, min_periods=MA_DAYS_LONG).mean()
    df['dollar_volume'] = df['close'] * df['volume']
    df['avg_dollar_volume'] = df['dollar_volume'].rolling(window=AVG_VOL_DAYS).mean()
    df['daily_range_factor'] = df['high'] / (df['low'] + 1e-9)
    df['adr_percent'] = (df['daily_range_factor'].rolling(window=ADR_DAYS).sum() / ADR_DAYS - 1) * 100
    
    # --- FIX: Added min_periods to all gain calculations for consistency ---
    min_low_1m = df['low'].rolling(window=GAIN_DAYS_1M, min_periods=GAIN_DAYS_1M).min()
    df['gain_1m'] = (df['close'] / min_low_1m - 1) * 100

    min_low_3m = df['low'].rolling(window=GAIN_DAYS_3M, min_periods=GAIN_DAYS_3M).min()
    df['gain_3m'] = (df['close'] / min_low_3m - 1) * 100

    min_low_6m = df['low'].rolling(window=GAIN_DAYS_6M, min_periods=GAIN_DAYS_6M).min()
    df['gain_6m'] = (df['close'] / min_low_6m - 1) * 100

    latest_metrics = df.iloc[-1]

    # --- Display Results and Criteria Check ---
    print("\n--- Ticker Information ---")
    sector = ticker_info['sector']
    industry = ticker_info['industry']
    print(f"Sector: {sector}")
    print(f"Industry: {industry}")
    
    print("\n--- Calculated Metrics (as of latest data) ---")
    
    price = latest_metrics['close']
    avg_vol = latest_metrics['avg_dollar_volume']
    adr = latest_metrics['adr_percent']
    ma200 = latest_metrics['ma200']
    gain1m = latest_metrics['gain_1m']
    gain3m = latest_metrics['gain_3m']
    gain6m = latest_metrics['gain_6m']

    print(f"Latest Close Price: ${price:,.2f}")
    print(f"200-Day MA: {'${:,.2f}'.format(ma200) if pd.notna(ma200) else 'N/A (Not enough data)'}")
    print(f"20-Day Avg Dollar Volume: ${avg_vol:,.0f}")
    print(f"20-Day ADR: {adr:.2f}%")
    # --- FIX: Updated print statements to handle potential N/A values ---
    print(f"1-Month Gain (21 days): {'{:.2f}%'.format(gain1m) if pd.notna(gain1m) else 'N/A (Not enough data)'}")
    print(f"3-Month Gain (63 days): {'{:.2f}%'.format(gain3m) if pd.notna(gain3m) else 'N/A (Not enough data)'}")
    print(f"6-Month Gain (126 days): {'{:.2f}%'.format(gain6m) if pd.notna(gain6m) else 'N/A (Not enough data)'}")

    print("\n--- Criteria Check ---")
    
    pass_price = price >= MIN_CLOSE_PRICE
    print(f"1. Min Price >= ${MIN_CLOSE_PRICE:,.2f}: {'PASS' if pass_price else 'FAIL'}")

    pass_volume = avg_vol >= MIN_DOLLAR_VOLUME
    print(f"2. Min Avg Volume >= ${MIN_DOLLAR_VOLUME:,.0f}: {'PASS' if pass_volume else 'FAIL'}")

    pass_adr = adr >= MIN_ADR_PERCENT
    print(f"3. Min ADR >= {MIN_ADR_PERCENT:.2f}%: {'PASS' if pass_adr else 'FAIL'}")
    
    pass_sector = sector not in EXCLUDED_SECTORS
    print(f"4. Sector Not Excluded: {'PASS' if pass_sector else 'FAIL'}")
    
    pass_industry = industry not in EXCLUDED_INDUSTRIES
    print(f"5. Industry Not Excluded: {'PASS' if pass_industry else 'FAIL'}")

    if USE_MA200_FILTER:
        pass_ma = (price >= ma200) or pd.isna(ma200)
        status = 'PASS' if pass_ma else 'FAIL'
        reason = "(Above MA or MA is N/A)" if pass_ma else "(Below MA)"
        print(f"6. MA200 Filter Active: {status} {reason}")
    else:
        print("6. MA200 Filter: INACTIVE")

if __name__ == "__main__":
    if TICKER_TO_INSPECT == "AAPL": # A default example
        print("Inspecting the default ticker 'AAPL'. Please edit the script to change the ticker.")
    inspect_ticker(TICKER_TO_INSPECT)