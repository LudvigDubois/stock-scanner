# data_audit.py
# Description: A utility to scan the database for tickers with incomplete historical data.
# It identifies stocks that have been trading for a while but have too few records,
# suggesting a corrupted download.
# Author: Gemini
# Version: 1.0

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
DB_FILE = "stock_market_data.db"
# The minimum number of days of data a stock should have if it's not a new issue.
# This should be higher than your longest calculation period (e.g., 200 for the MA).
MIN_DAYS_THRESHOLD = 220 
# How old a stock's first data point must be to be considered for the audit.
# This prevents flagging recent IPOs. (e.g., 1 year)
MIN_AGE_DAYS = 60
OUTPUT_FILE = "tickers_to_fix.txt"

def run_data_audit():
    """
    Scans the daily_data table to find tickers with suspiciously incomplete history.
    """
    print("Starting data integrity audit...")
    
    try:
        conn = sqlite3.connect(DB_FILE)
        # Query to get the first date and total count of records for each ticker
        query = "SELECT ticker, MIN(date) as first_date, COUNT(*) as record_count FROM daily_data GROUP BY ticker"
        df = pd.read_sql_query(query, conn)
        conn.close()
    except Exception as e:
        print(f"Error loading data from database: {e}")
        return

    if df.empty:
        print("No data found in the database to audit.")
        return

    print(f"Auditing {len(df)} tickers...")

    df['first_date'] = pd.to_datetime(df['first_date'])
    today = datetime.now()
    
    suspect_tickers = []
    for index, row in df.iterrows():
        ticker = row['ticker']
        first_date = row['first_date']
        record_count = row['record_count']
        
        # Calculate how long ago the stock's first data point was
        days_since_first_data = (today - first_date).days
        
        # Check for two conditions:
        # 1. Is the stock "old" enough to be audited? (i.e., not a recent IPO)
        # 2. Does this "old" stock have fewer records than our threshold?
        if days_since_first_data > MIN_AGE_DAYS and record_count < MIN_DAYS_THRESHOLD:
            print(f"  -> Flagged {ticker}: First data from {first_date.date()}, but only has {record_count} records.")
            suspect_tickers.append(ticker)
            
    if not suspect_tickers:
        print("\nAudit complete. No tickers with incomplete data found.")
    else:
        print(f"\nAudit complete. Found {len(suspect_tickers)} tickers with likely incomplete data.")
        try:
            with open(OUTPUT_FILE, 'w') as f:
                for ticker in suspect_tickers:
                    f.write(f"{ticker}\n")
            print(f"A list of these tickers has been saved to: {OUTPUT_FILE}")
            print("You can now use the 'delete_ticker_data.py' utility to fix them.")
        except Exception as e:
            print(f"Error saving list to file: {e}")

if __name__ == "__main__":
    run_data_audit()
