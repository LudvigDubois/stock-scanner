# data_fetcher.py
# Description: Fetches historical stock data using yfinance in larger, parallelized batches
# for maximum speed. Uses precise logic to skip batches that are already up-to-date.
# Author: Gemini
# Version: 10.0 (Definitive)

import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

# --- Configuration ---
DB_FILE = "stock_market_data.db"
# This is the default history to fetch for BRAND NEW tickers to ensure
# all calculations (like the 200-day MA) can be performed.
# This should be set to your longest required period.
START_DATE = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
BATCH_SIZE = 500
DELAY_SECONDS = 0.2

# --- Database Functions ---

def initialize_database():
    """
    Creates the database and required tables if they don't exist.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_data (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (ticker, date)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickers_exchange (
                ticker TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                sector TEXT,
                industry TEXT
            )
        ''')
        conn.commit()
        conn.close()
        print("Database initialized successfully.")
    except sqlite3.Error as e:
        print(f"Database error during initialization: {e}")

def get_last_fetch_dates_for_batch(tickers_batch):
    """
    Gets the most recent date for each ticker in a batch.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        placeholders = ','.join('?' for _ in tickers_batch)
        query = f"SELECT ticker, MAX(date) FROM daily_data WHERE ticker IN ({placeholders}) GROUP BY ticker"
        df = pd.read_sql_query(query, conn, params=tickers_batch)
        conn.close()
        return pd.Series(df['MAX(date)'].values, index=df.ticker).to_dict()
    except sqlite3.Error as e:
        print(f"Database error getting last fetch dates: {e}")
        return {}

def save_data_to_db(data):
    """
    Saves a DataFrame of stock data to the SQLite database.
    """
    if data.empty:
        return True
    try:
        conn = sqlite3.connect(DB_FILE)
        data.to_sql('daily_data', conn, if_exists='append', index=False)
        conn.close()
        return True
    except sqlite3.Error as e:
        print(f"Database error while saving data: {e}")
        return False

# --- Data Fetching Functions ---

def load_tickers_from_db():
    """
    Loads tickers from the tickers_exchange table in the database.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query("SELECT ticker FROM tickers_exchange", conn)
        conn.close()
        if df.empty:
            print("No tickers found in the database. Please run get_tickers.py first.")
            return []
        return sorted(df['ticker'].tolist())
    except Exception as e:
        print(f"Error loading tickers from database: {e}")
        return []

def reshape_batch_data(data):
    """
    Reshapes the multi-level column DataFrame from yfinance into a flat format.
    """
    if data.empty or not isinstance(data.columns, pd.MultiIndex):
        return pd.DataFrame()

    data = data.stack(level=1, future_stack=True)
    data.index.names = ['date', 'ticker']
    data.reset_index(inplace=True)
    data.rename(columns={
        'Open': 'open', 'High': 'high', 'Low': 'low', 
        'Close': 'close', 'Volume': 'volume'
    }, inplace=True)
    required_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
    data = data.reindex(columns=required_cols)
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')
    return data

def fetch_and_store_data_in_batches():
    """
    Main function to fetch data for all tickers in batches and store it in the database.
    """
    tickers = load_tickers_from_db()
    if not tickers:
        return

    total_tickers = len(tickers)
    print(f"Starting data fetch for {total_tickers} tickers from database...")

    for i in range(0, total_tickers, BATCH_SIZE):
        batch_tickers = tickers[i:i+BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = -(-total_tickers // BATCH_SIZE)
        
        print(f"\n--- Processing Batch {batch_num}/{total_batches} ({len(batch_tickers)} tickers) ---")

        last_dates = get_last_fetch_dates_for_batch(batch_tickers)

        new_tickers_exist = any(t not in last_dates for t in batch_tickers)

        if new_tickers_exist or not last_dates:
            # If there's at least one new ticker, we must download the full history for the batch to backfill it.
            # The global START_DATE is used here.
            batch_start_date = START_DATE
            print("New tickers found or batch is empty, fetching full history...")
        else:
            # If all tickers in the batch already exist, we can do a quick, targeted update.
            today = datetime.now().date()
            weekday = today.weekday() # Monday is 0, Sunday is 6

            # Determine what date the data *should* be updated to for a nightly scan.
            if weekday >= 5: # If today is Saturday or Sunday...
                # ...the data is up-to-date if it includes last Friday's close.
                required_date = today - timedelta(days=weekday - 4)
            else: # If today is a weekday...
                # ...the data is up-to-date if it includes today's close (or yesterday's if market is closed).
                # To be safe, we check against yesterday.
                required_date = today - timedelta(days=1)

            min_last_date_str = min(last_dates.values())
            min_last_date = datetime.strptime(min_last_date_str, '%Y-%m-%d').date()

            if min_last_date >= required_date:
                print("All tickers in this batch are up to date. Skipping.")
                continue
            
            # If not up-to-date, calculate the start date for a targeted update.
            # This is what makes daily updates fast.
            batch_start_date = (min_last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"Fetching data since {batch_start_date}...")

        try:
            data = yf.download(
                batch_tickers, 
                start=batch_start_date, 
                progress=False,
                threads=True
            )
            
            if data.empty:
                print("No new data found for this batch.")
                continue

            reshaped_data = reshape_batch_data(data)
            
            rows_to_save = []
            for _, row in reshaped_data.iterrows():
                ticker = row['ticker']
                last_date_for_ticker = last_dates.get(ticker)
                if not last_date_for_ticker or row['date'] > last_date_for_ticker:
                    rows_to_save.append(row)
            
            final_df = pd.DataFrame(rows_to_save)

            if not final_df.empty:
                if save_data_to_db(final_df):
                    print(f"Successfully fetched and stored {len(final_df)} new records.")
            else:
                print("No new records to store for this batch.")

        except Exception as e:
            print(f"An error occurred while processing this batch: {e}")
        
        time.sleep(DELAY_SECONDS)

if __name__ == "__main__":
    initialize_database()
    fetch_and_store_data_in_batches()
    print("\nData fetching process complete.")
