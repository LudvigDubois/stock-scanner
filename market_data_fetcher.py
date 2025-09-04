# data_fetcher.py
# Description: Fetches historical stock data using yfinance in larger, parallelized batches
# for maximum speed. This version uses a hybrid approach for maximum efficiency.
# Author: Gemini
# Version: 14.0 (Definitive Hybrid Logic)

import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

# --- Configuration ---
DB_FILE = "stock_market_data.db"
# The default history to fetch for BRAND NEW tickers.
START_DATE = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
BATCH_SIZE = 500
DELAY_SECONDS = 0.5

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
    
    data.dropna(subset=['open'], inplace=True)
    
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
        
        # --- FINAL FIX: HYBRID APPROACH ---
        # Separate tickers into three groups: new, out-of-date, and up-to-date.
        
        today = datetime.now().date()
        weekday = today.weekday()
        required_date = today - timedelta(days=weekday - 4) if weekday >= 5 else today

        new_tickers = [t for t in batch_tickers if t not in last_dates]
        out_of_date_tickers = [t for t in batch_tickers if t in last_dates and datetime.strptime(last_dates[t], '%Y-%m-%d').date() < required_date]

        if not new_tickers and not out_of_date_tickers:
            print("All tickers in this batch are up to date. Skipping.")
            continue

        # --- Download Logic ---
        all_data_to_save = []

        # 1. Download full history for brand new tickers
        if new_tickers:
            print(f"Found {len(new_tickers)} new tickers. Fetching full history...")
            data_new = yf.download(new_tickers, start=START_DATE, progress=False, threads=True)
            if data_new is not None and not data_new.empty:
                all_data_to_save.append(reshape_batch_data(data_new))

        # 2. Download recent history for out-of-date tickers
        if out_of_date_tickers:
            print(f"Found {len(out_of_date_tickers)} out-of-date tickers. Fetching recent data...")
            # Calculate start date based on the oldest of the out-of-date tickers
            oldest_date_str = min(last_dates[t] for t in out_of_date_tickers)
            start_update_date = (datetime.strptime(oldest_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            data_outdated = yf.download(out_of_date_tickers, start=start_update_date, progress=False, threads=True)
            if data_outdated is not None and not data_outdated.empty:
                reshaped_outdated = reshape_batch_data(data_outdated)
                # Filter just in case (shouldn't be necessary but is safe)
                rows_to_save = [row for _, row in reshaped_outdated.iterrows() if row['date'] > last_dates[row['ticker']]]
                if rows_to_save:
                    all_data_to_save.append(pd.DataFrame(rows_to_save))

        # --- Save to DB ---
        if all_data_to_save:
            final_df = pd.concat(all_data_to_save, ignore_index=True)
            if not final_df.empty:
                if save_data_to_db(final_df):
                    print(f"Successfully fetched and stored {len(final_df)} new records.")
            else:
                print("No new records to store for this batch.")
        else:
            print("No new data found for this batch.")

        time.sleep(DELAY_SECONDS)

if __name__ == "__main__":
    initialize_database()
    fetch_and_store_data_in_batches()
    print("\nData fetching process complete.")
