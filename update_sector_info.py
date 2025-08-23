# update_sector_info.py
# Description: A script to fetch and store the sector and industry for each
# ticker in the database. This is a slow, one-time process.
# Author: Gemini
# Version: 1.0

import sqlite3
import yfinance as yf
import pandas as pd
import time

# --- Configuration ---
DB_FILE = "stock_market_data.db"
DELAY_SECONDS = 0.2 # Delay between requests to avoid rate limiting

def setup_database_columns():
    """
    Adds 'sector' and 'industry' columns to the tickers_exchange table if they don't exist.
    """
    print("Verifying database schema...")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check for sector column
        cursor.execute("PRAGMA table_info(tickers_exchange)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'sector' not in columns:
            cursor.execute("ALTER TABLE tickers_exchange ADD COLUMN sector TEXT")
            print("Added 'sector' column to the database.")
        
        # Check for industry column
        if 'industry' not in columns:
            cursor.execute("ALTER TABLE tickers_exchange ADD COLUMN industry TEXT")
            print("Added 'industry' column to the database.")
            
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error during setup: {e}")

def get_tickers_without_sector_info():
    """
    Finds tickers in the database that are missing sector information.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        # Select tickers where sector is NULL or an empty string
        query = "SELECT ticker FROM tickers_exchange WHERE sector IS NULL OR sector = ''"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df['ticker'].tolist()
    except Exception as e:
        print(f"Error loading tickers from database: {e}")
        return []

def update_sector_info():
    """
    Main function to loop through tickers and update their sector/industry info.
    """
    tickers_to_update = get_tickers_without_sector_info()
    if not tickers_to_update:
        print("All tickers already have sector information. No updates needed.")
        return

    total_tickers = len(tickers_to_update)
    print(f"Found {total_tickers} tickers missing sector information. Starting update...")

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for i, ticker_symbol in enumerate(tickers_to_update):
        print(f"Processing {i+1}/{total_tickers}: {ticker_symbol}")
        try:
            ticker_obj = yf.Ticker(ticker_symbol)
            info = ticker_obj.info
            
            # ETFs/Funds often don't have a sector/industry, so we provide defaults.
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            
            # Update the database for this ticker
            cursor.execute(
                "UPDATE tickers_exchange SET sector = ?, industry = ? WHERE ticker = ?",
                (sector, industry, ticker_symbol)
            )
            conn.commit()
            
        except Exception as e:
            print(f"  -> Could not fetch info for {ticker_symbol}. Error: {e}")
            # Mark as 'Error' so we don't try it again next time
            cursor.execute(
                "UPDATE tickers_exchange SET sector = ?, industry = ? WHERE ticker = ?",
                ('Error', 'Error', ticker_symbol)
            )
            conn.commit()
            
        time.sleep(DELAY_SECONDS)

    conn.close()
    print("\nSector and industry update process complete.")

if __name__ == "__main__":
    setup_database_columns()
    update_sector_info()
