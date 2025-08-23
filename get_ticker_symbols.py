# get_tickers.py
# Description: Fetches tickers from NASDAQ, NYSE, AMEX, and ARCA, and correctly
# maps exchanges for TradingView compatibility. Now includes sector/industry columns.
# Author: Gemini
# Version: 7.0

import pandas as pd
import sqlite3
import numpy as np

# --- Configuration ---
DB_FILE = "stock_market_data.db"

def initialize_database():
    """
    Ensures the database and the tickers_exchange table exist with the new schema.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # The schema now includes nullable columns for sector and industry
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
    except sqlite3.Error as e:
        print(f"Database error during initialization: {e}")

def get_all_tickers_with_mapping():
    """
    Fetches all stock tickers and maps their exchanges for TradingView.
    """
    print("Fetching tickers from NASDAQ, NYSE, AMEX, and ARCA...")
    
    nasdaq_url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    other_url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

    try:
        nasdaq_df = pd.read_csv(nasdaq_url, sep='|')
        nasdaq_df = nasdaq_df[['Symbol', 'ETF']].copy()
        nasdaq_df.rename(columns={'Symbol': 'ticker'}, inplace=True)
        nasdaq_df['exchange'] = 'NASDAQ'
        nasdaq_df['asset_class'] = np.where(nasdaq_df['ETF'] == 'Y', 'Fund', 'Stock')

        # Fetch tickers from other exchanges
        other_df = pd.read_csv(other_url, sep='|')
        
        # Keep NYSE (N), AMEX (A), and ARCA (P) tickers ***
        included_exchanges = ['N', 'A', 'P']
        filtered_other_df = other_df[other_df['Exchange'].isin(included_exchanges)].copy()
        
        # Define the mapping for TradingView
        # Both 'A' (AMEX) and 'P' (ARCA) will be labeled as 'AMEX'
        exchange_map = {
            'N': 'NYSE',
            'A': 'AMEX',
            'P': 'AMEX' # Map ARCA to AMEX
        }
        
        filtered_other_df['exchange'] = filtered_other_df['Exchange'].map(exchange_map)
        filtered_other_df.rename(columns={'ACT Symbol': 'ticker'}, inplace=True)
        filtered_other_df['asset_class'] = np.where(filtered_other_df['ETF'] == 'Y', 'Fund', 'Stock')
        
        # Combine the NASDAQ and the other filtered dataframes
        all_tickers_df = pd.concat([nasdaq_df, filtered_other_df], ignore_index=True)
        
        # Clean up data
        all_tickers_df.dropna(subset=['ticker'], inplace=True)
        all_tickers_df = all_tickers_df[~all_tickers_df['ticker'].str.contains('\$|\.|File Creation Time')]
        all_tickers_df.drop_duplicates(subset=['ticker'], inplace=True)
        
        # Select only the columns we need for the database
        final_df = all_tickers_df[['ticker', 'exchange', 'asset_class']]
        
        print(f"Successfully fetched {len(final_df)} unique tickers.")
        return final_df

    except Exception as e:
        print(f"An error occurred while fetching tickers: {e}")
        return pd.DataFrame()

def save_tickers_to_db(tickers_df):
    """
    Saves the ticker-exchange-class mapping to the database.
    """
    if tickers_df.empty:
        return
    try:
        conn = sqlite3.connect(DB_FILE)
        # Using 'replace' is fine here as it will recreate the table with the new schema
        tickers_df.to_sql('tickers_exchange', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Saved {len(tickers_df)} ticker mappings to the database.")
    except sqlite3.Error as e:
        print(f"Database error while saving tickers: {e}")

if __name__ == "__main__":
    initialize_database()
    tickers_df = get_all_tickers_with_mapping()
    if not tickers_df.empty:
        save_tickers_to_db(tickers_df)
