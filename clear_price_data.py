# clear_price_data.py
# Description: Clear price data without deleting ticker/sector data

import sqlite3

DB_FILE = "stock_market_data.db"
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
print("Clearing old price data from the 'daily_data' table...")
cursor.execute("DELETE FROM daily_data")
conn.commit()
conn.close()
print("Done.")