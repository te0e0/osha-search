import sqlite3
import pandas as pd
import requests
import os
import json

DB_PATH = "osha_ca.db"

def load_industry_codes():
    if not os.path.exists(DB_PATH):
        print(f"Database {DB_PATH} not found. Please run ingest_data.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create tables
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sic_codes (
            code TEXT PRIMARY KEY,
            title TEXT
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS naics_codes (
            code TEXT PRIMARY KEY,
            title TEXT
        )
    ''')
    
    # 1. Load SIC Codes (from saintsjd/sic4-list)
    print("Downloading SIC codes...")
    sic_url = "https://raw.githubusercontent.com/saintsjd/sic4-list/master/sic-codes.csv"
    try:
        df_sic = pd.read_csv(sic_url, dtype=str)
        # Ensure we just use the relevant columns: 'SIC' and 'Description'
        records_sic = []
        for index, row in df_sic.iterrows():
            code = str(row.get('SIC', '')).strip()
            # Left pad to 4 digits if needed (optional, but good practice for SIC)
            code = code.zfill(4)
            title = str(row.get('Description', '')).strip()
            if code and title:
                records_sic.append((code, title))
                
        cur.executemany("INSERT OR REPLACE INTO sic_codes (code, title) VALUES (?, ?)", records_sic)
        print(f"Loaded {len(records_sic)} SIC codes.")
    except Exception as e:
        print(f"Error loading SIC codes: {e}")

    # 2. Load NAICS Codes (from codeforamerica/naics-api 2012 codes)
    print("Downloading NAICS codes...")
    naics_url = "https://raw.githubusercontent.com/codeforamerica/naics-api/master/data/codes-2012.json"
    try:
        r = requests.get(naics_url)
        r.raise_for_status()
        data_naics = r.json()
        
        records_naics = []
        for code, details in data_naics.items():
            if isinstance(details, dict) and 'title' in details:
                title = str(details['title']).strip()
                if code and title:
                    records_naics.append((str(code), title))
                    
        cur.executemany("INSERT OR REPLACE INTO naics_codes (code, title) VALUES (?, ?)", records_naics)
        print(f"Loaded {len(records_naics)} NAICS codes.")
    except Exception as e:
        print(f"Error loading NAICS codes: {e}")

    conn.commit()
    conn.close()
    print("Done loading industry codes.")

if __name__ == "__main__":
    load_industry_codes()
