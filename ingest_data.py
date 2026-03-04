import sqlite3
import pandas as pd
import requests
import zipfile
import os
import glob

# URLs for datasets
INSPECTION_URL = "https://data.dol.gov/data-catalog/OSHA/inspection/OSHA_inspection.zip"
VIOLATION_URL = "https://data.dol.gov/data-catalog/OSHA/violation/OSHA_violation.zip"
DB_PATH = "osha_ca.db"
DATA_DIR = "data"

def download_and_extract(url, name):
    print(f"--- Starting {name} ---")
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # Check if we already have CSVs for this dataset
    existing_csvs = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not existing_csvs:
        print(f"Streaming from {url}...")
        try:
            temp_zip = os.path.join(DATA_DIR, f"temp_{name}.zip")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(temp_zip, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            print(f"Download complete. Extracting {name}...")
            with zipfile.ZipFile(temp_zip, 'r') as z:
                z.extractall(DATA_DIR)
            
            os.remove(temp_zip) # Delete ZIP to save space
            print(f"Extraction for {name} complete.")
        except Exception as e:
            print(f"Error processing {name}: {e}")
    else:
        print(f"Using existing {name} data in {DATA_DIR}.")

def get_cols(df):
    cols = {c.upper(): c for c in df.columns}
    state = cols.get('SITE_STATE') or cols.get('STATE') or cols.get('SITE_STATE_FLAG')
    act = cols.get('ACTIVITY_NR') or cols.get('ACTIVITY_NUMBER')
    return state, act

def ingest():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Ensure data is present
    download_and_extract(INSPECTION_URL, "Inspection")
    download_and_extract(VIOLATION_URL, "Violation")
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL") # Improve concurrency
    
    # Explicitly create tables to avoid "no such table" errors on some environments
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inspections (
            ACTIVITY_NR TEXT PRIMARY KEY,
            ESTAB_NAME TEXT,
            SITE_ADDRESS TEXT,
            SITE_CITY TEXT,
            SITE_STATE TEXT,
            SITE_ZIP TEXT,
            OPEN_DATE TEXT,
            INSP_TYPE TEXT,
            INSP_SCOPE TEXT,
            UNION_STATUS TEXT,
            SIC_CODE TEXT,
            NAICS_CODE TEXT,
            OWNER_TYPE TEXT,
            CLOSE_CASE_DATE TEXT,
            CASE_MOD_DATE TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS violations (
            ACTIVITY_NR TEXT,
            CITATION_ID TEXT,
            STANDARD TEXT,
            VIOL_TYPE TEXT,
            INITIAL_PENALTY REAL,
            CURRENT_PENALTY REAL,
            ABATE_DATE TEXT,
            FOREIGN KEY(ACTIVITY_NR) REFERENCES inspections(ACTIVITY_NR)
        )
    """)
    
    all_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"Found {len(all_files)} files. Starting chunked processing...")
    
    total_insp = 0
    total_viol = 0
    ca_activities = set()

    # Pass 1: Inspections (Chunked)
    for f in all_files:
        try:
            # Detect columns first with a tiny read
            sample = pd.read_csv(f, nrows=1)
            state_col, act_col = get_cols(sample)
            
            if state_col and 'ESTAB_NAME' in sample.columns:
                reader = pd.read_csv(f, chunksize=20000, low_memory=False)
                for chunk in reader:
                    df_ca = chunk[chunk[state_col] == 'CA'].copy()
                    if not df_ca.empty:
                        if act_col: df_ca = df_ca.rename(columns={act_col: 'ACTIVITY_NR'})
                        df_ca.to_sql('inspections', conn, if_exists='append', index=False)
                        total_insp += len(df_ca)
                        if act_col: ca_activities.update(df_ca['ACTIVITY_NR'])
        except Exception as e:
            print(f"Error in inspection pass for {f}: {e}")
    
    print(f"Indexed {total_insp} inspections. Processing violations in chunks...")

    # Pass 2: Violations (Chunked)
    for f in all_files:
        try:
            sample = pd.read_csv(f, nrows=1)
            _, act_col = get_cols(sample)
            
            if act_col and ('STANDARD' in sample.columns or 'VIOL_TYPE' in sample.columns):
                reader = pd.read_csv(f, chunksize=20000, low_memory=False)
                for chunk in reader:
                    df_viol = chunk[chunk[act_col].isin(ca_activities)].copy()
                    if not df_viol.empty:
                        df_viol = df_viol.rename(columns={act_col: 'ACTIVITY_NR'})
                        df_viol.to_sql('violations', conn, if_exists='append', index=False)
                        total_viol += len(df_viol)
        except Exception as e:
            print(f"Error in violation pass for {f}: {e}")
    
    if total_insp > 0:
        conn.execute("CREATE INDEX idx_insp_act ON inspections(ACTIVITY_NR)")
        conn.execute("CREATE INDEX idx_viol_act ON violations(ACTIVITY_NR)")
        conn.execute("CREATE INDEX idx_insp_name ON inspections(ESTAB_NAME)")
    
    conn.commit()
    conn.close()

    # CRITICAL: Clean up large CSV files to save Render disk space
    print("Ingestion complete. Cleaning up raw CSV files...")
    for f in all_files:
        try:
            os.remove(f)
        except:
            pass
    print("DONE.")

if __name__ == "__main__":
    ingest()
