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
    
    print(f"Streaming from {url}...")
    try:
        temp_zip = os.path.join(DATA_DIR, f"temp_{name}.zip")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(temp_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno()) # Force write to disk immediately
        
        print(f"Download complete. Extracting {name}...")
        with zipfile.ZipFile(temp_zip, 'r') as z:
            z.extractall(DATA_DIR)
        
        os.remove(temp_zip) # Delete ZIP to save space
        print(f"Extraction for {name} complete.")
    except Exception as e:
        print(f"Error processing {name}: {e}")

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
    REQUIRED_INSP_COLS = [
        'ACTIVITY_NR', 'ESTAB_NAME', 'SITE_ADDRESS', 'SITE_CITY', 
        'SITE_STATE', 'SITE_ZIP', 'OPEN_DATE', 'INSP_TYPE', 
        'INSP_SCOPE', 'UNION_STATUS', 'SIC_CODE', 'NAICS_CODE', 
        'OWNER_TYPE', 'CLOSE_CASE_DATE', 'CASE_MOD_DATE'
    ]

    for f in all_files:
        try:
            # Detect columns first with a tiny read
            sample = pd.read_csv(f, nrows=1)
            state_col, act_col = get_cols(sample)
            
            if state_col and 'ESTAB_NAME' in sample.columns:
                reader = pd.read_csv(f, chunksize=20000, low_memory=False)
                for chunk in reader:
                    # Rename activity number if needed
                    if act_col and act_col != 'ACTIVITY_NR':
                        chunk = chunk.rename(columns={act_col: 'ACTIVITY_NR'})
                    
                    df_ca = chunk[chunk[state_col] == 'CA'].copy()
                    if not df_ca.empty:
                        # Only keep columns that exist in both the table and the CSV
                        cols_to_keep = [c for c in REQUIRED_INSP_COLS if c in df_ca.columns]
                        df_ca = df_ca[cols_to_keep]
                        
                        df_ca.to_sql('inspections', conn, if_exists='append', index=False)
                        total_insp += len(df_ca)
                        ca_activities.update(df_ca['ACTIVITY_NR'])
        except Exception as e:
            import traceback
            print(f"Error in inspection pass for {f}: {e}")
            traceback.print_exc()
    
    print(f"Indexed {total_insp} inspections. Processing violations in chunks...")

    # Pass 2: Violations (Chunked)
    REQUIRED_VIOL_COLS = [
        'ACTIVITY_NR', 'CITATION_ID', 'STANDARD', 'VIOL_TYPE', 
        'INITIAL_PENALTY', 'CURRENT_PENALTY', 'ABATE_DATE'
    ]

    for f in all_files:
        try:
            sample = pd.read_csv(f, nrows=1)
            _, act_col = get_cols(sample)
            
            if act_col and ('STANDARD' in sample.columns or 'VIOL_TYPE' in sample.columns):
                reader = pd.read_csv(f, chunksize=20000, low_memory=False)
                for chunk in reader:
                    # Rename activity number if needed
                    if act_col and act_col != 'ACTIVITY_NR':
                        chunk = chunk.rename(columns={act_col: 'ACTIVITY_NR'})
                        
                    df_viol = chunk[chunk['ACTIVITY_NR'].isin(ca_activities)].copy()
                    if not df_viol.empty:
                        cols_to_keep = [c for c in REQUIRED_VIOL_COLS if c in df_viol.columns]
                        df_viol = df_viol[cols_to_keep]
                        
                        df_viol.to_sql('violations', conn, if_exists='append', index=False)
                        total_viol += len(df_viol)
        except Exception as e:
            import traceback
            print(f"Error in violation pass for {f}: {e}")
            traceback.print_exc()
    
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
