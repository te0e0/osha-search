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
            CASE_MOD_DATE TEXT,
            REPORTING_ID TEXT
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
            LATEST_EVENT TEXT,
            FINAL_ORDER_DATE TEXT,
            CONTEST_DATE TEXT,
            NR_INSTANCES INTEGER,
            NR_EXPOSED INTEGER,
            GRAVITY TEXT,
            FOREIGN KEY(ACTIVITY_NR) REFERENCES inspections(ACTIVITY_NR)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_temp (
            ACTIVITY_NR TEXT,
            CITATION_ID TEXT,
            LATEST_EVENT TEXT
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
        'OWNER_TYPE', 'CLOSE_CASE_DATE', 'CASE_MOD_DATE', 'REPORTING_ID', 'REPORT_ID'
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
                        # Normalize reporting ID name if federal data uses REPORT_ID
                        if 'REPORT_ID' in df_ca.columns and 'REPORTING_ID' not in df_ca.columns:
                            df_ca.rename(columns={'REPORT_ID': 'REPORTING_ID'}, inplace=True)
                            
                        # Only keep columns that exist in both the table and the CSV
                        cols_to_keep = [c for c in REQUIRED_INSP_COLS if c in df_ca.columns]
                        # Remove REPORT_ID from cols_to_keep if we just renamed it
                        if 'REPORT_ID' in cols_to_keep:
                            cols_to_keep.remove('REPORT_ID')
                            
                        df_ca = df_ca[cols_to_keep]
                        
                        df_ca.drop_duplicates(subset=['ACTIVITY_NR'], inplace=True)
                        df_ca.to_sql('inspections_temp', conn, if_exists='replace', index=False)
                        conn.execute("INSERT OR IGNORE INTO inspections SELECT * FROM inspections_temp")
                        
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
        'INITIAL_PENALTY', 'CURRENT_PENALTY', 'ABATE_DATE', 'REC', 'FINAL_ORDER_DATE',
        'CONTEST_DATE', 'NR_INSTANCES', 'NR_EXPOSED', 'GRAVITY'
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
                        # Rename REC to LATEST_EVENT if it exists (REC is the raw CSV column name)
                        if 'REC' in df_viol.columns:
                            df_viol = df_viol.rename(columns={'REC': 'LATEST_EVENT'})
                        
                        # Define the target columns for the database
                        target_cols = ['ACTIVITY_NR', 'CITATION_ID', 'STANDARD', 'VIOL_TYPE', 
                                      'INITIAL_PENALTY', 'CURRENT_PENALTY', 'ABATE_DATE', 
                                      'LATEST_EVENT', 'FINAL_ORDER_DATE',
                                      'CONTEST_DATE', 'NR_INSTANCES', 'NR_EXPOSED', 'GRAVITY']
                        
                        # Only keep columns that are actually in the dataframe
                        cols_to_keep = [c for c in target_cols if c in df_viol.columns]
                        df_viol = df_viol[cols_to_keep]
                        
                        
                        df_viol.to_sql('violations', conn, if_exists='append', index=False)
                        total_viol += len(df_viol)
            
            elif act_col and 'REC' in sample.columns and ('STANDARD' not in sample.columns):
                reader = pd.read_csv(f, chunksize=20000, low_memory=False)
                for chunk in reader:
                    if act_col and act_col != 'ACTIVITY_NR':
                        chunk = chunk.rename(columns={act_col: 'ACTIVITY_NR'})
                        
                    df_evt = chunk[chunk['ACTIVITY_NR'].isin(ca_activities)].copy()
                    if not df_evt.empty:
                        df_evt = df_evt.rename(columns={'REC': 'LATEST_EVENT'})
                        
                        target_cols = ['ACTIVITY_NR', 'CITATION_ID', 'LATEST_EVENT']
                        cols_to_keep = [c for c in target_cols if c in df_evt.columns]
                        df_evt = df_evt[cols_to_keep]
                        
                        df_evt.to_sql('events_temp', conn, if_exists='append', index=False)

        except Exception as e:
            import traceback
            print(f"Error in violation pass for {f}: {e}")
            traceback.print_exc()
    
    if total_insp > 0:
        conn.execute("CREATE INDEX idx_insp_act ON inspections(ACTIVITY_NR)")
        conn.execute("CREATE INDEX idx_viol_act ON violations(ACTIVITY_NR)")
        conn.execute("CREATE INDEX idx_insp_name ON inspections(ESTAB_NAME)")
        
        # Merge event codes from events_temp into violations
        conn.execute("CREATE INDEX idx_evt_temp ON events_temp(ACTIVITY_NR, CITATION_ID)")
        
        # Priority order for updating LATEST_EVENT: R > J/A > F > I
        # 'R' is Review Commission (Highest appeal level)
        conn.execute("""
            UPDATE violations
            SET LATEST_EVENT = 'R'
            WHERE EXISTS (
                SELECT 1 FROM events_temp 
                WHERE events_temp.ACTIVITY_NR = violations.ACTIVITY_NR 
                  AND (events_temp.CITATION_ID = violations.CITATION_ID OR events_temp.CITATION_ID IS NULL)
                  AND events_temp.LATEST_EVENT = 'R'
            )
            AND (LATEST_EVENT != 'R' OR LATEST_EVENT IS NULL)
        """)

        # 'J' is ALJ Decision
        conn.execute("""
            UPDATE violations
            SET LATEST_EVENT = 'J'
            WHERE EXISTS (
                SELECT 1 FROM events_temp 
                WHERE events_temp.ACTIVITY_NR = violations.ACTIVITY_NR 
                  AND (events_temp.CITATION_ID = violations.CITATION_ID OR events_temp.CITATION_ID IS NULL)
                  AND events_temp.LATEST_EVENT = 'J'
            )
            AND (LATEST_EVENT NOT IN ('R', 'J') OR LATEST_EVENT IS NULL)
        """)
        
        # 'A' is ALJ Affirm
        conn.execute("""
            UPDATE violations
            SET LATEST_EVENT = 'A'
            WHERE EXISTS (
                SELECT 1 FROM events_temp 
                WHERE events_temp.ACTIVITY_NR = violations.ACTIVITY_NR 
                  AND (events_temp.CITATION_ID = violations.CITATION_ID OR events_temp.CITATION_ID IS NULL)
                  AND events_temp.LATEST_EVENT = 'A'
            )
            AND (LATEST_EVENT NOT IN ('R', 'J', 'A') OR LATEST_EVENT IS NULL)
        """)

        # 'F' is Formal Settlement
        conn.execute("""
            UPDATE violations
            SET LATEST_EVENT = 'F'
            WHERE EXISTS (
                SELECT 1 FROM events_temp 
                WHERE events_temp.ACTIVITY_NR = violations.ACTIVITY_NR 
                  AND (events_temp.CITATION_ID = violations.CITATION_ID OR events_temp.CITATION_ID IS NULL)
                  AND events_temp.LATEST_EVENT = 'F'
            )
            AND (LATEST_EVENT NOT IN ('R', 'J', 'A', 'F') OR LATEST_EVENT IS NULL)
        """)

        # 'I' is Informal Settlement
        conn.execute("""
            UPDATE violations
            SET LATEST_EVENT = 'I'
            WHERE EXISTS (
                SELECT 1 FROM events_temp 
                WHERE events_temp.ACTIVITY_NR = violations.ACTIVITY_NR 
                  AND (events_temp.CITATION_ID = violations.CITATION_ID OR events_temp.CITATION_ID IS NULL)
                  AND events_temp.LATEST_EVENT = 'I'
            )
            AND (LATEST_EVENT NOT IN ('R', 'J', 'A', 'F', 'I') OR LATEST_EVENT IS NULL)
        """)
        
        conn.execute("DROP TABLE events_temp")
    
    conn.commit()
    conn.close()

    # Load industry code mapping tables (SIC/NAICS titles)
    try:
        import load_industry_codes
        load_industry_codes.load_industry_codes()
    except Exception as e:
        print(f"Warning: Industry codes could not be loaded: {e}")

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
