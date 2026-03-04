import sqlite3
import pandas as pd
import requests
import zipfile
import io
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
        print(f"Downloading from {url}...")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(DATA_DIR)
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
    all_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    print(f"Found {len(all_files)} files. Starting processing...")
    
    total_insp = 0
    total_viol = 0
    ca_activities = set()

    # Pass 1: Inspections
    for f in all_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            state_col, act_col = get_cols(df)
            if state_col and 'ESTAB_NAME' in df.columns:
                df_ca = df[df[state_col] == 'CA'].copy()
                if not df_ca.empty:
                    if act_col: df_ca = df_ca.rename(columns={act_col: 'ACTIVITY_NR'})
                    df_ca.to_sql('inspections', conn, if_exists='append', index=False)
                    total_insp += len(df_ca)
                    if act_col: ca_activities.update(df_ca['ACTIVITY_NR'])
        except: continue
    
    print(f"Indexed {total_insp} inspections. Processing violations...")

    # Pass 2: Violations
    for f in all_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            _, act_col = get_cols(df)
            if act_col and ('STANDARD' in df.columns or 'VIOL_TYPE' in df.columns):
                df_viol = df[df[act_col].isin(ca_activities)].copy()
                if not df_viol.empty:
                    df_viol = df_viol.rename(columns={act_col: 'ACTIVITY_NR'})
                    df_viol.to_sql('violations', conn, if_exists='append', index=False)
                    total_viol += len(df_viol)
        except: continue

    print(f"DONE. Inspections: {total_insp}, Violations: {total_viol}")
    
    if total_insp > 0:
        conn.execute("CREATE INDEX idx_insp_act ON inspections(ACTIVITY_NR)")
        conn.execute("CREATE INDEX idx_viol_act ON violations(ACTIVITY_NR)")
        conn.execute("CREATE INDEX idx_insp_name ON inspections(ESTAB_NAME)")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    ingest()
