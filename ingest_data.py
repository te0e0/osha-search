import sqlite3
import pandas as pd
import os
import glob

DB_PATH = "osha_ca.db"
DATA_DIR = "data"

def get_cols(df):
    # Find exact names for required columns
    cols = {c.upper(): c for c in df.columns}
    state = cols.get('SITE_STATE') or cols.get('STATE') or cols.get('SITE_STATE_FLAG')
    act = cols.get('ACTIVITY_NR') or cols.get('ACTIVITY_NUMBER')
    return state, act

def ingest():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    all_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    print(f"Processing {len(all_files)} files...")
    
    total_insp = 0
    total_viol = 0
    ca_activities = set()

    # Pass 1: Inspections (Files with SITE_STATE)
    for f in all_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            state_col, act_col = get_cols(df)
            
            if state_col and 'ESTAB_NAME' in df.columns:
                df_ca = df[df[state_col] == 'CA'].copy()
                if not df_ca.empty:
                    # Normalize names for SQL
                    if act_col: df_ca = df_ca.rename(columns={act_col: 'ACTIVITY_NR'})
                    df_ca.to_sql('inspections', conn, if_exists='append', index=False)
                    total_insp += len(df_ca)
                    if act_col: ca_activities.update(df_ca['ACTIVITY_NR'])
        except: continue
    
    print(f"Found {total_insp} inspections. Pass 2: Violations...")

    # Pass 2: Violations
    for f in all_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            _, act_col = get_cols(df)
            
            if act_col and ('STANDARD' in df.columns or 'VIOLATION_TYPE' in df.columns):
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
