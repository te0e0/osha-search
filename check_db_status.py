import sqlite3
import os

DB_PATH = "osha_ca.db"

def check_db():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    print(f"Tables: {tables}")
    
    # Check for 'R' cases in violations
    try:
        cur.execute("SELECT COUNT(*) FROM violations WHERE LATEST_EVENT = 'R'")
        count_r = cur.fetchone()[0]
        print(f"Review Commission (R) cases count: {count_r}")
        
        if count_r > 0:
            cur.execute("SELECT ACTIVITY_NR, CITATION_ID, LATEST_EVENT FROM violations WHERE LATEST_EVENT = 'R' LIMIT 5")
            print("Sample R cases:", cur.fetchall())
            
        # Check for 'C' cases (Contested)
        cur.execute("SELECT COUNT(*) FROM violations WHERE LATEST_EVENT = 'C'")
        count_c = cur.fetchone()[0]
        print(f"Contested (C) cases count: {count_c}")
        
        # Check for columns like CONTEST_DATE
        cur.execute("PRAGMA table_info(violations)")
        cols = cur.fetchall()
        print("violations table columns:", [c[1] for c in cols])
        
    except Exception as e:
        print(f"Error querying violations: {e}")
        
    conn.close()

if __name__ == "__main__":
    check_db()
