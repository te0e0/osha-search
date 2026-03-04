from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
import sqlite3
import pandas as pd
from typing import Optional
import uvicorn
import os

app = FastAPI(title="Cal/OSHA Search Dashboard")

DB_PATH = "osha_ca.db"

def get_db_connection():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/search")
def search_inspections(
    employer: Optional[str] = None,
    city: Optional[str] = None,
    year: Optional[int] = None,
    severity: Optional[str] = None,
    standard: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    conn = get_db_connection()
    if not conn:
        return {"error": "Database not ready yet. Please wait for ingestion to complete.", "results": []}

    query = """
    SELECT i.*, GROUP_CONCAT(v.STANDARD) as standards, GROUP_CONCAT(v.VIOL_TYPE) as severities
    FROM inspections i
    LEFT JOIN violations v ON i.ACTIVITY_NR = v.ACTIVITY_NR
    WHERE 1=1
    """
    params = []

    if employer:
        query += " AND i.ESTAB_NAME LIKE ?"
        params.append(f"%{employer}%")
    
    if city:
        query += " AND i.SITE_CITY LIKE ?"
        params.append(f"%{city}%")
    
    if year:
        query += " AND i.OPEN_DATE LIKE ?"
        params.append(f"%{year}%")
        
    if severity:
        query += " AND v.VIOL_TYPE = ?"
        params.append(severity)
        
    if standard:
        query += " AND v.STANDARD LIKE ?"
        params.append(f"%{standard}%")

    query += " GROUP BY i.ACTIVITY_NR ORDER BY i.OPEN_DATE DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        df = pd.read_sql_query(query, conn, params=params)
        return {"results": df.to_dict(orient="records")}
    finally:
        conn.close()

@app.get("/", response_class=HTMLResponse)
@app.head("/", response_class=HTMLResponse)
def read_root():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cal/OSHA Inspection Search</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #2563eb;
            --bg: #0f172a;
            --card: rgba(30, 41, 59, 0.7);
            --text: #f8fafc;
            --muted: #94a3b8;
        }
        body {
            font-family: 'Inter', sans-serif;
            background: var(--bg);
            color: var(--text);
            margin: 0;
            padding: 2rem;
            min-height: 100vh;
            background: radial-gradient(circle at top right, #1e293b, #0f172a);
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { font-weight: 300; font-size: 2.5rem; margin-bottom: 2rem; text-align: center; color: #fff; }
        
        /* Search Box */
        .search-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            background: var(--card);
            backdrop-filter: blur(10px);
            padding: 2rem;
            border-radius: 1rem;
            border: 1px solid rgba(255,255,255,0.1);
            margin-bottom: 2rem;
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        }
        .input-group { display: flex; flex-direction: column; gap: 0.5rem; }
        label { font-size: 0.8rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
        input, select {
            background: rgba(15, 23, 42, 0.5);
            border: 1px solid rgba(255,255,255,0.1);
            color: #fff;
            padding: 0.75rem;
            border-radius: 0.5rem;
            transition: all 0.2s;
        }
        input:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.2); }
        button {
            background: var(--primary);
            color: white;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: 0.5rem;
            cursor: pointer;
            font-weight: 600;
            align-self: flex-end;
            transition: transform 0.1s;
        }
        button:active { transform: scale(0.98); }

        /* Results Table */
        .table-container {
            background: var(--card);
            backdrop-filter: blur(10px);
            border-radius: 1rem;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.1);
        }
        table { width: 100%; border-collapse: collapse; }
        th { text-align: left; padding: 1rem; background: rgba(15, 23, 42, 0.8); color: var(--muted); font-weight: 600; font-size: 0.8rem; }
        td { padding: 1rem; border-top: 1px solid rgba(255,255,255,0.05); }
        tr:hover td { background: rgba(255,255,255,0.02); }

        /* Badges */
        .badge {
            padding: 0.25rem 0.5rem;
            border-radius: 9999px;
            font-size: 0.7rem;
            font-weight: 600;
        }
        .badge-serious { background: rgba(239, 68, 68, 0.2); color: #f87171; }
        .badge-willful { background: rgba(245, 158, 11, 0.2); color: #fbbf24; }
        .badge-other { background: rgba(148, 163, 184, 0.2); color: #cbd5e1; }
        
        .loading { text-align: center; padding: 3rem; color: var(--muted); font-style: italic; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Cal/OSHA Search Dashboard</h1>
        
        <div class="search-grid">
            <div class="input-group">
                <label>Employer</label>
                <input type="text" id="employer" placeholder="e.g. Tesla">
            </div>
            <div class="input-group">
                <label>City</label>
                <input type="text" id="city" placeholder="e.g. Los Angeles">
            </div>
            <div class="input-group">
                <label>Regulation (Title 8)</label>
                <input type="text" id="standard" placeholder="e.g. 3203">
            </div>
            <div class="input-group">
                <label>Year</label>
                <input type="number" id="year" placeholder="2024">
            </div>
            <button onclick="performSearch()">Search</button>
        </div>

        <div class="table-container">
            <table id="resultsTable">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Employer</th>
                        <th>City</th>
                        <th>Activity #</th>
                        <th>Standards Cited</th>
                    </tr>
                </thead>
                <tbody id="resultsBody">
                    <tr><td colspan="5" class="loading">Enter search criteria and click Search</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        async function performSearch() {
            const body = document.getElementById('resultsBody');
            body.innerHTML = '<tr><td colspan="5" class="loading">Loading records...</td></tr>';

            const params = new URLSearchParams({
                employer: document.getElementById('employer').value,
                city: document.getElementById('city').value,
                year: document.getElementById('year').value,
                standard: document.getElementById('standard').value
            });

            try {
                const response = await fetch(`/api/search?${params}`);
                const data = await response.json();

                if (data.error) {
                    body.innerHTML = `<tr><td colspan="5" class="loading" style="color:#f87171">${data.error}</td></tr>`;
                    return;
                }

                if (data.results.length === 0) {
                    body.innerHTML = '<tr><td colspan="5" class="loading">No records found matching criteria.</td></tr>';
                    return;
                }

                body.innerHTML = data.results.map(row => `
                    <tr>
                        <td>${new Date(row.OPEN_DATE).toLocaleDateString()}</td>
                        <td style="font-weight:600">${row.ESTAB_NAME}</td>
                        <td>${row.SITE_CITY}</td>
                        <td style="color:#94a3b8">${row.ACTIVITY_NR}</td>
                        <td>
                            ${(row.standards || '').split(',').map(s => `<span class="badge badge-other">${s}</span>`).join(' ')}
                        </td>
                    </tr>
                `).join('');

            } catch (err) {
                body.innerHTML = `<tr><td colspan="5" class="loading">Error connecting to server.</td></tr>`;
            }
        }
    </script>
</body>
</html>
    """

import threading

def run_ingestion():
    if not os.path.exists(DB_PATH):
        print("Database not found. Starting initial data ingestion in background...")
        try:
            import ingest_data
            ingest_data.ingest()
            print("Background ingestion complete.")
        except Exception as e:
            print(f"Failed to ingest data in background: {e}")

if __name__ == "__main__":
    # Start ingestion in a background thread to avoid blocking server start
    threading.Thread(target=run_ingestion, daemon=True).start()
            
    print("Starting dashboard server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
