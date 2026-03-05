from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
import sqlite3
import pandas as pd
from typing import Optional
import uvicorn
import os

app = FastAPI(title="Cal/OSHA Search Dashboard")

DB_PATH = "osha_ca.db"

# Global status for ingestion tracking
ingestion_status = {"status": "starting", "progress": 0}

@app.get("/api/status")
def get_status():
    return ingestion_status

def get_db_connection():
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20) # High timeout for concurrent writes
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn
    except:
        return None

@app.get("/api/search")
def search_inspections(
    employer: Optional[str] = None,
    city: Optional[str] = None,
    address: Optional[str] = None,
    start_year: Optional[str] = None,
    end_year: Optional[str] = None,
    inv_status: Optional[str] = None,
    has_viol: Optional[str] = None,
    classification: Optional[str] = None,
    insp_type: Optional[str] = None,
    union_status: Optional[str] = None,
    region: Optional[str] = None,
    standard: Optional[str] = None,
    sic: Optional[str] = None,
    naics: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    conn = get_db_connection()
    if not conn:
        return {"error": "Index still building... give it 5 minutes.", "results": []}

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
        
    if address:
        query += " AND i.SITE_ADDRESS LIKE ?"
        params.append(f"%{address}%")
    
    if start_year and str(start_year).strip():
        query += " AND i.OPEN_DATE >= ?"
        params.append(f"{start_year}-01-01")
        
    if end_year and str(end_year).strip():
        query += " AND i.OPEN_DATE <= ?"
        params.append(f"{end_year}-12-31")
        
    if inv_status == 'Pending':
        query += " AND (i.CLOSE_CASE_DATE IS NULL OR i.CLOSE_CASE_DATE = '')"
    elif inv_status == 'Completed':
        query += " AND i.CLOSE_CASE_DATE IS NOT NULL AND i.CLOSE_CASE_DATE != ''"
        
    if has_viol == 'Yes':
        query += " AND EXISTS (SELECT 1 FROM violations v2 WHERE v2.ACTIVITY_NR = i.ACTIVITY_NR)"
    elif has_viol == 'No':
        query += " AND NOT EXISTS (SELECT 1 FROM violations v2 WHERE v2.ACTIVITY_NR = i.ACTIVITY_NR)"
        
    if insp_type:
        query += " AND i.INSP_TYPE = ?"
        params.append(insp_type)
        
    if union_status:
        query += " AND i.UNION_STATUS = ?"
        params.append(union_status)
        
    if classification:
        query += " AND v.VIOL_TYPE = ?"
        params.append(classification)
        
    if region:
        # Map regions to CA zip code prefixes
        region_map = {
            "1": ["94%"], # Bay Area
            "2": ["95%", "96%"], # Northern CA
            "3": ["92%"], # San Diego/Orange/Inland Empire
            "4": ["90%", "91%"], # Los Angeles/Ventura
            "8": ["93%"]  # Central Valley
        }
        prefixes = region_map.get(region)
        if prefixes:
            conditions = " OR ".join(["i.SITE_ZIP LIKE ?" for _ in prefixes])
            query += f" AND ({conditions})"
            params.extend(prefixes)
        
    if standard:
        query += " AND v.STANDARD LIKE ?"
        params.append(f"%{standard}%")
        
    if sic:
        query += " AND i.SIC_CODE = ?"
        params.append(sic.split(" - ")[0].strip())

    if naics:
        query += " AND i.NAICS_CODE = ?"
        params.append(naics.split(" - ")[0].strip())

    query += " GROUP BY i.ACTIVITY_NR ORDER BY i.OPEN_DATE DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        import numpy as np
        df = pd.read_sql_query(query, conn, params=params)
        # JSON cannot handle NaN or Infinity results from numeric columns
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return {"results": df.to_dict(orient="records")}
    except Exception as e:
        if "no such table" in str(e).lower():
            return {"error": "Data is still indexing... check back in 1 minute.", "results": []}
        return {"error": f"Search failed: {str(e)}", "results": []}
    finally:
        conn.close()

def sanitize_row(row):
    """Convert SQLite row to dict and replace NaN/Inf with None for JSON compliance"""
    import math
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            d[k] = None
    return d

@app.get("/api/inspection/{activity_nr}")
def get_inspection_detail(activity_nr: str):
    conn = get_db_connection()
    if not conn:
        return {"error": "Database not ready."}
    
    try:
        insp = conn.execute("""
            SELECT i.*, sc.title as SIC_TITLE, nc.title as NAICS_TITLE 
            FROM inspections i 
            LEFT JOIN sic_codes sc ON CAST(i.SIC_CODE AS INTEGER) = CAST(sc.code AS INTEGER)
            LEFT JOIN naics_codes nc ON CAST(i.NAICS_CODE AS INTEGER) = CAST(nc.code AS INTEGER)
            WHERE i.ACTIVITY_NR = ?
        """, (activity_nr,)).fetchone()
        
        if not insp:
            return {"error": "Inspection not found."}
            
        viols_raw = conn.execute("SELECT * FROM violations WHERE ACTIVITY_NR = ?", (activity_nr,)).fetchall()
        
        return {
            "inspection": sanitize_row(insp),
            "violations": [sanitize_row(v) for v in viols_raw]
        }
    finally:
        conn.close()

@app.get("/api/autocomplete")
def autocomplete(field: str, q: str):
    if not q or len(q) < 2:
        return {"options": []}
    
    conn = get_db_connection()
    if not conn:
        return {"options": []}
        
    try:
        cur = conn.cursor()
        
        if field == "sic":
            cur.execute("SELECT code || ' - ' || title FROM sic_codes WHERE code LIKE ? OR title LIKE ? ORDER BY code LIMIT 15", (f"%{q}%", f"%{q}%"))
            return {"options": [row[0] for row in cur.fetchall()]}
            
        if field == "naics":
            cur.execute("SELECT code || ' - ' || title FROM naics_codes WHERE code LIKE ? OR title LIKE ? ORDER BY code LIMIT 15", (f"%{q}%", f"%{q}%"))
            return {"options": [row[0] for row in cur.fetchall()]}

        valid_fields = {
            "employer": ("inspections", "ESTAB_NAME"),
            "city": ("inspections", "SITE_CITY"),
            "address": ("inspections", "SITE_ADDRESS"),
            "standard": ("violations", "STANDARD")
        }
        
        if field not in valid_fields:
            return {"options": []}
            
        table, col = valid_fields[field]
        cur.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {col} LIKE ? ORDER BY {col} LIMIT 15", (f"%{q}%" if field != "standard" else f"{q}%",))
        results = [row[col] for row in cur.fetchall() if row[col]]
        return {"options": results}
    except Exception as e:
        return {"options": []}
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

        /* Modal Styles */
        #modal-overlay {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(15, 23, 42, 0.9); backdrop-filter: blur(8px);
            display: none; justify-content: center; align-items: center; z-index: 1000;
            padding: 2rem;
        }
        .modal-content {
            background: var(--bg); border: 1px solid rgba(255,255,255,0.1);
            width: 100%; max-width: 900px; max-height: 90vh; overflow-y: auto;
            border-radius: 1.5rem; padding: 2.5rem; position: relative;
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 1);
        }
        .close-btn {
            position: absolute; top: 1.5rem; right: 1.5rem;
            background: none; border: none; color: var(--muted);
            font-size: 1.5rem; cursor: pointer; border-radius: 50%;
            width: 2.5rem; height: 2.5rem; display: flex; align-items: center; justify-content: center;
            transition: all 0.2s;
        }
        .close-btn:hover { background: rgba(255,255,255,0.1); color: #fff; }
        
        .detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 2rem; margin-top: 2rem; }
        .detail-section h3 { font-size: 0.7rem; text-transform: uppercase; color: var(--primary); margin-bottom: 0.5rem; }
        .detail-item { margin-bottom: 1rem; }
        .detail-label { font-size: 0.7rem; color: var(--muted); margin-bottom: 0.2rem; }
        .detail-value { font-size: 0.9rem; font-weight: 500; }
        
        .violation-card {
            background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.05);
            border-radius: 0.75rem; padding: 1.25rem; margin-bottom: 1rem;
        }
        tr.clickable { cursor: pointer; }
        tr.clickable:hover td { background: rgba(37, 99, 235, 0.1); }
    </style>
</head>
<body>
    <div class="container">
        <div id="status-bar" style="text-align: right; font-size: 0.7rem; color: var(--muted); margin-bottom: 0.5rem; display: none;">
            Indexing in progress... (takes ~5 mins)
        </div>
        <h1>Cal/OSHA Search Dashboard</h1>
        
        <div class="search-grid">
            <div class="input-group">
                <label>Employer</label>
                <input type="text" id="employer" list="employer-list" placeholder="e.g. Tesla">
                <datalist id="employer-list"></datalist>
            </div>
            <div class="input-group">
                <label>City</label>
                <input type="text" id="city" list="city-list" placeholder="e.g. Los Angeles">
                <datalist id="city-list"></datalist>
            </div>
            <div class="input-group">
                <label>Site Address</label>
                <input type="text" id="address" list="address-list" placeholder="e.g. 123 Main St">
                <datalist id="address-list"></datalist>
            </div>
            <div class="input-group">
                <label>Regulation (Title 8)</label>
                <input type="text" id="standard" list="standard-list" placeholder="e.g. 3203">
                <datalist id="standard-list"></datalist>
            </div>
            <div class="input-group">
                <label>Industry (SIC)</label>
                <input type="text" id="sic" list="sic-list" placeholder="e.g. General Contractors">
                <datalist id="sic-list"></datalist>
            </div>
            <div class="input-group">
                <label>Industry (NAICS)</label>
                <input type="text" id="naics" list="naics-list" placeholder="e.g. Automobile Manufacturing">
                <datalist id="naics-list"></datalist>
            </div>
            <div class="input-group">
                <label>Year (Range)</label>
                <div style="display:flex; gap:0.5rem">
                    <input type="number" id="start_year" style="width: 100%; min-width: 0;" placeholder="From">
                    <input type="number" id="end_year" style="width: 100%; min-width: 0;" placeholder="To">
                </div>
            </div>
            <div class="input-group">
                <label>Status</label>
                <select id="inv_status">
                    <option value="">Any</option>
                    <option value="Pending">Pending</option>
                    <option value="Completed">Completed</option>
                </select>
            </div>
            <div class="input-group">
                <label>Violations</label>
                <select id="has_viol">
                    <option value="">Any</option>
                    <option value="Yes">Violation Found</option>
                    <option value="No">No Violation</option>
                </select>
            </div>
            <div class="input-group">
                <label>Inspection Type</label>
                <select id="insp_type">
                    <option value="">Any</option>
                    <option value="A">Accident (A)</option>
                    <option value="B">Complaint (B)</option>
                    <option value="C">Referral (C)</option>
                    <option value="D">Monitoring (D)</option>
                    <option value="E">Variance (E)</option>
                    <option value="F">FollowUp (F)</option>
                    <option value="G">Unprogrammed Related (G)</option>
                    <option value="H">Programmed Planned (H)</option>
                    <option value="I">Programmed Related (I)</option>
                    <option value="J">Unprogrammed Other (J)</option>
                    <option value="K">Programmed Other (K)</option>
                </select>
            </div>
            <div class="input-group">
                <label>Unionized</label>
                <select id="union_status">
                    <option value="">Any</option>
                    <option value="A">Yes</option>
                    <option value="B">No</option>
                </select>
            </div>
            <div class="input-group">
                <label>Regional Office</label>
                <select id="region">
                    <option value="">Any</option>
                    <option value="1">Region 1 (SF Bay Area)</option>
                    <option value="2">Region 2 (Northern CA)</option>
                    <option value="3">Region 3 (San Diego / Orange / IE)</option>
                    <option value="4">Region 4 (Los Angeles / Ventura)</option>
                    <option value="8">Region 8 (Central Valley)</option>
                </select>
            </div>
            <div class="input-group">
                <label>Classification</label>
                <select id="classification">
                    <option value="">Any</option>
                    <option value="S">Serious</option>
                    <option value="O">Other / General / Regulatory</option>
                    <option value="W">Willful</option>
                    <option value="R">Repeat</option>
                    <option value="U">Unclassified</option>
                </select>
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
                        <th>Standards Cited</th>
                    </tr>
                </thead>
                <tbody id="resultsBody">
                    <tr><td colspan="4" class="loading">Enter search criteria and click Search</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <div id="modal-overlay" onclick="if(event.target === this) closeModal()">
        <div class="modal-content">
            <button class="close-btn" onclick="closeModal()">×</button>
            <div id="modal-body">
                <!-- Content injected via JS -->
            </div>
        </div>
    </div>
    <script>
        function setupAutocomplete(inputId) {
            let timeout = null;
            document.getElementById(inputId).addEventListener('input', function(e) {
                clearTimeout(timeout);
                const query = e.target.value;
                
                if (query.length < 2) return;
                
                timeout = setTimeout(async () => {
                    try {
                        const res = await fetch(`/api/autocomplete?field=${inputId}&q=${encodeURIComponent(query)}`);
                        const data = await res.json();
                        
                        const datalist = document.getElementById(`${inputId}-list`);
                        datalist.innerHTML = data.options.map(o => `<option value="${o}">`).join('');
                    } catch (err) {}
                }, 300);
            });
        }
        
        ['employer', 'city', 'address', 'standard', 'sic', 'naics'].forEach(setupAutocomplete);

        async function checkStatus() {
            const bar = document.getElementById('status-bar');
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                if (data.status === 'indexing') {
                    bar.style.display = 'block';
                } else {
                    bar.style.display = 'none';
                }
            } catch (e) {}
        }
        setInterval(checkStatus, 10000);
        checkStatus();

        async function performSearch() {
            const body = document.getElementById('resultsBody');
            body.innerHTML = '<tr><td colspan="5" class="loading">Loading records...</td></tr>';

            const params = new URLSearchParams();
            const startYearVal = document.getElementById('start_year').value;
            const endYearVal = document.getElementById('end_year').value;
            const empVal = document.getElementById('employer').value;
            const cityVal = document.getElementById('city').value;
            const addrVal = document.getElementById('address').value;
            const stdVal = document.getElementById('standard').value;
            const inspTypeVal = document.getElementById('insp_type').value;
            const unionVal = document.getElementById('union_status').value;
            const classVal = document.getElementById('classification').value;
            const regionVal = document.getElementById('region').value;
            const sicVal = document.getElementById('sic').value;
            const naicsVal = document.getElementById('naics').value;
            const invStatusVal = document.getElementById('inv_status').value;
            const hasViolVal = document.getElementById('has_viol').value;

            if (empVal) params.append('employer', empVal);
            if (cityVal) params.append('city', cityVal);
            if (addrVal) params.append('address', addrVal);
            if (startYearVal) params.append('start_year', startYearVal);
            if (endYearVal) params.append('end_year', endYearVal);
            if (stdVal) params.append('standard', stdVal);
            if (inspTypeVal) params.append('insp_type', inspTypeVal);
            if (unionVal) params.append('union_status', unionVal);
            if (classVal) params.append('classification', classVal);
            if (regionVal) params.append('region', regionVal);
            if (sicVal) params.append('sic', sicVal);
            if (naicsVal) params.append('naics', naicsVal);
            if (invStatusVal) params.append('inv_status', invStatusVal);
            if (hasViolVal) params.append('has_viol', hasViolVal);

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
                    <tr class="clickable" onclick="showDetails('${row.ACTIVITY_NR}')">
                        <td>${new Date(row.OPEN_DATE).toLocaleDateString()}</td>
                        <td style="font-weight:600">${row.ESTAB_NAME}</td>
                        <td>${row.SITE_CITY}</td>
                        <td>
                            ${(row.standards || '').split(',').map(s => `<span class="badge badge-other">${s}</span>`).join(' ')}
                        </td>
                    </tr>
                `).join('');

            } catch (err) {
                body.innerHTML = `<tr><td colspan="5" class="loading">Error connecting to server. Server may be indexing data... please wait 1 minute and try again.</td></tr>`;
            }
        }

        async function showDetails(id) {
            const overlay = document.getElementById('modal-overlay');
            const content = document.getElementById('modal-body');
            overlay.style.display = 'flex';
            content.innerHTML = '<div class="loading">Loading details...</div>';

            try {
                const res = await fetch(`/api/inspection/${id}`);
                const data = await res.json();
                
                if (data.error) {
                    content.innerHTML = `<div class="loading" style="color:#f87171">${data.error}</div>`;
                    return;
                }

                const i = data.inspection;
                content.innerHTML = `
                    <h2 style="margin-top:0">${i.ESTAB_NAME}</h2>
                    <p style="color:var(--muted); font-size:0.9rem">${i.SITE_ADDRESS}, ${i.SITE_CITY}, ${i.SITE_STATE} ${i.SITE_ZIP || ''}</p>
                    
                    <div class="detail-grid">
                        <div class="detail-section">
                            <h3>Inspection Details</h3>
                            <div class="detail-item"><div class="detail-label">Type</div><div class="detail-value">
                                ${i.INSP_TYPE === 'A' ? 'Programmed Planned' : 
                                  i.INSP_TYPE === 'B' ? 'Programmed Related' : 
                                  i.INSP_TYPE === 'C' ? 'Unprogrammed Related' : 
                                  i.INSP_TYPE === 'D' ? 'Unprogrammed Other' : 
                                  i.INSP_TYPE === 'E' ? 'Fatality/Catastrophe' : 
                                  i.INSP_TYPE === 'F' ? 'Complaints' : 
                                  i.INSP_TYPE === 'G' ? 'Referrals' : 
                                  i.INSP_TYPE === 'H' ? 'FollowUp' : 
                                  i.INSP_TYPE === 'I' ? 'Unprogrammed Related (FollowUp)' : 
                                  i.INSP_TYPE === 'J' ? 'Unprogrammed Other (FollowUp)' : 
                                  i.INSP_TYPE || 'N/A'}
                            </div></div>
                            <div class="detail-item"><div class="detail-label">Union Status</div><div class="detail-value">${i.UNION_STATUS === 'A' ? 'Yes' : i.UNION_STATUS === 'B' ? 'No' : i.UNION_STATUS || 'N/A'}</div></div>
                        </div>
                        <div class="detail-section">
                            <h3>Dates</h3>
                            <div class="detail-item"><div class="detail-label">Opened</div><div class="detail-value">${new Date(i.OPEN_DATE).toLocaleDateString()}</div></div>
                            <div class="detail-item"><div class="detail-label">Case Closed</div><div class="detail-value">${i.CLOSE_CASE_DATE ? new Date(i.CLOSE_CASE_DATE).toLocaleDateString() : 'N/A'}</div></div>
                        </div>
                        <div class="detail-section">
                            <h3>Industry</h3>
                            <div class="detail-item"><div class="detail-label">SIC</div><div class="detail-value">${i.SIC_TITLE ? i.SIC_TITLE : i.SIC_CODE || 'N/A'}</div></div>
                            <div class="detail-item"><div class="detail-label">NAICS</div><div class="detail-value">${i.NAICS_TITLE ? i.NAICS_TITLE : i.NAICS_CODE || 'N/A'}</div></div>
                        </div>
                    </div>

                    <h3 style="margin-top:2.5rem; text-transform:uppercase; font-size:0.7rem; color:var(--primary)">Violations (${data.violations.length})</h3>
                    <div id="violation-list">
                        ${data.violations.map(v => `
                            <div class="violation-card">
                                <div style="display:flex; justify-content:space-between; margin-bottom:0.8rem">
                                    <span style="font-weight:600; color:var(--primary)">${v.STANDARD}</span>
                                    <span class="badge ${v.VIOL_TYPE === 'S' ? 'badge-serious' : v.VIOL_TYPE === 'W' ? 'badge-willful' : 'badge-other'}">
                                        ${v.VIOL_TYPE === 'S' ? 'Serious' : v.VIOL_TYPE === 'O' ? 'Other/General' : v.VIOL_TYPE === 'W' ? 'Willful' : v.VIOL_TYPE === 'R' ? 'Repeat' : v.VIOL_TYPE === 'U' ? 'Unclassified' : v.VIOL_TYPE}
                                    </span>
                                </div>
                                <div style="display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem; font-size:0.8rem">
                                    <div style="grid-column: span 2;"><span style="color:var(--muted)">Severity Classification:</span> ${v.VIOL_TYPE === 'S' ? 'Serious' : v.VIOL_TYPE === 'O' ? 'Other' : v.VIOL_TYPE === 'W' ? 'Willful' : v.VIOL_TYPE === 'R' ? 'Repeat' : v.VIOL_TYPE === 'U' ? 'Unclassified' : v.VIOL_TYPE}</div>
                                    <div><span style="color:var(--muted)">Instances:</span> ${v.NR_INSTANCES || 0}</div>
                                    <div><span style="color:var(--muted)">Abate Date:</span> ${v.ABATE_DATE ? new Date(v.ABATE_DATE).toLocaleDateString() : 'N/A'}</div>
                                    <div style="grid-column: span 2; margin-top: 0.5rem;"><span style="color:var(--muted)">Initial Fine Assessed:</span> ${parseFloat(v.INITIAL_PENALTY || 0).toLocaleString('en-US', {style:'currency', currency:'USD'})}</div>
                                    <div style="grid-column: span 2;"><span style="color:var(--muted)">Current Fine (Post-Appeals/Settlements):</span> ${parseFloat(v.CURRENT_PENALTY || 0).toLocaleString('en-US', {style:'currency', currency:'USD'})}</div>
                                </div>
                            </div>
                        `).join('') || '<p style="color:var(--muted); font-size:0.9rem">No individual violations recorded.</p>'}
                    </div>
                `;
            } catch (err) {
                content.innerHTML = `<div class="loading" style="color:#f87171">Error fetching details.</div>`;
            }
        }

        function closeModal() {
            document.getElementById('modal-overlay').style.display = 'none';
        }
    </script>
</body>
</html>
    """

import threading

def run_ingestion():
    global ingestion_status
    if not os.path.exists(DB_PATH):
        ingestion_status["status"] = "indexing"
        print("Database not found. Attempting to download pre-compiled DB from GitHub...")
        try:
            import download_db
            success = download_db.download_database()
            if success:
                ingestion_status["status"] = "complete"
                print("Successfully downloaded DB from GitHub.")
                return
        except Exception as e:
            print(f"Failed to download DB: {e}")
            
        print("Falling back to parsing raw CSV files. This may take 5+ minutes...")
        try:
            import ingest_data
            ingest_data.ingest()
            ingestion_status["status"] = "complete"
            print("Background ingestion complete.")
        except Exception as e:
            ingestion_status["status"] = "failed"
            ingestion_status["error"] = str(e)
            print(f"Failed to ingest data in background: {e}")
    else:
        ingestion_status["status"] = "complete"

if __name__ == "__main__":
    # Start ingestion in a background thread
    threading.Thread(target=run_ingestion, daemon=True).start()
            
    print("Starting dashboard server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
