# Deployment Guide: OSHA Search Dashboard

Follow these steps to publish your dashboard to the web.

## Prerequisites
1.  A **GitHub** account.
2.  A **Render.com** or **Railway.app** account.

## Step 1: Create a GitHub Repository
1.  Go to GitHub and create a new **Private** repository named `osha-search`.
2.  Upload the following files from `C:\Users\xgcoh\.gemini\antigravity\scratch\osha-search`:
    *   `main.py`
    *   `ingest_data.py`
    *   `Dockerfile`
    *   `requirements.txt`
    *(Do NOT upload `osha_ca.db` or the `data/` folder; the server will generate these automatically).*

## Step 2: Deploy to Render (Recommended)
1.  Log in to [Render](https://render.com).
2.  Click **New +** > **Web Service**.
3.  Connect your `osha-search` GitHub repository.
4.  **Configuration**:
    *   **Name**: `osha-search`
    *   **Runtime**: `Docker`
    *   **Instance Type**: `Starter` (Recommended for the 500MB database).
5.  Click **Deploy Web Service**.

## Step 3: First Launch
*   **Health Checks**: The server will start immediately and pass Render's health checks.
*   **Background Ingestion**: On the first launch, the app will download and index the 500MB of data in the background. 
*   **Wait for Data**: If you search immediately after deployment, you may see "Database not ready". Please wait about **5-8 minutes** for the first-time ingestion to complete. 
*   Check the **Render Logs** to see the progress: you'll see "Background ingestion complete" when it's ready.

---
> [!NOTE]
> **Persistent Storage**: If you use the Free tier on Render, your database will be reset every 48 hours. For permanent history, upgrading to a "Starter" instance with a "Disk" (volume) attached is recommended.
