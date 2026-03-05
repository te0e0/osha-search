import requests
import os
import zipfile
import io
import sys

# We'll dynamically construct the URL using the repository environment variable on Render,
# but for now, we'll configure it to match the 'latest-db' tag created by our GitHub Action.
# Make sure to replace yourusername/yourosha repo below!
GITHUB_RELEASE_ZIP_URL = "https://github.com/te0e0/osha-search/releases/download/latest-db/osha_ca.zip"
DB_FILE = "osha_ca.db"

def download_database():
    if os.path.exists(DB_FILE):
        return True
        
    print(f"Downloading pre-compiled database from {GITHUB_RELEASE_ZIP_URL}...")
        
    try:
        response = requests.get(GITHUB_RELEASE_ZIP_URL, stream=True)
        response.raise_for_status()
        
        # It's highly recommended to zip the DB before uploading to GitHub 
        # (130MB unzipped -> ~35MB zipped)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall()
            
        print("Database downloaded and extracted successfully!")
        return True
    except Exception as e:
        print(f"Failed to download database from GitHub: {e}")
        return False

if __name__ == "__main__":
    download_database()
