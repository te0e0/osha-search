import requests
import os
import zipfile
import io
import sys
import shutil

# We'll dynamically construct the URL using the repository environment variable on Render,
# but for now, we'll configure it to match the 'latest-db' tag created by our GitHub Action.
# Make sure to replace yourusername below!
GITHUB_RELEASE_ZIP_URL = "https://github.com/xgcoh/osha-search/releases/download/latest-db/osha_ca.zip"
DB_FILE_NAME = "osha_ca.db"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE_PATH = os.path.join(SCRIPT_DIR, DB_FILE_NAME)

def download_database(force=False, progress_callback=None, status_callback=None, swap_lock=None):
    if not force and os.path.exists(DB_FILE_PATH):
        return True
        
    if status_callback:
        status_callback("Downloading")
        
    print(f"Downloading pre-compiled Cal/OSHA database from {GITHUB_RELEASE_ZIP_URL}...")
        
    try:
        response = requests.get(GITHUB_RELEASE_ZIP_URL, stream=True)
        response.raise_for_status()
        
        # Stream the download to a temporary file
        temp_zip = "temp_db_ca.zip"
        downloaded = 0
        with open(temp_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback:
                        progress_callback(downloaded)
                    elif (downloaded // (1024*1024)) % 10 == 0:
                        print(f"Downloaded {downloaded // (1024*1024)} MB...")
        
        print(f"Download complete: {downloaded // (1024*1024)} MB")
        
        if status_callback:
            status_callback("Extracting & Indexing")
            
        temp_dir = "temp_db_extract_ca"
        os.makedirs(temp_dir, exist_ok=True)
        
        device_ready = False
        try:
            with zipfile.ZipFile(temp_zip) as z:
                z.extractall(temp_dir)
            device_ready = True
        finally:
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
            
        if not device_ready:
            return False
            
        if status_callback:
            status_callback("Finalizing")

        extracted_db = os.path.join(temp_dir, DB_FILE_NAME)
        if os.path.exists(extracted_db):
            # Atomic swap
            if swap_lock:
                with swap_lock:
                    os.replace(extracted_db, DB_FILE_PATH)
                    # Clean up WAL/SHM
                    for ext in ['-wal', '-shm']:
                        wal_file = DB_FILE_PATH + ext
                        if os.path.exists(wal_file):
                            try: os.remove(wal_file)
                            except: pass
            else:
                os.replace(extracted_db, DB_FILE_PATH)
                for ext in ['-wal', '-shm']:
                    wal_file = DB_FILE_PATH + ext
                    if os.path.exists(wal_file):
                        try: os.remove(wal_file)
                        except: pass
            
            shutil.rmtree(temp_dir, ignore_errors=True)
            print("Database downloaded and extracted successfully!")
            return True
        else:
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"Error: {DB_FILE_NAME} not found in archive.")
            return False
            
    except Exception as e:
        print(f"Download failed: {e}")
        return False

if __name__ == "__main__":
    download_database(force=True)
