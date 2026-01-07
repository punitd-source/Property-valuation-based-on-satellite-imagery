import os
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm
import time

# Load environment variables
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

def download_excel_files(url, filename):
    if os.path.exists(filename):
        print(f"{filename} already exists. Skipping download.")
        return
    
    print(f"Downloading {filename} from {url}...")
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            print(f"Successfully downloaded {filename}")
        else:
            print(f"Failed to download {filename}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")

def fetch_satellite_image(lat, long, output_path):
    """
    Fetches a satellite image from Google Maps Static API.
    """
    base_url = "https://maps.googleapis.com/maps/api/staticmap"
    params = {
        "center": f"{lat},{long}",
        "zoom": 19,        # High zoom to see the property details
        "size": "600x600", # 640x640 is max for free tier (standard), 600x600 is safe
        "maptype": "satellite",
        "key": API_KEY
    }
    
    try:
        response = requests.get(base_url, params=params, stream=True)
        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return True
        else:
            print(f"Error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"Exception fetching image: {e}")
        return False

def download_images_from_df(df, output_dir):
    """
    Iterates through a dataframe and downloads images to output_dir.
    """
    # Verify columns
    required_cols = ['lat', 'long'] 
    for col in required_cols:
        if col not in df.columns:
            # Try to be smart about column names
            found = False
            for c in df.columns:
                if c.strip().lower() == col:
                    df.rename(columns={c: col}, inplace=True)
                    found = True
                    break
            if not found:
                print(f"Error: Column '{col}' not found. Available: {df.columns.tolist()}")
                return

    os.makedirs(output_dir, exist_ok=True)
    print(f"Target Folder: {output_dir} | Records: {len(df)}")
    
    success_count = 0
    skip_count = 0
    fail_count = 0

    for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Downloading to {output_dir}"):
        # Use 'id' if available, otherwise index
        img_id = str(int(row['id'])) if 'id' in df.columns else f"idx_{idx}"
        lat = row['lat']
        long = row['long']
        
        output_path = os.path.join(output_dir, f"{img_id}.jpg")
        
        if os.path.exists(output_path):
            skip_count += 1
            continue
            
        if pd.isna(lat) or pd.isna(long):
            fail_count += 1
            continue
            
        if fetch_satellite_image(lat, long, output_path):
            success_count += 1
        else:
            fail_count += 1
            
        time.sleep(0.1)
        
    print(f"Done. Saved: {success_count} | Skipped: {skip_count} | Failed: {fail_count}\n")

def main():
    if not API_KEY:
        print("CRITICAL ERROR: GOOGLE_MAPS_API_KEY not found in .env file.")
        return

    # 1. Download Excel Files
    print("--- Step 1: Checking Data Files ---")
    train_url = "https://1drv.ms/x/c/8cf6803adf7941c3/IQBue1q4w4TETL_7xWMGhcD_AejALtdsXTBejVUjRA9qeM8?download=1"
    test_url = "https://1drv.ms/x/c/8cf6803adf7941c3/IQAwCVfSggmjQ4DJH51zJK-tARwRQWE9fl0bPlwo1mRF2PQ?download=1"
    
    download_excel_files(train_url, "train.xlsx")
    download_excel_files(test_url, "test.xlsx")

    # 2. Process TRAIN Data
    print("\n--- Step 2: Processing Train Images ---")
    if os.path.exists("train.xlsx"):
        try:
            df_train = pd.read_excel("train.xlsx")
            download_images_from_df(df_train, "satellite_images")
        except Exception as e:
            print(f"Error processing train.xlsx: {e}")
    else:
        print("train.xlsx not found.")

    # 3. Process TEST Data
    print("\n--- Step 3: Processing Test Images ---")
    if os.path.exists("test.xlsx"):
        try:
            df_test = pd.read_excel("test.xlsx")
            # User requested separate folder for test images
            download_images_from_df(df_test, "test_images")
        except Exception as e:
            print(f"Error processing test.xlsx: {e}")
    else:
        print("test.xlsx not found.")

if __name__ == "__main__":
    main()
