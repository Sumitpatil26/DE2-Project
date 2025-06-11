import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime
import time

API_KEY = 'ded8c784-de69-728b-f437-b15d157ebb4b'
GCS_BUCKET_NAME = 'fuel-prices-bucket'
RADIUS = 25
WAIT_SEC = 3

LOCATIONS = {
    "Heidelberg": (49.3988, 8.6724),
    "Mannheim": (49.4875, 8.4660),
    "Stuttgart": (48.7758, 9.1829),
    "Karlsruhe": (49.0069, 8.4037),
    "Ulm": (48.4011, 9.9876),
    "Freiburg": (47.9990, 7.8421),
    "Konstanz": (47.6597, 9.1758),
    "Reutlingen": (48.4914, 9.2043),
    "Ravensburg": (47.7816, 9.6106)
}

def fetch_fuel_data(lat, lng):
    url = (
        f"https://creativecommons.tankerkoenig.de/json/list.php?"
        f"lat={lat}&lng={lng}&rad={RADIUS}&sort=dist&type=all&apikey={API_KEY}"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data.get("stations", []))
    except Exception as e:
        print(f"❌ Error at ({lat},{lng}): {e}")
        return pd.DataFrame()

def upload_to_gcs(filename, bucket_name, destination_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(filename)
    print(f"✅ Uploaded to gs://{bucket_name}/{destination_path}")

def run_ingestion():
    all_data = []
    print(f"📍 Fetching data from {len(LOCATIONS)} cities...\n")

    for city, (lat, lng) in LOCATIONS.items():
        print(f"Fetching from {city}...")
        df = fetch_fuel_data(lat, lng)
        if not df.empty:
            all_data.append(df)
        time.sleep(WAIT_SEC)

    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        df_all.drop_duplicates(subset='id', inplace=True)
        df_all = df_all[[ 'id', 'name', 'brand', 'street', 'place', 'postCode',
                          'houseNumber', 'lat', 'lng', 'dist', 'diesel', 'e5', 'e10', 'isOpen' ]]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"/tmp/fuel_data_{timestamp}.csv"
        df_all.to_csv(filename, index=False)
        upload_to_gcs(filename, GCS_BUCKET_NAME, f"fuel_data/{filename.split('/')[-1]}")
        print(f"✅ Uploaded {len(df_all)} records.")
    else:
        print("⚠️ No data retrieved.")

# Cloud Function entry point
def ingest_fuel_data(request):
    run_ingestion()
    return "OK", 200
