import io
import re
import pandas as pd
from google.cloud import storage

storage_client = storage.Client()

def process_csv(event, context):
    bucket_name = event['bucket']
    file_name = event['name']

    print(f"New file detected: gs://{bucket_name}/{file_name}")

    # Only process CSV files
    if not file_name.endswith('.csv'):
        print("Not a CSV file. Skipping.")
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    df = pd.read_csv(io.StringIO(content))

    # Data cleaning and aggregation
    df_open = df[df["isOpen"] == True].copy()
    for col in ['diesel', 'e5', 'e10']:
        df_open[col] = pd.to_numeric(df_open[col], errors='coerce')
    df_open.dropna(subset=['diesel', 'e5', 'e10'], inplace=True)
    df_open["place"] = df_open["place"].str.strip().str.title()
    df_open["brand"] = df_open["brand"].str.strip().str.upper()

    # Average fuel prices per city
    avg_prices = df_open.groupby("place")[['diesel', 'e5', 'e10']].mean()

    # Average lat and lng per city
    avg_coords = df_open.groupby("place")[['lat', 'lng']].mean()

    # Combine average prices with average coordinates
    agg_with_coords = avg_prices.join(avg_coords).round(3)

    # Sort by E5 price ascending
    agg_with_coords = agg_with_coords.sort_values(by='e5')

    cheapest_e5 = df_open.loc[df_open['e5'].idxmin()]
    cheapest_diesel = df_open.loc[df_open['diesel'].idxmin()]
    cheapest_e10 = df_open.loc[df_open['e10'].idxmin()]

    print("Average fuel prices by city:")
    print(agg_with_coords.head(10))

    print("\nCheapest stations:")
    print(f"E5: {cheapest_e5['name']} in {cheapest_e5['place']} at €{cheapest_e5['e5']}")
    print(f"Diesel: {cheapest_diesel['name']} in {cheapest_diesel['place']} at €{cheapest_diesel['diesel']}")
    print(f"E10: {cheapest_e10['name']} in {cheapest_e10['place']} at €{cheapest_e10['e10']}")

    # Save aggregated results with coordinates as CSV to GCS
    timestamp_match = re.search(r"(\d{8}_\d{6})", file_name)
    timestamp_str = timestamp_match.group(1) if timestamp_match else "unknown"

    out_filename = f"aggregated/avg_prices_with_coords_{timestamp_str}.csv"
    out_bucket = storage_client.bucket(bucket_name)
    out_blob = out_bucket.blob(out_filename)

    csv_data = agg_with_coords.to_csv()
    out_blob.upload_from_string(csv_data, content_type='text/csv')

    print(f"Aggregated data with coords saved to gs://{bucket_name}/{out_filename}")
