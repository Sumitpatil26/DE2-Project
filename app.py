import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from google.cloud import storage
import io
import re
from streamlit_autorefresh import st_autorefresh

# Configuration
BUCKET_NAME = "fuel-prices-bucket"
AGGREGATED_FOLDER = "aggregated/"

# Auto-refresh page every 5 minutes (300,000 milliseconds)
count = st_autorefresh(interval=300000, limit=None, key="datarefresh")

@st.cache_resource
def get_storage_client():
    return storage.Client()

@st.cache_data(ttl=300)  # Cache data for 5 minutes to sync with auto-refresh
def load_latest_aggregated_csv():
    client = get_storage_client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix=AGGREGATED_FOLDER))
    csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
    if not csv_blobs:
        return None, None

    latest_blob = sorted(csv_blobs, key=lambda b: b.name)[-1]
    content = latest_blob.download_as_text()
    df = pd.read_csv(io.StringIO(content))

    timestamp = re.search(r"(\d{8}_\d{6})", latest_blob.name)
    ts = timestamp.group(1) if timestamp else "Unknown"

    return df, ts

st.title("Fuel Prices Map - Baden-Württemberg")

df, timestamp = load_latest_aggregated_csv()

if df is None:
    st.error("No aggregated data found!")
else:
    st.write(f"### Data timestamp: {timestamp}")

    # Sample map centered around Baden-Württemberg
    m = folium.Map(location=[48.7, 9.1], zoom_start=8)

    def price_color(price):
        if price < 1.55:
            return 'green'
        elif price < 1.65:
            return 'orange'
        else:
            return 'red'

    for _, row in df.iterrows():
        if 'lat' in row and 'lng' in row and pd.notna(row['lat']) and pd.notna(row['lng']):
            popup = f"<b>{row['place']}</b><br>E5: €{row['e5']}<br>Diesel: €{row['diesel']}<br>E10: €{row['e10']}"
            folium.CircleMarker(
                location=[row['lat'], row['lng']],
                radius=6,
                color=price_color(row['e5']),
                fill=True,
                fill_opacity=0.7,
                popup=popup
            ).add_to(m)

    st_folium(m, width=700, height=500)
