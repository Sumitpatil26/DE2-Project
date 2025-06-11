import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from google.cloud import storage
import io
import re
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
from google.oauth2 import service_account

# Configuration (same as before)
BUCKET_NAME = "fuel-prices-bucket"
AGGREGATED_FOLDER = "aggregated/"
main_cities = [
    "Heidelberg", "Mannheim", "Stuttgart", "Karlsruhe", "Ulm",
    "Freiburg", "Konstanz", "Reutlingen", "Ravensburg"
]
price_ranges = {
    'e5': {'green': 'Below €1.55 (cheap)', 'orange': '€1.55 to €1.65 (moderate)', 'red': 'Above €1.65 (expensive)'},
    'diesel': {'green': 'Below €1.55 (cheap)', 'orange': '€1.55 to €1.65 (moderate)', 'red': 'Above €1.65 (expensive)'},
    'e10': {'green': 'Below €1.50 (cheap)', 'orange': '€1.50 to €1.60 (moderate)', 'red': 'Above €1.60 (expensive)'},
}

count = st_autorefresh(interval=300000, limit=None, key="datarefresh")

@st.cache_resource
def get_storage_client():
    key_dict = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return storage.Client(credentials=credentials)

@st.cache_data(ttl=300)
def load_latest_aggregated_csv():
    client = get_storage_client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix=AGGREGATED_FOLDER))
    csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
    if not csv_blobs:
        return None
    latest_blob = max(csv_blobs, key=lambda b: b.updated)
    content = latest_blob.download_as_text()
    df = pd.read_csv(io.StringIO(content))
    return df

@st.cache_data(ttl=300)
def load_latest_raw_csv():
    client = get_storage_client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix="fuel_data/"))
    csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
    if not csv_blobs:
        return None
    latest_blob = max(csv_blobs, key=lambda b: b.updated)
    content = latest_blob.download_as_text()
    df_raw = pd.read_csv(io.StringIO(content))
    return df_raw

# Sidebar for page selection
page = st.sidebar.radio("Select Visualization", ["Map", "Line Chart", "Brand Market Share"])

df = load_latest_aggregated_csv()
df_raw = load_latest_raw_csv()

if df is None or df_raw is None:
    st.error("Aggregated or raw data not found!")
else:
    if page == "Map":
        fuel_type = st.sidebar.selectbox("Select fuel type:", options=['e5', 'diesel', 'e10'], index=0)
        fuel_type_name = {'e5': 'E5', 'diesel': 'Diesel', 'e10': 'E10'}[fuel_type]

        st.title(f"Fuel Stations Map - {fuel_type_name}")

        st.markdown(f"""
        **Price Ranges for {fuel_type_name}:**

        - 🟢 {price_ranges[fuel_type]['green']}  
        - 🟠 {price_ranges[fuel_type]['orange']}  
        - 🔴 {price_ranges[fuel_type]['red']}  
        """)

        def price_color(price, fuel_type):
            if fuel_type == 'e5':
                if price < 1.55:
                    return 'green'
                elif price < 1.65:
                    return 'orange'
                else:
                    return 'red'
            elif fuel_type == 'diesel':
                if price < 1.55:
                    return 'green'
                elif price < 1.65:
                    return 'orange'
                else:
                    return 'red'
            elif fuel_type == 'e10':
                if price < 1.50:
                    return 'green'
                elif price < 1.60:
                    return 'orange'
                else:
                    return 'red'
            else:
                return 'gray'

        m = folium.Map(location=[48.7, 9.1], zoom_start=8)

        for _, row in df.iterrows():
            price = row.get(fuel_type)
            if pd.notna(price) and 'lat' in row and 'lng' in row and pd.notna(row['lat']) and pd.notna(row['lng']):
                popup = f"<b>{row['place']}</b><br>{fuel_type_name} Price: €{price:.3f}"
                folium.CircleMarker(
                    location=[row['lat'], row['lng']],
                    radius=7,
                    color=price_color(price, fuel_type),
                    fill=True,
                    fill_opacity=0.7,
                    popup=popup
                ).add_to(m)

        st_folium(m, width=700, height=500)

    elif page == "Line Chart":
        st.title("Average Fuel Prices for Main 9 Cities")
        st.write("""
        This line chart shows average prices of Diesel, E5, and E10 for selected main cities.  
        Colors represent different fuel types.
        """)

        avg_prices_main = df[df['place'].isin(main_cities)][['place', 'diesel', 'e5', 'e10']].copy()

        avg_prices_long = avg_prices_main.melt(id_vars='place', 
                                              value_vars=['diesel', 'e5', 'e10'], 
                                              var_name='Fuel Type', 
                                              value_name='Average Price (€)')

        fig_line = px.line(
            avg_prices_long,
            x='place',
            y='Average Price (€)',
            color='Fuel Type',
            markers=True,
            title='Average Fuel Prices by Fuel Type for Main Cities'
        )
        fig_line.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_line, use_container_width=True)

    elif page == "Brand Market Share":
        st.title("Fuel Station Brand Market Share")
        st.write("""
        This pie chart shows the share of fuel stations by brand in the main cities.
        """)

        brand_counts = df_raw[df_raw['place'].isin(main_cities)]['brand'].value_counts().reset_index()
        brand_counts.columns = ['brand', 'count']

        fig_pie = px.pie(
            brand_counts,
            values='count',
            names='brand',
            title='Fuel Station Distribution by Brand'
        )
        st.plotly_chart(fig_pie, use_container_width=True)
