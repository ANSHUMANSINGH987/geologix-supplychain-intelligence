"""
GeoLogix: Real-Time Vessel Tracking Dashboard (Streamlit + PyDeck)
Subscribes to Kafka topic and renders live WebGL map.
"""
import time
import json
import logging
from datetime import datetime
import pandas as pd
import streamlit as st
import pydeck as pdk
from confluent_kafka import Consumer, KafkaError

# Configure page layout
st.set_page_config(page_title="GeoLogix Live Tracker", page_icon="🚢", layout="wide")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Streamlit_App")

# Kafka Settings
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "vessel_stream"
GROUP_ID = "streamlit_dashboard_group"

# State management for vessel positions
if "vessels" not in st.session_state:
    st.session_state.vessels = {}

# Initialize Kafka Consumer (only once per session)
@st.cache_resource
def get_kafka_consumer():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])
    return consumer

consumer = get_kafka_consumer()

# UI Layout
st.title("🌍 GeoLogix: Real-Time Maritime Supply Chain Tracker")
st.markdown("Live Sub-Second Telemetry Streaming via Apache Kafka & Deck.gl")

# Container for metrics and map
metrics_col = st.columns(3)
kpi_total_vessels = metrics_col[0].empty()
kpi_avg_speed = metrics_col[1].empty()
kpi_last_update = metrics_col[2].empty()

map_placeholder = st.empty()

def update_map(df: pd.DataFrame):
    """Generates the 3D PyDeck map."""
    view_state = pdk.ViewState(
        latitude=df["latitude"].mean() if not df.empty else 12.58,
        longitude=df["longitude"].mean() if not df.empty else 43.33,
        zoom=3,
        pitch=45
    )

    # Vessel Layer (Scatterplot)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_color=[0, 229, 255, 200],  # Cyan
        get_radius=20000,
        pickable=True
    )

    # Text Layer (Vessel Names)
    text_layer = pdk.Layer(
        "TextLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_text="vessel_name",
        get_size=16,
        get_color=[255, 255, 255, 200],
        get_alignment_baseline="'bottom'"
    )

    r = pdk.Deck(
        layers=[layer, text_layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/dark-v10",
        tooltip={"text": "Vessel: {vessel_name}\nType: {vessel_type}\nSpeed: {speed_knots} kts"}
    )
    
    map_placeholder.pydeck_chart(r)

# Streaming Loop
try:
    while True:
        # Poll Kafka for new positions
        msg = consumer.poll(timeout=0.1)
        
        if msg is not None and not msg.error():
            payload = json.loads(msg.value().decode('utf-8'))
            
            # Update state with latest position for this MMSI
            st.session_state.vessels[payload["mmsi"]] = payload
            
        # Every iteration, render the current state
        if st.session_state.vessels:
            df = pd.DataFrame.from_dict(st.session_state.vessels, orient='index')
            
            # Update KPIs
            kpi_total_vessels.metric("Active Vessels Tracked", len(df))
            kpi_avg_speed.metric("Average Fleet Speed", f"{df['speed_knots'].mean():.1f} kts")
            kpi_last_update.metric("Last Telemetry Ping", datetime.now().strftime("%H:%M:%S"))
            
            # Update Map
            update_map(df)
            
        # Control refresh rate of the Streamlit app
        time.sleep(0.5)

except Exception as e:
    logger.error(f"Stream interrupted: {e}")