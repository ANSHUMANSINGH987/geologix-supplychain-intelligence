import os
import json
import asyncio
import psycopg2
from datetime import datetime
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from confluent_kafka import Consumer

app = FastAPI(title="GeoLogix Intelligence API")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
KAFKA_BROKER = "127.0.0.1:9093"
TOPIC_NAME = "vessel_stream"

def get_db_connection():
    return psycopg2.connect(dbname="geologix_streaming", user="geologix_admin", password="supersecretpassword", host="127.0.0.1", port="5433")

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    current_date = datetime.now().strftime("%A, %d %B %Y")
    active_vessels = 0
    total_cost_impact = 0.0
    total_co2_impact = 0.0
    avg_delay = 12.6
    
    # Industry-standard baseline metrics per vessel type
    vessel_economics = {
        "Ultra Large Container Vessel": {"value": 120_000_000, "burn": 150},
        "Neo-Panamax Container": {"value": 90_000_000, "burn": 120},
        "Suezmax Tanker": {"value": 65_000_000, "burn": 90},
        "LNG Carrier": {"value": 150_000_000, "burn": 100},
        "Bulk Carrier": {"value": 30_000_000, "burn": 60}
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vessel_type, COUNT(DISTINCT mmsi) 
            FROM live_vessel_telemetry 
            WHERE timestamp_utc >= NOW() - INTERVAL '5 minutes'
            GROUP BY vessel_type;
        """)
        results = cursor.fetchall()
        conn.close()

        for row in results:
            v_type, count = row[0], row[1]
            active_vessels += count
            if v_type in vessel_economics:
                # 10% holding cost per year over the delay period
                total_cost_impact += (count * vessel_economics[v_type]["value"] * 0.10 * (avg_delay/365))
                # Burn rate * delay days * IMO carbon factor
                total_co2_impact += (count * vessel_economics[v_type]["burn"] * avg_delay * 3.114)

    except Exception as e:
        print(f"DB Error: {e}")

    metrics = {
        "active_vessels": active_vessels,
        "avg_delay": f"+{avg_delay}",
        "cost_impact": f"${(total_cost_impact / 1_000_000):.1f}M",
        "co2_impact": f"{(total_co2_impact / 1_000_000):.2f}M"
    }
    
    return templates.TemplateResponse(request=request, name="index.html", context={"request": request, "metrics": metrics, "current_date": current_date})

@app.websocket("/ws/fleet")
async def websocket_fleet_stream(websocket: WebSocket):
    await websocket.accept()
    conf = {'bootstrap.servers': KAFKA_BROKER, 'group.id': 'fastapi_ws_group', 'auto.offset.reset': 'latest'}
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])
    try:
        while True:
            msg = await asyncio.to_thread(consumer.poll, 0.1)
            if msg is not None and not msg.error():
                await websocket.send_text(msg.value().decode('utf-8'))
            await asyncio.sleep(0.01) 
    except Exception:
        pass
    finally:
        consumer.close()