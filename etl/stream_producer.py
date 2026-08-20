"""
GeoLogix: Massive-Scale Kafka Stream Producer
Simulates 500+ live commercial vessels across global corridors at 10 pings/sec.
"""
import time
import json
import math
import random
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger("Kafka_Producer_Massive")

KAFKA_BROKER = "127.0.0.1:9093"
TOPIC_NAME = "vessel_stream"

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
kafka_producer = Producer(producer_conf)

# Expanded Oceanic Zones for realistic map distribution
ZONES = [
    {"name": "CAPE_OF_GOOD_HOPE", "lat_range": (-40.0, 0.0), "lon_range": (20.0, 70.0), "cog_range": (210, 260)},
    {"name": "BAB_EL_MANDEB", "lat_range": (5.0, 18.0), "lon_range": (45.0, 70.0), "cog_range": (270, 310)},
    {"name": "SUEZ_CANAL", "lat_range": (32.0, 38.0), "lon_range": (5.0, 30.0), "cog_range": (100, 140)},
    {"name": "STRAIT_OF_MALACCA", "lat_range": (0.0, 15.0), "lon_range": (85.0, 115.0), "cog_range": (90, 130)},
    {"name": "NORTH_ATLANTIC", "lat_range": (20.0, 50.0), "lon_range": (-60.0, -10.0), "cog_range": (70, 110)}
]

VESSEL_TYPES = ["Ultra Large Container Vessel", "Neo-Panamax Container", "Suezmax Tanker", "LNG Carrier", "Bulk Carrier"]
CARRIERS = ["Maersk", "MSC", "CMA CGM", "Hapag-Lloyd", "COSCO", "ONE"]

def generate_global_fleet(fleet_size=500):
    fleet = []
    for i in range(fleet_size):
        zone = random.choice(ZONES)
        fleet.append({
            "mmsi": 200000000 + i,
            "imo": f"IMO9{random.randint(100000, 999999)}",
            "name": f"{random.choice(CARRIERS)} VOYAGER {i}",
            "type": random.choice(VESSEL_TYPES),
            "lat": random.uniform(*zone["lat_range"]),
            "lon": random.uniform(*zone["lon_range"]),
            "sog": random.uniform(12.0, 22.0),
            "cog": random.uniform(*zone["cog_range"]),
            "cp": zone["name"]
        })
    return fleet

def stream_live_telemetry():
    logger.info("Initializing oceanic global fleet (500 vessels)...")
    fleet = generate_global_fleet(500)
    dt_seconds = 0.1 
    
    try:
        while True:
            for vessel in fleet:
                speed_ms = vessel["sog"] * 0.514444
                rad_heading = math.radians(90 - vessel["cog"]) 
                
                dx = (speed_ms * math.cos(rad_heading) * dt_seconds) / 111320.0
                dy = (speed_ms * math.sin(rad_heading) * dt_seconds) / 111320.0
                
                vessel["lat"] += dy
                vessel["lon"] += dx
                
                payload = {
                    "mmsi": vessel["mmsi"],
                    "imo": vessel["imo"],
                    "vessel_name": vessel["name"],
                    "vessel_type": vessel["type"],
                    "latitude": round(vessel["lat"], 6),
                    "longitude": round(vessel["lon"], 6),
                    "speed_knots": vessel["sog"],
                    "heading_degrees": vessel["cog"],
                    "nearest_chokepoint": vessel["cp"],
                    "timestamp_utc": datetime.now(timezone.utc).isoformat()
                }
                
                kafka_producer.produce(
                    TOPIC_NAME,
                    key=str(vessel["mmsi"]),
                    value=json.dumps(payload)
                )
            
            kafka_producer.poll(0)
            time.sleep(dt_seconds)
            
    except KeyboardInterrupt:
        logger.info("Streaming stopped.")
    finally:
        kafka_producer.flush()

if __name__ == "__main__":
    stream_live_telemetry()