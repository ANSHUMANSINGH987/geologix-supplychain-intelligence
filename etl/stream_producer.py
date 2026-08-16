"""
GeoLogix: High-Throughput Kafka Stream Producer
Simulates live vessel movements at 10+ pings/sec using physical interpolation.
"""
import time
import json
import math
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger("Kafka_Producer")

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "vessel_stream"

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
kafka_producer = Producer(producer_conf)

# Baseline state for interpolation
FLEET = [
    {"mmsi": 219018000, "imo": "IMO9632064", "name": "MAERSK MC-KINNEY MOLLER", "type": "ULCV", "lat": 12.50, "lon": 43.30, "sog": 18.5, "cog": 320.0, "cp": "BAB_EL_MANDEB"},
    {"mmsi": 353131000, "imo": "IMO9839438", "name": "MSC GULSUN", "type": "ULCV", "lat": -34.30, "lon": 18.40, "sog": 19.2, "cog": 270.0, "cp": "CAPE_OF_GOOD_HOPE"},
    {"mmsi": 228386800, "imo": "IMO9839210", "name": "CMA CGM JACQUES SAADE", "type": "LNG", "lat": 30.50, "lon": 32.20, "sog": 12.0, "cog": 10.0, "cp": "SUEZ_CANAL"}
]

def delivery_report(err, msg):
    """Callback triggered by Kafka once a message is delivered."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def stream_live_telemetry():
    """Physics engine running at 10 Hz pushing to Kafka."""
    logger.info(f"Starting Live Stream to Kafka Topic: {TOPIC_NAME} at 10 pings/sec")
    
    # Time delta per loop (0.1 seconds = 10 Hz)
    dt_seconds = 0.1 
    
    try:
        while True:
            for vessel in FLEET:
                # Physics calculation: 1 knot = 0.514444 m/s. 
                # Converting speed to degrees roughly (1 deg lat ~= 111,320m)
                speed_ms = vessel["sog"] * 0.514444
                
                # Math angle (0 is North/COG, clockwise)
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
                
                # Push to Kafka
                kafka_producer.produce(
                    TOPIC_NAME,
                    key=str(vessel["mmsi"]),
                    value=json.dumps(payload),
                    callback=delivery_report
                )
            
            # Poll handles delivery callbacks, sleep maintains our 10Hz frequency
            kafka_producer.poll(0)
            time.sleep(dt_seconds)
            
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user.")
    finally:
        kafka_producer.flush()
        logger.info("Kafka producer flushed and safely shut down.")

if __name__ == "__main__":
    stream_live_telemetry()