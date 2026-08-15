"""
GeoLogix Data Pipeline: Live Marine Meteorological Extraction
Author: Anshuman Singh
Description: Ingests real-time sea state, wave height, and wind metrics for 
             strategic global maritime chokepoints and hub ports using Open-Meteo.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pandas as pd
import requests
from pydantic import BaseModel, Field, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("extract_marine_weather")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Strategic Chokepoints & Key Port Coordinates
MARITIME_NODES: Dict[str, Dict[str, Any]] = {
    "SUEZ_CANAL_NORTH": {"name": "Suez Canal (Port Said)", "lat": 31.26, "lon": 32.30},
    "BAB_EL_MANDEB": {"name": "Bab el-Mandeb Strait", "lat": 12.58, "lon": 43.33},
    "STRAIT_OF_HORMUZ": {"name": "Strait of Hormuz", "lat": 26.56, "lon": 56.25},
    "CAPE_OF_GOOD_HOPE": {"name": "Cape of Good Hope", "lat": -34.35, "lon": 18.47},
    "STRAIT_OF_MALACCA": {"name": "Strait of Malacca", "lat": 4.00, "lon": 100.00},
    "PORT_ROTTERDAM": {"name": "Port of Rotterdam", "lat": 51.92, "lon": 4.47},
    "PORT_SHANGHAI": {"name": "Port of Shanghai", "lat": 31.23, "lon": 121.47},
    "PORT_SINGAPORE": {"name": "Port of Singapore", "lat": 1.35, "lon": 103.81}
}

class MarineTelemetry(BaseModel):
    """Pydantic model enforcing type safety and schema validation."""
    node_code: str
    node_name: str
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    ingestion_timestamp_utc: str
    wave_height_meters: Optional[float] = None
    wave_direction_degrees: Optional[float] = None
    wave_period_seconds: Optional[float] = None
    wind_wave_height_meters: Optional[float] = None

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout))
)
def fetch_node_weather(lat: float, lon: float) -> Dict[str, Any]:
    """Queries Open-Meteo Marine API with automated retry on transient network errors."""
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["wave_height", "wave_direction", "wave_period", "wind_wave_height"],
        "timezone": "UTC"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def extract_all_maritime_weather() -> List[Dict[str, Any]]:
    """Iterates through all maritime nodes, validates the responses, and persists raw snapshots."""
    validated_records: List[Dict[str, Any]] = []
    current_utc = datetime.now(timezone.utc).isoformat()

    logger.info("Initiating live maritime weather ingestion from Open-Meteo API...")

    for code, info in MARITIME_NODES.items():
        try:
            raw_response = fetch_node_weather(info["lat"], info["lon"])
            current_metrics = raw_response.get("current", {})

            record = MarineTelemetry(
                node_code=code,
                node_name=info["name"],
                latitude=info["lat"],
                longitude=info["lon"],
                ingestion_timestamp_utc=current_utc,
                wave_height_meters=current_metrics.get("wave_height"),
                wave_direction_degrees=current_metrics.get("wave_direction"),
                wave_period_seconds=current_metrics.get("wave_period"),
                wind_wave_height_meters=current_metrics.get("wind_wave_height")
            )
            validated_records.append(record.model_dump())
            logger.info("Successfully validated telemetry for node: %s", info["name"])
        except ValidationError as val_err:
            logger.error("Schema validation failed for node %s: %s", info["name"], val_err)
        except Exception as exc:
            logger.error("Failed to extract data for node %s: %s", info["name"], exc)

    if validated_records:
        file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_output_path = os.path.join(RAW_DATA_DIR, f"raw_marine_weather_{file_timestamp}.json")
        
        with open(raw_output_path, "w", encoding="utf-8") as f:
            json.dump(validated_records, f, indent=2)
            
        logger.info("Persisted %d raw records to %s", len(validated_records), raw_output_path)
    else:
        logger.warning("No records were extracted.")

    return validated_records

if __name__ == "__main__":
    records = extract_all_maritime_weather()
    if records:
        df = pd.DataFrame(records)
        print("\n--- Ingested Live Marine Meteorological Telemetry ---")
        print(df[["node_code", "wave_height_meters", "wave_period_seconds", "wind_wave_height_meters"]])