"""
GeoLogix: Initialize PostgreSQL Streaming Sink
"""
import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger("DB_Init")

DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB", "geologix_streaming"),
    "user": os.getenv("POSTGRES_USER", "geologix_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "supersecretpassword"),
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": os.getenv("POSTGRES_PORT", "5433")
}

STREAMING_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS live_vessel_telemetry (
    event_id SERIAL PRIMARY KEY,
    mmsi BIGINT NOT NULL,
    imo VARCHAR(20) NOT NULL,
    vessel_name VARCHAR(100),
    vessel_type VARCHAR(50),
    latitude NUMERIC(10, 6) NOT NULL,
    longitude NUMERIC(10, 6) NOT NULL,
    speed_knots NUMERIC(5, 2),
    heading_degrees NUMERIC(5, 2),
    nearest_chokepoint VARCHAR(100),
    timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for Power BI Time-Intelligence and DirectQuery performance
CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON live_vessel_telemetry(timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_mmsi ON live_vessel_telemetry(mmsi);
"""

def initialize_database():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("Connected to PostgreSQL successfully.")
        cursor.execute(STREAMING_TABLE_DDL)
        logger.info("Streaming table 'live_vessel_telemetry' initialized with indexes.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

if __name__ == "__main__":
    initialize_database()