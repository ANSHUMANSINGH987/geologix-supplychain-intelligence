"""
GeoLogix Data Pipeline: Kafka to PostgreSQL Micro-Batch Consumer
Author: Anshuman Singh
Description: Consumes high-throughput telemetry from the 'vessel_stream' Kafka topic
             and executes micro-batched bulk inserts into PostgreSQL for Power BI DirectQuery.
"""

import json
import os
import time
import logging
from datetime import datetime
from typing import List, Tuple
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_values

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("Kafka_Postgres_Consumer")

# Database & Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9093")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "vessel_stream")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "postgres_sink_group")

DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB", "geologix_streaming"),
    "user": os.getenv("POSTGRES_USER", "geologix_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "supersecretpassword"),
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": os.getenv("POSTGRES_PORT", "5433")
}

# Micro-Batching Tuning Parameters
BATCH_SIZE = 50
BATCH_TIMEOUT_SEC = 1.0

INSERT_SQL = """
INSERT INTO live_vessel_telemetry (
    mmsi, imo, vessel_name, vessel_type, latitude, longitude,
    speed_knots, heading_degrees, nearest_chokepoint, timestamp_utc
) VALUES %s
"""

def create_db_connection():
    """Establishes and returns a connection to PostgreSQL with autocommit."""
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    return conn

def flush_batch_to_postgres(cursor, records: List[Tuple]):
    """Executes an optimized multi-row insert into PostgreSQL."""
    if not records:
        return
    
    execute_values(cursor, INSERT_SQL, records)
    logger.info("Successfully flushed batch of %d records to PostgreSQL.", len(records))

def run_consumer():
    """Main consumer loop reading from Kafka and sinking to Postgres."""
    # 1. Initialize Kafka Consumer
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])
    logger.info("Subscribed to Kafka topic: %s with consumer group: %s", TOPIC_NAME, GROUP_ID)

    # 2. Initialize Database Connection
    db_conn = create_db_connection()
    cursor = db_conn.cursor()
    logger.info("Connected to PostgreSQL database: %s", DB_PARAMS["dbname"])

    buffer: List[Tuple] = []
    last_flush_time = time.time()

    try:
        while True:
            # Poll for Kafka messages (timeout 0.1s)
            msg = consumer.poll(timeout=0.1)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                else:
                    payload = json.loads(msg.value().decode('utf-8'))
                    record_tuple = (
                        payload["mmsi"],
                        payload["imo"],
                        payload["vessel_name"],
                        payload["vessel_type"],
                        payload["latitude"],
                        payload["longitude"],
                        payload["speed_knots"],
                        payload["heading_degrees"],
                        payload["nearest_chokepoint"],
                        payload["timestamp_utc"]
                    )
                    buffer.append(record_tuple)

            # Check if buffer threshold or time threshold is met
            current_time = time.time()
            time_elapsed = current_time - last_flush_time

            if len(buffer) >= BATCH_SIZE or (buffer and time_elapsed >= BATCH_TIMEOUT_SEC):
                flush_batch_to_postgres(cursor, buffer)
                buffer.clear()
                last_flush_time = current_time

    except KeyboardInterrupt:
        logger.info("Consumer shutdown requested by user.")
    finally:
        # Flush remaining records before exiting
        if buffer:
            flush_batch_to_postgres(cursor, buffer)
        cursor.close()
        db_conn.close()
        consumer.close()
        logger.info("Kafka consumer and database connections cleanly closed.")

if __name__ == "__main__":
    run_consumer()