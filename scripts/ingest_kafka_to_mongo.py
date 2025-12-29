import json
import logging
import time
from datetime import datetime

from kafka import KafkaConsumer
from pymongo import MongoClient
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, JSON
# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# Postgres Configuration
# -------------------------------------------------
POSTGRES_URI = "postgresql://superset:superset@superset-postgres:5432/superset"

pg_engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
pg_metadata = MetaData()


def pg_table(name):
    return Table(
        name,
        pg_metadata,
        Column("id", Integer, primary_key=True),
        Column("event_time", String),
        Column("payload", JSON),
        extend_existing=True,
    )


tables = {
    "orders_stream": pg_table("orders_events"),
    "rider_stream": pg_table("rider_events"),
    "kitchen_stream": pg_table("kitchen_events"),
}


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def sanitize(event):
    """Ensure payload is JSON-serializable for Postgres"""
    clean = {}
    for k, v in event.items():
        if k == "_id":
            continue
        try:
            json.dumps(v)
            clean[k] = v
        except Exception:
            clean[k] = str(v)
    return clean


# -------------------------------------------------
# Main Ingestion Logic
# -------------------------------------------------
def main():
    # ---------------------------
    # MongoDB
    # ---------------------------
    mongo = MongoClient(
        "mongodb://root:password123@mongo:27017/",
        serverSelectionTimeoutMS=5000
    )
    mongo_db = mongo["food_delivery"]
    logger.info("✅ Connected to MongoDB")

    # ---------------------------
    # Postgres
    # ---------------------------
    pg_metadata.create_all(pg_engine)
    pg_conn = pg_engine.connect()
    logger.info("✅ Connected to Postgres")

    # ---------------------------
    # Kafka Consumer
    # ---------------------------
    consumer = KafkaConsumer(
        *tables.keys(),
        bootstrap_servers="kafka:9092",
        group_id="ingest-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_interval_ms=300000,
    )

    logger.info("🚀 Kafka → MongoDB + Postgres ingestion running")

    # ---------------------------
    # Ingestion Loop
    # ---------------------------
    while True:
        records = consumer.poll(timeout_ms=1000)

        for tp, messages in records.items():
            topic_name = tp.topic

            for msg in messages:
                try:
                    # Skip empty messages
                    if msg.value is None:
                        continue

                    # Safe JSON parsing
                    event = json.loads(msg.value.decode("utf-8"))

                    # ------------------
                    # MongoDB insert
                    # ------------------
                    mongo_db[
                        topic_name.replace("_stream", "_events")
                    ].insert_one(event)

                    # ------------------
                    # Postgres insert
                    # ------------------
                    pg_conn.execute(
                        tables[topic_name]
                        .insert()
                        .values(
                            event_time=datetime.utcnow().isoformat(),
                            payload=sanitize(event),
                        )
                    )

                except json.JSONDecodeError:
                    logger.warning(
                        f"⚠️ Skipping non-JSON message from {topic_name}: {msg.value}"
                    )

                except SQLAlchemyError as e:
                    logger.error(f"❌ Postgres error: {e}")

                except Exception as e:
                    logger.error(f"❌ Ingest error: {e}")

        time.sleep(0.2)


# -------------------------------------------------
# Entry Point
# -------------------------------------------------
if __name__ == "__main__":
    main()
