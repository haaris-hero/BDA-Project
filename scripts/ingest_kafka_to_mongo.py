import json
from kafka import KafkaConsumer
from pymongo import MongoClient
import logging
import sys
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_stream(topic_name, collection_name):
    """
    Consume from Kafka topic and insert into MongoDB
    """
    try:
        # MongoDB connection
        mongo_client = MongoClient('mongodb://root:password123@mongo:27017/')
        db = mongo_client['food_delivery']
        collection = db[collection_name]
        
        logger.info(f"✅ Connected to MongoDB collection: {collection_name}")
        
        # Kafka consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers='kafka:9092',
            group_id=f'ingest-{collection_name}',
            auto_offset_reset='latest',
            max_poll_records=100,
            consumer_timeout_ms=30000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        logger.info(f"✅ Connected to Kafka topic: {topic_name}")
        logger.info(f"🔄 Listening for messages from {topic_name}...")
        
        inserted_count = 0
        
        while True:
            for message in consumer:
                try:
                    # Insert into MongoDB
                    result = collection.insert_one(message.value)
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:
                        count = collection.count_documents({})
                        logger.info(f"[{collection_name.upper()}] Inserted #{inserted_count} | Total in DB: {count}")
                    
                except Exception as e:
                    logger.debug(f"Insert error (likely duplicate): {str(e)[:50]}")
        
    except KeyboardInterrupt:
        logger.info(f"🛑 Ingestion for {collection_name} stopped")
        mongo_client.close()
        consumer.close()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Fatal error in {collection_name}: {str(e)}")
        mongo_client.close()
        consumer.close()
        sys.exit(1)

if __name__ == "__main__":
    import threading
    
    logger.info("=" * 50)
    logger.info("🚀 KAFKA → MONGODB INGESTION STARTED")
    logger.info("=" * 50)
    
    # Start 3 ingestion threads (one for each topic)
    threads = [
        threading.Thread(target=ingest_stream, args=('kitchen_stream', 'kitchen_events'), daemon=True),
        threading.Thread(target=ingest_stream, args=('rider_stream', 'rider_events'), daemon=True),
        threading.Thread(target=ingest_stream, args=('orders_stream', 'orders_events'), daemon=True),
    ]
    
    for thread in threads:
        thread.start()
    
    # Keep main thread alive
    for thread in threads:
        thread.join()
