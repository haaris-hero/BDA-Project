from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'ingest_streams_to_mongo',
    default_args=default_args,
    description='Ingest Kafka streams to MongoDB hot storage',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['ingestion', 'production'],
)

def consume_kitchen_stream(**context):
    """Consume kitchen_stream from Kafka and insert to MongoDB"""
    try:
        consumer = KafkaConsumer(
            'kitchen_stream',
            bootstrap_servers='kafka:9092',
            group_id='mongo-ingest-kitchen',
            auto_offset_reset='latest',
            max_poll_records=100,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        client = MongoClient('mongodb://root:password123@mongo:27017/')
        db = client['food_delivery']
        collection = db['kitchen_events']
        
        messages_inserted = 0
        for message in consumer:
            try:
                collection.insert_one(message.value)
                messages_inserted += 1
            except Exception as e:
                logger.debug(f"Duplicate: {str(e)[:50]}")
        
        consumer.close()
        client.close()
        logger.info(f"✅ Kitchen: {messages_inserted} records inserted")
        return messages_inserted
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def consume_rider_stream(**context):
    """Consume rider_stream from Kafka and insert to MongoDB"""
    try:
        consumer = KafkaConsumer(
            'rider_stream',
            bootstrap_servers='kafka:9092',
            group_id='mongo-ingest-rider',
            auto_offset_reset='latest',
            max_poll_records=100,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        client = MongoClient('mongodb://root:password123@mongo:27017/')
        db = client['food_delivery']
        collection = db['rider_events']
        
        messages_inserted = 0
        for message in consumer:
            collection.insert_one(message.value)
            messages_inserted += 1
        
        consumer.close()
        client.close()
        logger.info(f"✅ Rider: {messages_inserted} records inserted")
        return messages_inserted
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def consume_orders_stream(**context):
    """Consume orders_stream from Kafka and insert to MongoDB"""
    try:
        consumer = KafkaConsumer(
            'orders_stream',
            bootstrap_servers='kafka:9092',
            group_id='mongo-ingest-orders',
            auto_offset_reset='latest',
            max_poll_records=100,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        client = MongoClient('mongodb://root:password123@mongo:27017/')
        db = client['food_delivery']
        collection = db['orders_events']
        
        messages_inserted = 0
        for message in consumer:
            try:
                collection.insert_one(message.value)
                messages_inserted += 1
            except Exception as e:
                logger.debug(f"Duplicate: {str(e)[:50]}")
        
        consumer.close()
        client.close()
        logger.info(f"✅ Orders: {messages_inserted} records inserted")
        return messages_inserted
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

# Tasks
task_kitchen = PythonOperator(task_id='ingest_kitchen', python_callable=consume_kitchen_stream, dag=dag)
task_rider = PythonOperator(task_id='ingest_rider', python_callable=consume_rider_stream, dag=dag)
task_orders = PythonOperator(task_id='ingest_orders', python_callable=consume_orders_stream, dag=dag)

[task_kitchen, task_rider, task_orders]
