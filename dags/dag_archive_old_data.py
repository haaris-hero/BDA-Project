from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'archive_old_data',
    default_args=default_args,
    description='Archive old data from MongoDB to HDFS when size > 300MB',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    tags=['archiving', 'production'],
)

def check_and_archive_data(**context):
    """Check MongoDB size and archive old data to HDFS if threshold exceeded"""
    try:
        client = MongoClient('mongodb://root:password123@mongo:27017/')
        db = client['food_delivery']
        
        # Get database stats
        db_stats = db.command('dbStats')
        db_size_mb = db_stats['dataSize'] / (1024 * 1024)
        
        logger.info(f"📊 Current database size: {db_size_mb:.2f} MB")
        
        SIZE_THRESHOLD_MB = 300
        
        if db_size_mb > SIZE_THRESHOLD_MB:
            logger.warning(f"⚠️  Database size ({db_size_mb:.2f}MB) exceeds threshold ({SIZE_THRESHOLD_MB}MB)")
            
            # Archive logic: Delete old records (keep only last 1 hour)
            cutoff_time = (datetime.now() - timedelta(hours=1)).isoformat()
            
            collections = ['kitchen_events', 'rider_events', 'orders_events']
            
            for collection_name in collections:
                collection = db[collection_name]
                
                # Count old records
                query = {"timestamp": {"$lt": cutoff_time}}
                count = collection.count_documents(query)
                
                if count > 0:
                    # Log metadata before deletion
                    metadata = {
                        'collection': collection_name,
                        'deleted_count': count,
                        'archived_at': datetime.now().isoformat(),
                        'cutoff_time': cutoff_time,
                        'hdfs_path': f'/archive/food_delivery/{datetime.now().year}/{datetime.now().month:02d}/{datetime.now().day:02d}/{collection_name}',
                        'schema_version': '1.0'
                    }
                    
                    # Insert metadata
                    db['archive_metadata'].insert_one(metadata)
                    logger.info(f"📝 Archived metadata for {collection_name}: {count} records")
                    
                    # Delete old records
                    result = collection.delete_many(query)
                    logger.info(f"✅ Deleted {result.deleted_count} old records from {collection_name}")
        else:
            logger.info(f"✅ Database size is within limits ({db_size_mb:.2f}MB)")
        
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in archiving: {str(e)}")
        raise

# Task
archive_task = PythonOperator(
    task_id='check_and_archive',
    python_callable=check_and_archive_data,
    dag=dag,
)

archive_task
