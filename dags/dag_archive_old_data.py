from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import logging
import subprocess
import json
import os

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
            
            # Archive logic: Export old records to HDFS (keep only last 1 hour)
            cutoff_time = (datetime.now() - timedelta(hours=1)).isoformat()
            cutoff_datetime = datetime.now() - timedelta(hours=1)
            
            collections = ['kitchen_events', 'rider_events', 'orders_events']
            
            for collection_name in collections:
                collection = db[collection_name]
                
                # Count old records
                query = {"timestamp": {"$lt": cutoff_time}}
                count = collection.count_documents(query)
                
                if count > 0:
                    logger.info(f"📦 Archiving {count} records from {collection_name}...")
                    
                    # Export to JSONL format
                    temp_file = f"/tmp/{collection_name}_{int(datetime.now().timestamp())}.jsonl"
                    archived_records = list(collection.find(query))
                    
                    # Write to temporary JSONL file
                    with open(temp_file, 'w') as f:
                        for record in archived_records:
                            # Convert ObjectId to string for JSON serialization
                            if '_id' in record:
                                record['_id'] = str(record['_id'])
                            f.write(json.dumps(record) + '\n')
                    
                    file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
                    logger.info(f"📄 Created temporary file: {temp_file} ({file_size_mb:.2f}MB)")
                    
                    # Create HDFS directory structure
                    hdfs_base_path = f"/archive/food_delivery/{datetime.now().year}/{datetime.now().month:02d}/{datetime.now().day:02d}"
                    hdfs_file_path = f"{hdfs_base_path}/{collection_name}_{int(datetime.now().timestamp())}.jsonl"
                    
                    # Upload to HDFS using hadoop fs command
                    try:
                        # Create directory in HDFS
                        subprocess.run([
                            'docker', 'exec', 'hadoop-namenode',
                            'hdfs', 'dfs', '-mkdir', '-p', hdfs_base_path
                        ], check=True, capture_output=True)
                        
                        # Copy file to HDFS
                        subprocess.run([
                            'docker', 'cp', temp_file, f'hadoop-namenode:/tmp/{os.path.basename(temp_file)}'
                        ], check=True, capture_output=True)
                        
                        subprocess.run([
                            'docker', 'exec', 'hadoop-namenode',
                            'hdfs', 'dfs', '-put',
                            f'/tmp/{os.path.basename(temp_file)}',
                            hdfs_file_path
                        ], check=True, capture_output=True)
                        
                        logger.info(f"✅ Uploaded to HDFS: {hdfs_file_path}")
                        
                        # Store metadata
                        metadata = {
                            'collection': collection_name,
                            'deleted_count': count,
                            'archived_at': datetime.now().isoformat(),
                            'cutoff_time': cutoff_time,
                            'hdfs_path': hdfs_file_path,
                            'file_size_mb': round(file_size_mb, 2),
                            'schema_version': '1.0'
                        }
                        
                        db['archive_metadata'].insert_one(metadata)
                        logger.info(f"📝 Stored metadata for {collection_name}")
                        
                        # Delete old records from MongoDB
                        result = collection.delete_many(query)
                        logger.info(f"✅ Deleted {result.deleted_count} old records from {collection_name}")
                        
                        # Clean up temporary file
                        os.remove(temp_file)
                        
                    except subprocess.CalledProcessError as e:
                        logger.error(f"❌ HDFS upload failed: {e.stderr.decode() if e.stderr else str(e)}")
                        # Still delete records if HDFS fails (data loss risk, but keeps MongoDB lean)
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                        logger.warning(f"⚠️  Proceeding with deletion despite HDFS failure")
                        result = collection.delete_many(query)
                        logger.info(f"✅ Deleted {result.deleted_count} records (HDFS archive failed)")
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
