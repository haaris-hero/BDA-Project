from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'spark_compute_kpis',
    default_args=default_args,
    description='Spark job to compute real-time KPIs every minute',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['analytics', 'spark', 'production'],
)

# Use BashOperator with docker exec - more reliable for cross-container communication
compute_kpis = BashOperator(
    task_id='compute_kpis_spark',
    bash_command="""
    # Use docker exec to submit Spark job from Spark master container
    DOCKER_API_VERSION=1.44 docker exec spark-master bash -c '
        mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars 2>/dev/null || true
        export IVY_HOME=/tmp/.ivy2
        export SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2"
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --driver-memory 1g \
          --executor-memory 1g \
          --executor-cores 2 \
          --deploy-mode client \
          --conf spark.mongodb.input.uri=mongodb://root:password123@mongo:27017/food_delivery \
          --conf spark.mongodb.output.uri=mongodb://root:password123@mongo:27017/food_delivery \
          --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
          /opt/spark-apps/compute_kpis.py
    '
    """,
    dag=dag,
)

def cache_to_redis_task(**context):
    """Cache KPIs from MongoDB to Redis"""
    import sys
    sys.path.append('/opt/airflow/scripts')
    from cache_kpis_to_redis import cache_kpis_to_redis
    return cache_kpis_to_redis()

cache_to_redis = PythonOperator(
    task_id='cache_kpis_to_redis',
    python_callable=cache_to_redis_task,
    dag=dag,
)

# Set task dependencies: Spark job runs first, then cache to Redis
compute_kpis >> cache_to_redis
