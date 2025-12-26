from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='spark_compute_kpis',
    default_args=default_args,
    description='Spark job to compute real-time KPIs every minute',
    schedule_interval='*/1 * * * *',
    catchup=False,
    tags=['analytics', 'spark', 'production'],
)

# ============================================================
# Spark KPI Computation (RUNS INSIDE SPARK CONTAINER)
# ============================================================
compute_kpis = BashOperator(
    task_id='compute_kpis_spark',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
      --conf spark.mongodb.input.uri=mongodb://root:password123@mongo:27017/food_delivery \
      --conf spark.mongodb.output.uri=mongodb://root:password123@mongo:27017/food_delivery \
      /opt/spark-apps/compute_kpis.py
    """,
    dag=dag,
)

# ============================================================
# Cache KPIs to Redis
# ============================================================
def cache_to_redis_task():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from cache_kpis_to_redis import cache_kpis_to_redis
    return cache_kpis_to_redis()

cache_to_redis = PythonOperator(
    task_id='cache_kpis_to_redis',
    python_callable=cache_to_redis_task,
    dag=dag,
)

compute_kpis >> cache_to_redis
