from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    description='Spark KPI computation every 1 minute',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['analytics', 'spark', 'production'],
)

compute_kpis = BashOperator(
    task_id='compute_kpis_spark',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 1g \
      --executor-memory 1g \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
      /opt/spark-apps/compute_kpis.py
    ''',
    dag=dag,
)

compute_kpis
