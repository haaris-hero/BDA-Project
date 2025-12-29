from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import redis
import json

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def cache_kpis_to_redis():
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    r.set(
        "latest_kpis",
        json.dumps({
            "status": "updated",
            "timestamp": datetime.utcnow().isoformat()
        })
    )

with DAG(
    dag_id="spark_compute_kpis",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["spark", "kpi"],
) as dag:

    run_spark_kpis = BashOperator(
        task_id="run_spark_kpis",
        bash_command="""
        docker exec spark-master bash -lc '
          mkdir -p /tmp/ivy && chmod 777 /tmp/ivy && \
          /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.0 \
            --conf spark.jars.ivy=/tmp/ivy \
            --conf spark.mongodb.read.connection.uri="mongodb://root:password123@mongo:27017/food_delivery?authSource=admin" \
            --conf spark.mongodb.write.connection.uri="mongodb://root:password123@mongo:27017/food_delivery?authSource=admin" \
            /opt/spark-apps/compute_kpis.py
        '
        """,
    )

    cache_kpis = PythonOperator(
        task_id="cache_kpis_to_redis",
        python_callable=cache_kpis_to_redis,
    )

    run_spark_kpis >> cache_kpis
