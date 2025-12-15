#!/bin/bash

# Install Python dependencies in airflow container
docker exec airflow-webserver pip install kafka-python pymongo faker numpy -q

# Run 3 data generators in background
echo "Starting Kitchen Stream Generator..."
docker exec -d airflow-webserver python /home/airflow/gcs/dags/../scripts/generate_kitchen_stream.py

echo "Starting Rider Stream Generator..."
docker exec -d airflow-webserver python /home/airflow/gcs/dags/../scripts/generate_rider_stream.py

echo "Starting Orders Stream Generator..."
docker exec -d airflow-webserver python /home/airflow/gcs/dags/../scripts/generate_orders_stream.py

echo "All generators started. Check with: docker-compose logs -f"
