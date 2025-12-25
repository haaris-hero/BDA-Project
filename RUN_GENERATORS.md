# How to Run Data Generators

## Problem
When running generators in a separate Docker container, you need to use the correct network name.

## Solution: Use the airflow-webserver Container (Recommended)

The `airflow-webserver` container is already on the correct network and has all dependencies installed.

### Option 1: Run in Separate Terminals (Interactive)

**Terminal 1 - Kitchen Stream:**
```bash
docker exec -it airflow-webserver python scripts/generate_kitchen_stream.py
```

**Terminal 2 - Rider Stream:**
```bash
docker exec -it airflow-webserver python scripts/generate_rider_stream.py
```

**Terminal 3 - Orders Stream:**
```bash
docker exec -it airflow-webserver python scripts/generate_orders_stream.py
```

### Option 2: Run in Background (Non-Interactive)

**Terminal 1 - Kitchen Stream:**
```bash
docker exec -d airflow-webserver python scripts/generate_kitchen_stream.py
```

**Terminal 2 - Rider Stream:**
```bash
docker exec -d airflow-webserver python scripts/generate_rider_stream.py
```

**Terminal 3 - Orders Stream:**
```bash
docker exec -d airflow-webserver python scripts/generate_orders_stream.py
```

---

## Alternative: Run in Separate Container (If Needed)

If you must use a separate container, use the correct network name:

```bash
docker run -it --rm \
  --network food-delivery-pipeline_food-delivery-net \
  -v $(pwd)/scripts:/scripts \
  python:3.9-slim bash
```

Then inside the container:
```bash
pip install kafka-python numpy
python /scripts/generate_kitchen_stream.py
```

**Note**: The network name must match exactly: `food-delivery-pipeline_food-delivery-net`

---

## Quick Check: Verify Network Name

```bash
docker network ls | grep food-delivery
```

You should see: `food-delivery-pipeline_food-delivery-net`

---

## Verify Kafka is Accessible

From within the airflow-webserver container:
```bash
docker exec airflow-webserver python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers='kafka:9092'); print('✅ Kafka accessible')"
```

