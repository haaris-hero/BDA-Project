# Setup Instructions - Food Delivery Pipeline

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available
- Ports available: 8888, 8090, 8080, 50070, 6379, 27017, 9092

---

## Step 1: Fix Spark Jobs Directory Permissions

The `spark_jobs` directory may have permission issues. Fix it:

```bash
cd /home/haaris/food-delivery-pipeline
sudo chown -R $USER:$USER spark_jobs
```

---

## Step 2: Copy Spark Job File

Copy the Spark job file to the correct location:

```bash
cp scripts/compute_kpis_spark.py spark_jobs/compute_kpis.py
chmod +x spark_jobs/compute_kpis.py
```

---

## Step 3: Start Docker Containers

```bash
cd /home/haaris/food-delivery-pipeline
docker-compose up -d
```

Wait for all services to be healthy (check with `docker-compose ps`).

---

## Step 4: Create Kafka Topics

```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic kitchen_stream \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic rider_stream \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic orders_stream \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# Verify topics
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

---

## Step 5: Initialize Dimension Tables

```bash
docker exec airflow-webserver pip install redis pymongo
docker exec airflow-webserver python scripts/init_dimension_tables.py
```

---

## Step 6: Create MongoDB Collections

```bash
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.createCollection('kitchen_events');
  db.createCollection('rider_events');
  db.createCollection('orders_events');
  db.createCollection('archive_metadata');
  show collections;
"
```

---

## Step 7: Start Data Generators

Open 3 separate terminal windows:

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

Keep all 3 terminals running!

---

## Step 8: Enable Airflow DAGs

```bash
# Enable ingestion DAG
docker exec airflow-scheduler airflow dags unpause ingest_streams_to_mongo

# Enable Spark KPI computation DAG
docker exec airflow-scheduler airflow dags unpause spark_compute_kpis

# Enable archive DAG
docker exec airflow-scheduler airflow dags unpause archive_old_data

# Trigger ingestion manually (optional)
docker exec airflow-scheduler airflow dags trigger ingest_streams_to_mongo

# Trigger Spark job manually (optional)
docker exec airflow-scheduler airflow dags trigger spark_compute_kpis
```

---

## Step 9: Verify Data Flow

**Check MongoDB:**
```bash
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  print('Kitchen events: ' + db.kitchen_events.countDocuments());
  print('Rider events: ' + db.rider_events.countDocuments());
  print('Orders events: ' + db.orders_events.countDocuments());
"
```

**Check KPIs:**
```bash
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  show collections;
  db.kpi_kitchen_load.findOne();
"
```

**Check Redis Cache:**
```bash
docker exec redis redis-cli GET "kpi:kitchen_load"
```

---

## Step 10: Configure Superset Dashboard

1. Open browser: http://localhost:8090
2. Login: admin / admin
3. Add MongoDB database:
   - Click "+" → "Data" → "Connect database"
   - Select "MongoDB"
   - Connection string: `mongodb://root:password123@mongo:27017/food_delivery`
   - Click "Connect"
4. Create datasets from KPI collections:
   - `kpi_kitchen_load`
   - `kpi_rider_efficiency`
   - `kpi_zone_demand`
   - `kpi_restaurant_performance`
   - `kpi_revenue_metrics`
   - `kpi_cancellation_risk`
5. Create charts:
   - Bar chart: Active orders by restaurant
   - Gauge chart: Average prep delay
   - Pie chart: Zone revenue distribution
   - Line chart: Revenue over time
   - Scatter plot: Rider efficiency scores
6. Create dashboard and add charts
7. Set auto-refresh to 60 seconds

---

## Step 11: Monitor Live Updates

**Monitor MongoDB growth:**
```bash
watch -n 5 'docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  var k = db.kitchen_events.countDocuments();
  var r = db.rider_events.countDocuments();
  var o = db.orders_events.countDocuments();
  print(\"K:\" + k + \" | R:\" + r + \" | O:\" + o + \" | Total:\" + (k+r+o));
"'
```

**Monitor Airflow DAGs:**
- Open http://localhost:8888
- Check DAG runs every minute

**Monitor Superset Dashboard:**
- Open http://localhost:8090
- Refresh dashboard every 60 seconds to see updates

---

## Troubleshooting

### Spark Job Not Found
```bash
# Verify file exists
docker exec spark-master ls -la /opt/spark-apps/

# Copy file manually if needed
docker cp spark_jobs/compute_kpis.py spark-master:/opt/spark-apps/
```

### Redis Connection Error
```bash
# Check Redis is running
docker exec redis redis-cli PING

# Should return: PONG
```

### MongoDB Connection Issues
```bash
# Check MongoDB is accessible
docker exec mongo mongosh -u root -p password123 --eval "db.adminCommand('ping')"

# Should return: { ok: 1 }
```

### HDFS Archiving Not Working
```bash
# Check HDFS is accessible
docker exec hadoop-namenode hdfs dfs -ls /

# Create archive directory manually if needed
docker exec hadoop-namenode hdfs dfs -mkdir -p /archive/food_delivery
```

---

## Architecture Overview

See `ARCHITECTURE.md` for complete architecture documentation.

---

## Key Features Implemented

✅ Redis caching for fast dashboard access
✅ Proper HDFS archiving with metadata
✅ Dimension tables for join-based queries
✅ Spark job with dimension table joins
✅ Timestamp field consistency
✅ Live dashboard updates every minute
✅ Complete dockerization

---

## What's Left to Implement

1. **Spark Job File**: Copy `scripts/compute_kpis_spark.py` to `spark_jobs/compute_kpis.py`
2. **Superset Redis Integration**: Configure Superset to use Redis cache (optional optimization)
3. **Architecture Diagram**: See `ARCHITECTURE.md` for visual representation

---

## Success Checklist

- [ ] All containers running (`docker-compose ps`)
- [ ] Kafka topics created
- [ ] MongoDB collections created
- [ ] Dimension tables initialized
- [ ] 3 data generators running
- [ ] Airflow DAGs enabled and running
- [ ] Spark job executing successfully
- [ ] KPIs appearing in MongoDB
- [ ] Redis cache populated
- [ ] Superset dashboard configured
- [ ] Live updates visible every minute

---

*For detailed architecture information, see `ARCHITECTURE.md`*

