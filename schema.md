# Food Delivery Pipeline - Data Schema & Dictionary

## Business Context
Real-time monitoring system for food delivery operations with focus on:
- Kitchen load and preparation efficiency
- Rider availability and performance
- Order delivery metrics and customer satisfaction

---

## FACT TABLES (KPIs)

### 1. kitchen_fact
**Purpose**: Track kitchen performance and order preparation

| Column | Type | Description |
|--------|------|-------------|
| order_id | String | Unique order identifier |
| restaurant_id | String | Restaurant reference |
| chef_id | String | Chef identifier |
| items_count | Integer | Number of items in order (1-15) |
| prep_start_time | Timestamp | When preparation started |
| prep_end_time | Timestamp | When preparation finished |
| prep_delay_minutes | Float | Actual delay vs expected time |
| predicted_prep_delay | Float | ML prediction of delay |
| priority_flag | Boolean | High-priority order flag |
| order_type | String | "delivery" or "dine-in" |
| timestamp | Timestamp | Event timestamp |

**Key Metrics**:
- `prep_delay_minutes` (avg, max, p95)
- `active_orders` (count per restaurant)
- `avg_items_per_order` (efficiency indicator)

---

### 2. rider_fact
**Purpose**: Track rider performance and location

| Column | Type | Description |
|--------|------|-------------|
| rider_id | String | Unique rider identifier |
| zone_id | String | Service zone (NORTH, SOUTH, etc) |
| rider_location.latitude | Float | Current latitude (-74 to -73.9) |
| rider_location.longitude | Float | Current longitude (40.7 to 40.8) |
| rider_status | String | idle/assigned/pickup/enroute |
| traffic_delay_minutes | Float | Current traffic delay (0-30 min) |
| pickup_delay_minutes | Float | Time to reach restaurant |
| dropoff_delay_minutes | Float | Time for delivery |
| distance_to_restaurant_km | Float | Distance (0-15 km) |
| trip_count_today | Integer | Trips completed today |
| idle_time_minutes | Integer | Consecutive idle time |
| timestamp | Timestamp | Event timestamp |

**Key Metrics**:
- `efficiency_score` = 100 - (avg_idle*5) - (traffic_delay*2)
- `avg_pickup_delay` per zone
- `traffic_delay` correlation with delivery time

---

### 3. orders_fact
**Purpose**: Track order lifecycle and revenue

| Column | Type | Description |
|--------|------|-------------|
| order_id | String | Unique order identifier |
| customer_id | String | Customer reference |
| restaurant_id | String | Restaurant reference |
| zone_id | String | Delivery zone |
| food_category | String | Food type (Biryani, Pizza, etc) |
| order_value | Float | Original order amount (₹200-2000) |
| discount_amount | Float | Discount given (15% if applicable) |
| final_amount | Float | Amount paid |
| payment_type | String | credit_card/debit/upi/wallet |
| estimated_delivery_minutes | Float | Original estimate |
| actual_delivery_minutes | Float | Actual delivery time |
| delivery_delay_minutes | Float | actual - estimated |
| cancellation_probability | Float | ML risk score (0-1) |
| is_cancelled | Boolean | Order cancelled status |
| customer_rating | Float | 1-5 star rating (if delivered) |
| timestamp | Timestamp | Order creation time |

**Key Metrics**:
- `order_count` per zone/category
- `zone_revenue` = sum(final_amount)
- `avg_delivery_delay` by zone
- `cancellation_rate` by delay severity
- `average_rating` by restaurant

---

## DIMENSION TABLES (Join Keys)

### dim_customer
| Column | Type | Description |
|--------|------|-------------|
| customer_id | String (PK) | Unique customer |
| customer_segment | String | premium/regular/budget |
| registration_date | Date | Customer onboarding |
| total_orders | Integer | Lifetime orders |
| preferred_category | String | Food type preference |
| avg_order_value | Float | Historical average |

### dim_restaurant
| Column | Type | Description |
|--------|------|-------------|
| restaurant_id | String (PK) | Unique restaurant |
| restaurant_name | String | Brand name |
| cuisine_type | String | Food category |
| zone_id | String | Primary service zone |
| kitchen_capacity | Integer | Max parallel orders |
| avg_prep_time | Float | Baseline prep time |
| rating | Float | Restaurant rating |
| active_status | Boolean | Currently accepting orders |

### dim_rider
| Column | Type | Description |
|--------|------|-------------|
| rider_id | String (PK) | Unique rider |
| vehicle_type | String | bike/scooter/car |
| zone_id | String | Primary zone |
| joining_date | Date | When rider onboarded |
| total_deliveries | Integer | Lifetime trips |
| avg_rating | Float | Rider rating |
| documents_verified | Boolean | KYC status |

### dim_zone
| Column | Type | Description |
|--------|------|-------------|
| zone_id | String (PK) | Zone identifier |
| zone_name | String | Display name |
| area_km2 | Float | Geographic area |
| avg_traffic_level | String | high/medium/low |
| restaurants_count | Integer | Active restaurants |
| riders_count | Integer | Available riders |
| peak_hours | Array | Hours with high demand |

### dim_time
| Column | Type | Description |
|--------|------|-------------|
| timestamp | Timestamp (PK) | Event time |
| hour | Integer | Hour of day (0-23) |
| day_of_week | Integer | 1=Monday, 7=Sunday |
| is_peak_hour | Boolean | 12-2 PM or 7-10 PM |
| is_holiday | Boolean | Public holiday flag |
| season | String | winter/summer/monsoon |

### dim_food_category
| Column | Type | Description |
|--------|------|-------------|
| category_id | String (PK) | Category code |
| category_name | String | Food type |
| avg_prep_time | Float | Typical prep duration |
| avg_order_value | Float | Historical average |
| popularity_rank | Integer | Popularity score |

---

## Archiving Policy

**Strategy**: HOT → COLD Storage Transition

**Trigger**: Database size > 300 MB

**Workflow**:
1. Identify data older than 1 hour
2. Export to JSONL format
3. Convert to Parquet (columnar compression)
4. Upload to HDFS: `/archive/food_delivery/YYYY/MM/DD/`
5. Insert metadata record in `archive_metadata`
6. Delete from MongoDB

**Metadata Stored** (`archive_metadata` collection):
```json
{
  "collection": "kitchen_events",
  "hdfs_path": "/archive/food_delivery/2024/01/15/kitchen_1705363200.parquet",
  "record_count": 5000,
  "size_mb": 5.2,
  "archived_at": "2024-01-15T10:30:00Z",
  "schema_version": "1.0"
}


# In separate terminals (or use tmux)
docker exec food-delivery-pipeline_kafka_1 python scripts/generate_kitchen_stream.py
docker exec food-delivery-pipeline_kafka_1 python scripts/generate_rider_stream.py
docker exec food-delivery-pipeline_kafka_1 python scripts/generate_orders_stream.py


3. Access Services
Service	URL	Credentials
Airflow	http://localhost:8888	admin / admin
Superset	http://localhost:8089	admin / admin
Spark Master	http://localhost:8080	-
Mongo	mongo:27017	root / password123
Hadoop HDFS	http://localhost:50070	-


# Ingest DAG
airflow dags trigger ingest_streams_to_mongo

# Archive DAG
airflow dags trigger archive_old_data

# Spark Analytics DAG
airflow dags trigger spark_transform_and_aggregate

commands to verify and run 

# Check status of all containers
docker-compose ps

# Expected output: All services should show "Up" or "Up (healthy)"
# If any show "Exit" or "Restarting", run: docker-compose logs <service_name>


# Check MongoDB is healthy
docker exec mongo mongosh -u root -p password123 --eval "db.adminCommand('ping')"
# Expected: { ok: 1 }

# Check Kafka is running
docker exec kafka /opt/confluent/bin/kafka-topics --list --bootstrap-server kafka:9092
# Expected: (empty list is OK, topics will be created)

# Check Airflow webserver is ready
curl -s http://localhost:8888/health | grep -o '"status":"healthy"'
# Expected: "status":"healthy"

# Check Spark master is running
curl -s http://localhost:8080 | grep -q "Spark Master" && echo "✅ Spark OK"


# Create 3 topics for streaming data
cd /home/haaris/food-delivery-pipeline

# Use bash -c with kafka-topics (without full path)
docker-compose exec kafka bash -c "
  kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --topic kitchen_stream \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
"

docker-compose exec kafka bash -c "
  kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --topic rider_stream \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
"

docker-compose exec kafka bash -c "
  kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --topic orders_stream \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
"

# Verify topics were created
docker-compose exec kafka bash -c "kafka-topics --list --bootstrap-server kafka:9092"

# Expected output:
# kitchen_stream
# orders_stream
# rider_stream


# Install required packages for data generators
docker exec airflow-webserver pip install -q kafka-python pymongo faker numpy python-dotenv



# Create scripts directory
mkdir -p /home/haaris/food-delivery-pipeline/scripts

# Now copy the generator scripts (they should already exist from Phase 2)
# If they don't exist, create them from the files provided earlier
ls -la /home/haaris/food-delivery-pipeline/scripts/
# Expected: 3 files listed (generate_kitchen_stream.py, generate_rider_stream.py, generate_orders_stream.py)




cd /home/haaris/food-delivery-pipeline
docker exec -it airflow-webserver python scripts/generate_kitchen_stream.py

# Expected output (continuously):
# [KITCHEN] ORD1705363200001 sent to Kafka
# [KITCHEN] ORD1705363200002 sent to Kafka
# ... (one every 2-6 seconds)



cd /home/haaris/food-delivery-pipeline
docker exec -it airflow-webserver python scripts/generate_rider_stream.py

# Expected output (continuously):
# [RIDER] RIDER0001 sent to Kafka
# [RIDER] RIDER0002 sent to Kafka
# ... (one every 1-3 seconds)



cd /home/haaris/food-delivery-pipeline
docker exec -it airflow-webserver python scripts/generate_orders_stream.py

# Expected output (continuously):
# [ORDER] ORD1705363200001 sent to Kafka
# [ORDER] ORD1705363200002 sent to Kafka
# ... (one every 2-5 seconds)



# Install tmux (if not present)
sudo apt-get install -y tmux

# Create new tmux session
tmux new-session -d -s food-delivery

# Create 3 windows
tmux new-window -t food-delivery -n kitchen
tmux new-window -t food-delivery -n rider
tmux new-window -t food-delivery -n orders

# Send commands to each window
tmux send-keys -t food-delivery:kitchen "cd /home/haaris/food-delivery-pipeline && docker exec -it airflow-webserver python scripts/generate_kitchen_stream.py" Enter
tmux send-keys -t food-delivery:rider "cd /home/haaris/food-delivery-pipeline && docker exec -it airflow-webserver python scripts/generate_rider_stream.py" Enter
tmux send-keys -t food-delivery:orders "cd /home/haaris/food-delivery-pipeline && docker exec -it airflow-webserver python scripts/generate_orders_stream.py" Enter

# View tmux sessions
tmux list-sessions

# Attach to session to see logs
tmux attach -t food-delivery
# Press Ctrl+B then arrow keys to switch windows



# Listen to kitchen_stream topic (will show last message)
docker exec kafka /opt/confluent/bin/kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic kitchen_stream \
  --from-beginning \
  --max-messages 1

# Expected: JSON message with order_id, restaurant_id, etc.

# Listen to rider_stream
docker exec kafka /opt/confluent/bin/kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic rider_stream \
  --from-beginning \
  --max-messages 1

# Listen to orders_stream
docker exec kafka /opt/confluent/bin/kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic orders_stream \
  --from-beginning \
  --max-messages 1



  # Connect to MongoDB
docker exec mongo mongosh -u root -p password123 food_delivery --eval "show collections"

# If collections don't exist yet, create them manually:
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.createCollection('kitchen_events');
  db.createCollection('rider_events');
  db.createCollection('orders_events');
  db.createCollection('archive_metadata');
  show collections;
"


# Open browser and go to:
# http://localhost:8888
# Login: admin / admin

# In UI:
# 1. Find "ingest_streams_to_mongo" DAG in the list
# 2. Click the toggle button to ENABLE it (should turn blue)
# 3. Click the "Trigger DAG" button (play icon)
# 4. Wait 2 minutes for it to run



# Unpause the DAG
docker exec airflow-scheduler airflow dags unpause ingest_streams_to_mongo

# Trigger it immediately
docker exec airflow-scheduler airflow dags trigger ingest_streams_to_mongo

# Check if it's running
docker exec airflow-scheduler airflow dags list --grep ingest



# Check if documents are being inserted
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.kitchen_events.countDocuments()
"
# Expected: Should increase (e.g., 100, 200, 300...)

docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.rider_events.countDocuments()
"
# Expected: Should increase

docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.orders_events.countDocuments()
"
# Expected: Should increase

# View a sample document
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.kitchen_events.findOne()
"



# Enable the archive DAG
docker exec airflow-scheduler airflow dags unpause archive_old_data

# This will run every 15 minutes automatically
# You can verify in logs:
docker exec airflow-scheduler airflow tasks logs archive_old_data



# Ensure spark_jobs directory exists in container
docker exec spark-master mkdir -p /opt/spark-apps

# Copy the compute_kpis.py file
docker cp /home/haaris/food-delivery-pipeline/spark_jobs/compute_kpis.py spark-master:/opt/spark-apps/



# Enable the Spark DAG
docker exec airflow-scheduler airflow dags unpause spark_compute_kpis

# Trigger it
docker exec airflow-scheduler airflow dags trigger spark_compute_kpis

# Check logs
docker exec airflow-scheduler airflow tasks logs spark_compute_kpis compute_kpis_spark



# Check if KPI collections were created
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  show collections
"
# Should see: kpi_kitchen_load, kpi_rider_efficiency, kpi_zone_demand

# View KPI results
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.kpi_kitchen_load.findOne()
"

docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.kpi_rider_efficiency.find().pretty()
"



# Open browser
# http://localhost:8089
# Login: admin / admin

# Click "+ DATABASE" to add MongoDB connection
# Connection String: mongodb+srv://root:password123@mongo:27017/food_delivery
# Or: mongodb://root:password123@mongo:27017/food_delivery

# Create datasets from:
# - kpi_kitchen_load
# - kpi_rider_efficiency
# - kpi_zone_demand

# Create dashboard with charts:
# - Bar chart: active_orders by restaurant
# - Gauge chart: avg_prep_delay
# - Pie chart: zone_revenue
# - Scatter: rider efficiency score




# Run this every 30 seconds to see updates
watch -n 5 'docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  print(\"Kitchen events: \" + db.kitchen_events.countDocuments());
  print(\"Rider events: \" + db.rider_events.countDocuments());
  print(\"Orders events: \" + db.orders_events.countDocuments());
  print(\"Last update: \" + new Date());
"'



# http://localhost:8888/dags
# Refresh page every 60 seconds to see dag runs



# http://localhost:8089/superset/dashboard/
# Refresh every 60 seconds to see updated KPIs



complete command sequence

# 1. Verify services
docker-compose ps

# 2. Create Kafka topics
docker exec kafka /opt/confluent/bin/kafka-topics --create --bootstrap-server kafka:9092 --topic kitchen_stream --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/confluent/bin/kafka-topics --create --bootstrap-server kafka:9092 --topic rider_stream --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka /opt/confluent/bin/kafka-topics --create --bootstrap-server kafka:9092 --topic orders_stream --partitions 3 --replication-factor 1 --if-not-exists

# 3. Install dependencies
docker exec airflow-webserver pip install -q kafka-python pymongo faker numpy

# 4. Create MongoDB collections
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.createCollection('kitchen_events');
  db.createCollection('rider_events');
  db.createCollection('orders_events');
  db.createCollection('archive_metadata');
"

# 5. Enable Airflow DAGs
docker exec airflow-scheduler airflow dags unpause ingest_streams_to_mongo
docker exec airflow-scheduler airflow dags unpause archive_old_data
docker exec airflow-scheduler airflow dags unpause spark_compute_kpis

# 6. Start data generators (in 3 separate terminals or tmux)
# Terminal 1:
docker exec -it airflow-webserver python scripts/generate_kitchen_stream.py
# Terminal 2:
docker exec -it airflow-webserver python scripts/generate_rider_stream.py
# Terminal 3:
docker exec -it airflow-webserver python scripts/generate_orders_stream.py

# 7. Trigger DAGs manually (after generators are running)
docker exec airflow-scheduler airflow dags trigger ingest_streams_to_mongo
docker exec airflow-scheduler airflow dags trigger spark_compute_kpis

# 8. Monitor data flow
docker exec mongo mongosh -u root -p password123 food_delivery --eval "
  db.kitchen_events.countDocuments();
  db.rider_events.countDocuments();
  db.orders_events.countDocuments();
"