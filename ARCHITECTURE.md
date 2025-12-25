# Real-Time Big Data Analytics Architecture
## Food Delivery Pipeline - Preventing Service Delays & Stockouts

---

## Overview

This document describes the complete architecture for a real-time Big Data Analytics (BDA) pipeline designed to monitor food delivery operations and prevent service delays through live dashboard visualization.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION LAYER                            │
│                                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │   Kitchen    │  │    Rider     │  │    Orders    │                 │
│  │   Stream     │  │   Stream     │  │   Stream     │                 │
│  │  Generator   │  │  Generator   │  │  Generator   │                 │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                 │
│         │                  │                  │                          │
│         └──────────────────┴──────────────────┘                          │
│                            │                                               │
│                            ▼                                               │
│                    ┌──────────────┐                                       │
│                    │    Kafka     │                                       │
│                    │   (Topics)   │                                       │
│                    └──────┬───────┘                                       │
└───────────────────────────┼───────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME PROCESSING & ARCHIVAL                      │
│                                                                           │
│  ┌──────────────────────────────────────────────────────┐              │
│  │              Airflow Orchestration                    │              │
│  │  • Schedule ingestion (every 1 min)                   │              │
│  │  • Trigger Spark jobs (every 1 min)                  │              │
│  │  • Archive old data (every 30 min)                   │              │
│  └──────────────────────────────────────────────────────┘              │
│                            │                                               │
│         ┌──────────────────┼──────────────────┐                           │
│         │                  │                  │                           │
│         ▼                  ▼                  ▼                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                     │
│  │   MongoDB    │  │    Spark    │  │   Hadoop    │                     │
│  │  (Hot Data)  │  │ (Analytics) │  │    HDFS     │                     │
│  │   ~300MB     │  │  (KPIs)     │  │  (Archive)  │                     │
│  └──────┬───────┘  └──────┬───────┘  └─────────────┘                     │
│         │                 │                                                │
│         │                 │                                                │
│         └────────┬────────┘                                                │
│                  │                                                         │
│                  ▼                                                         │
│         ┌──────────────┐                                                  │
│         │   Dimension   │                                                  │
│         │    Tables     │                                                  │
│         │  (Joins)      │                                                  │
│         └──────────────┘                                                  │
└───────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          CACHING LAYER                                    │
│                                                                           │
│                    ┌──────────────┐                                      │
│                    │    Redis     │                                      │
│                    │   (Cache)    │                                      │
│                    │  TTL: 120s   │                                      │
│                    └──────┬───────┘                                      │
└───────────────────────────┼───────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DASHBOARD VISUALIZATION                                │
│                                                                           │
│                    ┌──────────────┐                                      │
│                    │   Superset    │                                      │
│                    │  Dashboard    │                                      │
│                    │  (Live 1min)  │                                      │
│                    └───────────────┘                                      │
│                                                                           │
│  • Kitchen Load Monitoring                                                │
│  • Rider Efficiency Scores                                                │
│  • Zone-Wise Demand Trends                                                │
│  • Revenue Metrics                                                        │
│  • Cancellation Risk Analysis                                             │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Data Ingestion Layer

**Components:**
- **Kafka**: Message broker for streaming data
- **3 Stream Generators**: Python scripts generating realistic statistical data
  - `generate_kitchen_stream.py`: Kitchen events (every 2-6 seconds)
  - `generate_rider_stream.py`: Rider location/status (every 1-3 seconds)
  - `generate_orders_stream.py`: Order events (every 2-5 seconds)

**Topics:**
- `kitchen_stream`
- `rider_stream`
- `orders_stream`

---

### 2. Real-Time Processing & Archival

#### MongoDB (Hot Storage)
- **Purpose**: Stores fresh streaming data (~5-15 min window)
- **Characteristics**:
  - High write throughput
  - Flexible schemas
  - Short-term view only
  - Size limit: 300 MB

**Collections:**
- `kitchen_events`
- `rider_events`
- `orders_events`
- `kpi_*` (computed KPIs)

#### Spark (OLAP Analytics)
- **Purpose**: Reads MongoDB, joins dimension tables, computes minute-level KPIs
- **Capabilities**:
  - OLAP-style analytics
  - SQL-like queries
  - Requires dimension joins
  - Minute-level refresh

**KPIs Computed:**
1. Real-Time Kitchen Load (with restaurant dimension join)
2. Rider Efficiency Score (with rider and zone dimension joins)
3. Zone-Wise Demand Trend (with zone and food category joins)
4. Restaurant Performance (with restaurant dimension join)
5. Revenue Metrics by Time Window
6. Cancellation Risk Analysis

#### Hadoop HDFS (Cold Storage)
- **Purpose**: Historical partitioned data archive
- **Archiving Policy**:
  - Trigger: MongoDB size > 300 MB
  - Export data older than 1 hour
  - Format: JSONL (compressed)
  - Path: `/archive/food_delivery/YYYY/MM/DD/`
  - Metadata stored in `archive_metadata` collection

#### Airflow Orchestration
- **DAGs**:
  1. `ingest_streams_to_mongo`: Runs every 1 minute
  2. `spark_compute_kpis`: Runs every 1 minute
  3. `archive_old_data`: Runs every 30 minutes

**Why Orchestration?**
- DAGs schedule ingestion, OLAP, and archival
- Trigger Spark jobs every minute
- Execute archive DAGs when threshold exceeded
- Mongo stays lean, HDFS holds historical, partitioned data

---

### 3. Caching Layer

#### Redis Cache
- **Purpose**: Caches latest KPIs for fast dashboard queries & reduced MongoDB load
- **Benefits**:
  - Avoids recomputation
  - Enables low-latency access
  - Real-time readiness
- **TTL**: 120 seconds (2 minutes)
- **Keys**:
  - `kpi:kitchen_load`
  - `kpi:rider_efficiency`
  - `kpi:zone_demand`
  - `kpi:restaurant_performance`

---

### 4. Dashboard Visualization

#### Apache Superset
- **Purpose**: Live dashboard showing real-time KPIs
- **Refresh Rate**: Every 1 minute
- **Data Sources**: MongoDB (via Redis cache when available)

**Dashboard Capabilities:**
- Spot high-risk products before stockouts
- Identify where delays are happening (e.g., specific zones/restaurants)
- Decide action: restock / pause ads / reroute fulfillment

---

## Data Flow

1. **Data Generation**: Stream generators produce events → Kafka topics
2. **Ingestion**: Airflow DAG consumes Kafka → MongoDB (hot storage)
3. **Processing**: Spark reads MongoDB → joins dimensions → computes KPIs
4. **Caching**: KPIs written to MongoDB → cached in Redis
5. **Archival**: When MongoDB > 300MB → export to HDFS → delete from MongoDB
6. **Visualization**: Superset reads from MongoDB/Redis → displays on dashboard

---

## Dimension Tables (for Joins)

1. **dim_restaurant**: Restaurant metadata (capacity, cuisine, zone)
2. **dim_rider**: Rider metadata (vehicle, zone, ratings)
3. **dim_zone**: Zone metadata (traffic, area, peak hours)
4. **dim_food_category**: Food category metadata (prep time, popularity)

---

## Fact Tables (KPIs)

1. **kitchen_fact**: Prep delays, active orders, items prepared
2. **rider_fact**: Pickup delays, travel time, idle time, traffic delays
3. **orders_fact**: Order value, delivery time, cancellation probability, ratings

---

## Technology Stack

- **Orchestration**: Apache Airflow
- **Streaming**: Apache Kafka
- **Hot Storage**: MongoDB
- **Cold Storage**: Hadoop HDFS
- **Processing**: Apache Spark
- **Caching**: Redis
- **Visualization**: Apache Superset
- **Containerization**: Docker & Docker Compose

---

## Key Features

✅ **Real-Time Processing**: 1-minute refresh cycle
✅ **Dimensional Modeling**: Proper fact/dimension table structure
✅ **Join-Based Queries**: SQL joins for complex analytics
✅ **Archiving Policy**: Automatic archival when threshold exceeded
✅ **Caching Layer**: Redis for fast dashboard access
✅ **Live Dashboard**: Superset with minute-level updates
✅ **Fully Dockerized**: All components containerized

---

## Business Problem Justification

Food delivery platforms face three critical real-time bottlenecks:

1. **Kitchen Overload**: Orders spike during lunch/dinner. Without instant detection → excess preparation delay → poor customer experience → cancellations.

2. **Rider Availability Mismatch**: Some zones have too many orders and too few riders → long pickup delays → longer delivery times → revenue loss.

3. **Unpredictable Delays**: Traffic congestion, rider idle time, slow kitchens, weather. Food delivery is a real-time coordination problem → managers need dashboards that update every minute.

This architecture enables:
- **Proactive Risk Detection**: Identify high-risk zones/restaurants before problems escalate
- **Resource Optimization**: Reallocate riders based on real-time demand
- **Performance Monitoring**: Track KPIs across multiple dimensions (zone, restaurant, time)
- **Data-Driven Decisions**: Make informed decisions about restocking, ad pausing, fulfillment routing

---

## Schema Requirements Met

✅ **5+ Numerical Facts (KPIs)**:
- prep_delay_minutes
- active_orders
- efficiency_score
- zone_revenue
- delivery_delay_minutes
- cancellation_rate
- revenue_1min
- avg_rating

✅ **5-10 Dimensions**:
- dim_restaurant (restaurant_id, zone_id, cuisine_type)
- dim_rider (rider_id, zone_id, vehicle_type)
- dim_zone (zone_id, avg_traffic_level)
- dim_food_category (category_name, avg_prep_time)
- dim_time (timestamp, hour, is_peak_hour)

✅ **Join-Based Queries**: All KPIs use SQL joins with dimension tables

✅ **300MB Data Size**: MongoDB maintains ~300MB, older data archived to HDFS

---

## Deployment

All components are dockerized and can be started with:

```bash
docker-compose up -d
```

Services:
- Airflow: http://localhost:8888 (admin/admin)
- Superset: http://localhost:8090 (admin/admin)
- Spark Master: http://localhost:8080
- Hadoop HDFS: http://localhost:50070
- Redis: localhost:6379
- MongoDB: localhost:27017

---

## Next Steps

1. Copy `scripts/compute_kpis_spark.py` to `spark_jobs/compute_kpis.py`
2. Initialize dimension tables: `python scripts/init_dimension_tables.py`
3. Start data generators
4. Enable Airflow DAGs
5. Configure Superset dashboards
6. Monitor live updates

---



