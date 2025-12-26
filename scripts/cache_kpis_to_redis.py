#!/usr/bin/env python3
"""
Cache KPIs from MongoDB to Redis and upsert to Superset Postgres
This script reads KPI collections from MongoDB, caches them to Redis,
and writes/updates a small `kpi_metrics` table in Superset's Postgres so
Superset dashboards can query live KPI values.
"""

import json
from datetime import datetime
from pymongo import MongoClient
import redis
import sys

def cache_kpis_to_redis():
    """Read KPIs from MongoDB and cache to Redis"""
    try:
        # Connect to MongoDB
        mongo_client = MongoClient('mongodb://root:password123@mongo:27017/')
        db = mongo_client['food_delivery']
        
        # Connect to Redis
        redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        
        print("✅ Connected to MongoDB and Redis")
        
        # KPI collections to cache
        kpi_collections = {
            'kpi_kitchen_load': 'kpi:kitchen_load',
            'kpi_rider_efficiency': 'kpi:rider_efficiency',
            'kpi_zone_demand': 'kpi:zone_demand',
            'kpi_restaurant_performance': 'kpi:restaurant_performance'
        }
        
        cached_payloads = {}
        cached_count = 0
        
        for mongo_collection, redis_key in kpi_collections.items():
            try:
                # Get all documents from MongoDB collection
                collection = db[mongo_collection]
                documents = list(collection.find({}, {'_id': 0}))
                
                if documents:
                    # Cache to Redis with 120 second TTL
                    cache_data = {
                        "data": documents,
                        "computed_at": datetime.utcnow().isoformat() + 'Z',
                        "record_count": len(documents)
                    }
                    
                    redis_client.setex(redis_key, 120, json.dumps(cache_data))
                    cached_payloads[redis_key] = cache_data
                    cached_count += 1
                    print(f"✅ Cached {len(documents)} records from {mongo_collection} to {redis_key}")
                else:
                    print(f"⚠️  No data found in {mongo_collection}")
                    
            except Exception as e:
                print(f"⚠️  Error caching {mongo_collection}: {e}")
                continue
        
        mongo_client.close()
        
        if cached_count > 0:
            print(f"✅ Successfully cached {cached_count} KPI collections to Redis")
            # attempt to upsert into Postgres for Superset
            try:
                upsert_kpis_to_postgres(cached_payloads)
            except Exception as e:
                print("⚠️  Postgres upsert error:", e)
            return True
        else:
            print("⚠️  No KPI collections found to cache")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

# --- Postgres upsert for Superset dashboards ---
def upsert_kpis_to_postgres(kpi_payloads):
    """
    Upsert KPI payloads into superset Postgres (superset metadata DB).
    Creates a small table `kpi_metrics(metric_name TEXT PRIMARY KEY, value_json JSONB, value_num DOUBLE PRECISION, updated_at TIMESTAMP)`.
    """
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except Exception as e:
        print("⚠️  psycopg2 missing:", e)
        return

    conn = None
    try:
        conn = psycopg2.connect(
            dbname="superset",
            user="superset",
            password="superset",
            host="superset-db",
            port=5432
        )
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_metrics (
            metric_name TEXT PRIMARY KEY,
            value_json JSONB,
            value_num DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT now()
        );
        """)
        rows = []
        for metric_name, payload in kpi_payloads.items():
            # try to extract a simple numeric value if payload contains a single numeric metric,
            # otherwise leave value_num as NULL. This helps building simple KPI cards.
            value_num = None
            try:
                data = payload.get('data')
                if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
                    for v in data[0].values():
                        if isinstance(v, (int, float)):
                            value_num = float(v)
                            break
            except Exception:
                value_num = None

            rows.append( (metric_name, json.dumps(payload), value_num) )

        execute_values(cur,
            """
            INSERT INTO kpi_metrics(metric_name, value_json, value_num, updated_at)
            VALUES %s
            ON CONFLICT (metric_name) DO UPDATE
            SET value_json = EXCLUDED.value_json,
                value_num = EXCLUDED.value_num,
                updated_at = now()
            """,
            rows
        )
        conn.commit()
        cur.close()
        print("✅ KPIs upserted to Postgres for Superset")
    except Exception as e:
        print("⚠️  Postgres upsert failed:", e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = cache_kpis_to_redis()
    sys.exit(0 if success else 1)

