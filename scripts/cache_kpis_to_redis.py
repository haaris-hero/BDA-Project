#!/usr/bin/env python3
"""
Cache KPIs from MongoDB to Redis
This script reads KPI collections from MongoDB and caches them to Redis
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
                        "computed_at": datetime.now().isoformat(),
                        "record_count": len(documents)
                    }
                    
                    redis_client.setex(redis_key, 120, json.dumps(cache_data))
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
            return True
        else:
            print("⚠️  No KPI collections found to cache")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = cache_kpis_to_redis()
    sys.exit(0 if success else 1)

