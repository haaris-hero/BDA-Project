#!/usr/bin/env python3
"""
Initialize dimension tables in MongoDB
These tables are used for joins in Spark SQL queries
"""

from pymongo import MongoClient
from datetime import datetime
import random

def init_dimension_tables():
    """Initialize all dimension tables"""
    
    client = MongoClient('mongodb://root:password123@mongo:27017/')
    db = client['food_delivery']
    
    print("📊 Initializing dimension tables...")
    
    # Clear existing dimension tables
    db.dim_restaurant.delete_many({})
    db.dim_rider.delete_many({})
    db.dim_zone.delete_many({})
    db.dim_food_category.delete_many({})
    db.dim_time.delete_many({})
    
    # ===== dim_restaurant =====
    restaurants = []
    cuisine_types = ["Biryani", "Pizza", "Burger", "Chinese", "Italian", "Dessert"]
    zones = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
    
    for i in range(1, 51):
        restaurants.append({
            "restaurant_id": f"R{str(i).zfill(4)}",
            "restaurant_name": f"Restaurant_{i}",
            "cuisine_type": cuisine_types[i % len(cuisine_types)],
            "zone_id": zones[i % len(zones)],
            "kitchen_capacity": 10 + (i % 20),
            "avg_prep_time": 5.0 + (i % 3) * 0.5,
            "rating": 4.0 + (i % 2) * 0.5,
            "active_status": True,
            "created_at": datetime.utcnow().isoformat()
        })
    
    db.dim_restaurant.insert_many(restaurants)
    print(f"✅ Created {len(restaurants)} restaurant records")
    
    # ===== dim_rider =====
    riders = []
    vehicle_types = ["bike", "scooter", "car"]
    
    for i in range(1, 101):
        riders.append({
            "rider_id": f"RIDER{str(i).zfill(4)}",
            "vehicle_type": vehicle_types[i % len(vehicle_types)],
            "zone_id": zones[i % len(zones)],
            "joining_date": f"2024-01-{(i % 28) + 1:02d}",
            "total_deliveries": 50 + (i * 3),
            "avg_rating": 4.2 + (i % 3) * 0.3,
            "documents_verified": True,
            "created_at": datetime.utcnow().isoformat()
        })
    
    db.dim_rider.insert_many(riders)
    print(f"✅ Created {len(riders)} rider records")
    
    # ===== dim_zone =====
    zones_data = [
        {
            "zone_id": "NORTH",
            "zone_name": "North Zone",
            "area_km2": 25.5,
            "avg_traffic_level": "high",
            "restaurants_count": 12,
            "riders_count": 20,
            "peak_hours": [12, 13, 19, 20],
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "zone_id": "SOUTH",
            "zone_name": "South Zone",
            "area_km2": 30.2,
            "avg_traffic_level": "medium",
            "restaurants_count": 10,
            "riders_count": 18,
            "peak_hours": [12, 13, 20, 21],
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "zone_id": "EAST",
            "zone_name": "East Zone",
            "area_km2": 22.8,
            "avg_traffic_level": "high",
            "restaurants_count": 11,
            "riders_count": 22,
            "peak_hours": [11, 12, 19, 20],
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "zone_id": "WEST",
            "zone_name": "West Zone",
            "area_km2": 28.1,
            "avg_traffic_level": "low",
            "restaurants_count": 9,
            "riders_count": 15,
            "peak_hours": [13, 14, 20, 21],
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "zone_id": "CENTRAL",
            "zone_name": "Central Zone",
            "area_km2": 15.3,
            "avg_traffic_level": "high",
            "restaurants_count": 15,
            "riders_count": 25,
            "peak_hours": [12, 13, 19, 20, 21],
            "created_at": datetime.utcnow().isoformat()
        }
    ]
    
    db.dim_zone.insert_many(zones_data)
    print(f"✅ Created {len(zones_data)} zone records")
    
    # ===== dim_food_category =====
    categories = [
        {
            "category_id": "BIRYANI",
            "category_name": "Biryani",
            "avg_prep_time": 15.0,
            "avg_order_value": 450.0,
            "popularity_rank": 1,
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "category_id": "PIZZA",
            "category_name": "Pizza",
            "avg_prep_time": 12.0,
            "avg_order_value": 550.0,
            "popularity_rank": 2,
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "category_id": "BURGER",
            "category_name": "Burger",
            "avg_prep_time": 8.0,
            "avg_order_value": 350.0,
            "popularity_rank": 3,
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "category_id": "CHINESE",
            "category_name": "Chinese",
            "avg_prep_time": 10.0,
            "avg_order_value": 400.0,
            "popularity_rank": 4,
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "category_id": "ITALIAN",
            "category_name": "Italian",
            "avg_prep_time": 14.0,
            "avg_order_value": 500.0,
            "popularity_rank": 5,
            "created_at": datetime.utcnow().isoformat()
        },
        {
            "category_id": "DESSERT",
            "category_name": "Dessert",
            "avg_prep_time": 5.0,
            "avg_order_value": 250.0,
            "popularity_rank": 6,
            "created_at": datetime.utcnow().isoformat()
        }
    ]
    
    db.dim_food_category.insert_many(categories)
    print(f"✅ Created {len(categories)} food category records")
    
    # Create indexes for faster joins (drop existing indexes first to avoid conflicts)
    print("🔧 Creating indexes...")
    
    # Drop all non-_id indexes first, then recreate
    for collection_name in ['dim_restaurant', 'dim_rider', 'dim_zone', 'dim_food_category']:
        collection = db[collection_name]
        indexes = list(collection.list_indexes())
        for idx in indexes:
            idx_name = idx.get('name', '')
            if idx_name != '_id_':
                try:
                    collection.drop_index(idx_name)
                except:
                    pass
    
    # Create indexes
    try:
        db.dim_restaurant.create_index("restaurant_id", unique=True)
    except Exception as e:
        print(f"  Note: restaurant_id index: {e}")
    
    try:
        db.dim_rider.create_index("rider_id", unique=True)
    except Exception as e:
        print(f"  Note: rider_id index: {e}")
    
    try:
        db.dim_zone.create_index("zone_id", unique=True)
    except Exception as e:
        print(f"  Note: zone_id index: {e}")
    
    try:
        db.dim_food_category.create_index("category_name", unique=True)
    except Exception as e:
        print(f"  Note: category_name index: {e}")
    
    print("✅ Created indexes on dimension tables")
    print("✅ All dimension tables initialized successfully")
    
    client.close()

if __name__ == "__main__":
    init_dimension_tables()

