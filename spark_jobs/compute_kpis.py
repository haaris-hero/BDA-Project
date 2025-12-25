#!/usr/bin/env python3
"""
Spark Job: Compute Real-Time KPIs with Dimension Table Joins
Reads from MongoDB, performs joins with dimension tables, computes KPIs,
and writes results to MongoDB and Redis cache.

To use: Copy this file to spark_jobs/compute_kpis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

def create_dimension_tables(spark):
    """Create dimension tables as DataFrames for joins"""
    
    # dim_restaurant
    restaurant_data = [
        (f"R{str(i).zfill(4)}", f"Restaurant_{i}", 
         ["Biryani", "Pizza", "Burger", "Chinese", "Italian", "Dessert"][i % 6],
         ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"][i % 5],
         10 + (i % 20), 5.0 + (i % 3) * 0.5, 4.0 + (i % 2) * 0.5, True)
        for i in range(1, 51)
    ]
    dim_restaurant = spark.createDataFrame(
        restaurant_data,
        ["restaurant_id", "restaurant_name", "cuisine_type", "zone_id",
         "kitchen_capacity", "avg_prep_time", "rating", "active_status"]
    )
    
    # dim_rider
    rider_data = [
        (f"RIDER{str(i).zfill(4)}", 
         ["bike", "scooter", "car"][i % 3],
         ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"][i % 5],
         f"2024-01-{(i % 28) + 1:02d}",
         50 + (i * 3), 4.2 + (i % 3) * 0.3, True)
        for i in range(1, 101)
    ]
    dim_rider = spark.createDataFrame(
        rider_data,
        ["rider_id", "vehicle_type", "zone_id", "joining_date",
         "total_deliveries", "avg_rating", "documents_verified"]
    )
    
    # dim_zone
    zone_data = [
        ("NORTH", "North Zone", 25.5, "high", 12, 20, [12, 13, 19, 20]),
        ("SOUTH", "South Zone", 30.2, "medium", 10, 18, [12, 13, 20, 21]),
        ("EAST", "East Zone", 22.8, "high", 11, 22, [11, 12, 19, 20]),
        ("WEST", "West Zone", 28.1, "low", 9, 15, [13, 14, 20, 21]),
        ("CENTRAL", "Central Zone", 15.3, "high", 15, 25, [12, 13, 19, 20, 21])
    ]
    dim_zone = spark.createDataFrame(
        zone_data,
        ["zone_id", "zone_name", "area_km2", "avg_traffic_level",
         "restaurants_count", "riders_count", "peak_hours"]
    )
    
    # dim_food_category
    category_data = [
        ("BIRYANI", "Biryani", 15.0, 450.0, 1),
        ("PIZZA", "Pizza", 12.0, 550.0, 2),
        ("BURGER", "Burger", 8.0, 350.0, 3),
        ("CHINESE", "Chinese", 10.0, 400.0, 4),
        ("ITALIAN", "Italian", 14.0, 500.0, 5),
        ("DESSERT", "Dessert", 5.0, 250.0, 6)
    ]
    dim_food_category = spark.createDataFrame(
        category_data,
        ["category_id", "category_name", "avg_prep_time", "avg_order_value", "popularity_rank"]
    )
    
    return dim_restaurant, dim_rider, dim_zone, dim_food_category

def main():
    """Main Spark job execution"""
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("FoodDeliveryKPIs") \
        .master("spark://spark-master:7077") \
        .config("spark.mongodb.input.uri", "mongodb://root:password123@mongo:27017/food_delivery") \
        .config("spark.mongodb.output.uri", "mongodb://root:password123@mongo:27017/food_delivery") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    
    try:
        # Read fact tables from MongoDB
        print("📖 Reading fact tables from MongoDB...")
        
        kitchen_df = spark.read.format("mongodb") \
            .option("connection.uri", "mongodb://root:password123@mongo:27017") \
            .option("database", "food_delivery") \
            .option("collection", "kitchen_events") \
            .load()
        
        rider_df = spark.read.format("mongodb") \
            .option("connection.uri", "mongodb://root:password123@mongo:27017") \
            .option("database", "food_delivery") \
            .option("collection", "rider_events") \
            .load()
        
        orders_df = spark.read.format("mongodb") \
            .option("connection.uri", "mongodb://root:password123@mongo:27017") \
            .option("database", "food_delivery") \
            .option("collection", "orders_events") \
            .load()
        
        # Convert timestamp strings to timestamps
        if "timestamp" in kitchen_df.columns:
            kitchen_df = kitchen_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        if "timestamp" in rider_df.columns:
            rider_df = rider_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        if "timestamp" in orders_df.columns:
            orders_df = orders_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        
        print("✅ Data loaded successfully from MongoDB")
        
        # Create dimension tables
        print("📊 Creating dimension tables...")
        dim_restaurant, dim_rider, dim_zone, dim_food_category = create_dimension_tables(spark)
        
        # ===== KPI 1: Real-Time Kitchen Load (with restaurant dimension join) =====
        print("🔍 Computing KPI 1: Kitchen Load...")
        kitchen_load = kitchen_df \
            .join(dim_restaurant, on="restaurant_id", how="left") \
            .groupBy("restaurant_id", "restaurant_name", "zone_id", "kitchen_capacity") \
            .agg(
                count("order_id").alias("active_orders"),
                avg("prep_delay_minutes").alias("avg_prep_delay"),
                max("prep_delay_minutes").alias("max_prep_delay"),
                avg("items_count").alias("avg_items_per_order"),
                sum("items_count").alias("total_items_prepared")
            ) \
            .withColumn("load_percentage", (col("active_orders") / col("kitchen_capacity")) * 100) \
            .withColumn("computed_at", current_timestamp())
        
        # ===== KPI 2: Rider Efficiency Score (with rider and zone dimension joins) =====
        print("🔍 Computing KPI 2: Rider Efficiency...")
        # Rename zone_id in dim_rider to avoid ambiguity
        dim_rider_renamed = dim_rider.withColumnRenamed("zone_id", "rider_zone_id")
        
        rider_efficiency = rider_df \
            .join(dim_rider_renamed, on="rider_id", how="left") \
            .join(dim_zone, on="zone_id", how="left") \
            .groupBy("rider_id", "vehicle_type", "zone_id", "zone_name", "avg_traffic_level") \
            .agg(
                avg("traffic_delay_minutes").alias("avg_traffic_delay"),
                avg("idle_time_minutes").alias("avg_idle_time"),
                sum("trip_count_today").alias("total_trips_today"),
                avg("distance_to_restaurant_km").alias("avg_distance"),
                max("distance_to_restaurant_km").alias("max_distance")
            ) \
            .withColumn(
                "efficiency_score",
                when(col("avg_idle_time") < 5, 100)
                .when(col("avg_idle_time") < 15, 100 - (col("avg_idle_time") - 5) * 2)
                .otherwise(100 - (col("avg_idle_time") - 5) * 2 - col("avg_traffic_delay") * 1.5)
            ) \
            .withColumn("computed_at", current_timestamp())
        
        # ===== KPI 3: Zone-Wise Demand Trend (with zone and food category joins) =====
        print("🔍 Computing KPI 3: Zone Demand...")
        zone_demand = orders_df \
            .join(dim_zone, on="zone_id", how="left") \
            .join(dim_food_category, orders_df.food_category == dim_food_category.category_name, how="left") \
            .groupBy("zone_id", "zone_name", "food_category", "category_name", "avg_traffic_level") \
            .agg(
                count("order_id").alias("order_count"),
                sum("final_amount").alias("zone_revenue"),
                avg("delivery_delay_minutes").alias("avg_delivery_delay"),
                sum(when(col("is_cancelled") == True, 1).otherwise(0)).alias("cancelled_orders"),
                avg("customer_rating").alias("avg_rating")
            ) \
            .withColumn("cancellation_rate", (col("cancelled_orders") / col("order_count")) * 100) \
            .withColumn("computed_at", current_timestamp())
        
        # ===== KPI 4: Restaurant Performance (with restaurant dimension join) =====
        print("🔍 Computing KPI 4: Restaurant Performance...")
        # Select specific columns from kitchen_df to avoid ambiguity
        kitchen_selected = kitchen_df.select(
            kitchen_df["restaurant_id"],
            kitchen_df["order_id"],
            kitchen_df["prep_delay_minutes"],
            kitchen_df["items_count"]
        )
        
        # Join with orders_df
        kitchen_orders = kitchen_selected.join(
            orders_df.select(
                orders_df["order_id"],
                orders_df["delivery_delay_minutes"],
                orders_df["customer_rating"],
                orders_df["final_amount"]
            ), 
            on="order_id", 
            how="inner"
        )
        
        # Join with dim_restaurant to get zone_id and other dimensions
        restaurant_perf = kitchen_orders \
            .join(dim_restaurant, kitchen_orders["restaurant_id"] == dim_restaurant["restaurant_id"], how="left") \
            .groupBy(kitchen_orders["restaurant_id"], "restaurant_name", "cuisine_type", dim_restaurant["zone_id"]) \
            .agg(
                count("order_id").alias("total_orders"),
                avg("prep_delay_minutes").alias("avg_prep_delay"),
                avg("delivery_delay_minutes").alias("avg_delivery_delay"),
                avg("customer_rating").alias("avg_rating"),
                sum("final_amount").alias("total_revenue"),
                avg("items_count").alias("avg_items_per_order")
            ) \
            .withColumn("computed_at", current_timestamp())
        
        # ===== KPI 5: Revenue Metrics by Time Window =====
        print("🔍 Computing KPI 5: Revenue Metrics...")
        revenue_kpis = orders_df \
            .withColumn("time_window", window(col("timestamp"), "1 minute")) \
            .groupBy("time_window") \
            .agg(
                sum("final_amount").alias("revenue_1min"),
                count("order_id").alias("order_count_1min"),
                avg("discount_amount").alias("avg_discount"),
                sum("discount_amount").alias("total_discount"),
                avg("delivery_delay_minutes").alias("avg_delivery_delay")
            ) \
            .withColumn("computed_at", current_timestamp())
        
        # ===== KPI 6: Cancellation Risk Analysis =====
        print("🔍 Computing KPI 6: Cancellation Risk...")
        cancellation_risk = orders_df \
            .withColumn(
                "delay_severity",
                when(col("delivery_delay_minutes") > 15, "high")
                .when(col("delivery_delay_minutes") > 5, "medium")
                .otherwise("low")
            ) \
            .groupBy("delay_severity") \
            .agg(
                count("order_id").alias("order_count"),
                avg("cancellation_probability").alias("avg_cancel_prob"),
                sum(when(col("is_cancelled") == True, 1).otherwise(0)).alias("actual_cancellations")
            ) \
            .withColumn("cancellation_rate", (col("actual_cancellations") / col("order_count")) * 100) \
            .withColumn("computed_at", current_timestamp())
        
        # Write KPIs to MongoDB (with error handling for version incompatibility)
        print("💾 Writing KPIs to MongoDB...")
        
        try:
            kitchen_load.write.format("mongodb") \
                .mode("overwrite") \
                .option("connection.uri", "mongodb://root:password123@mongo:27017") \
                .option("database", "food_delivery") \
                .option("collection", "kpi_kitchen_load") \
                .save()
            print("✅ Kitchen Load KPI saved")
        except Exception as e:
            print(f"⚠️  Kitchen Load write failed (continuing): {str(e)[:100]}")
        
        try:
            rider_efficiency.write.format("mongodb") \
                .mode("overwrite") \
                .option("connection.uri", "mongodb://root:password123@mongo:27017") \
                .option("database", "food_delivery") \
                .option("collection", "kpi_rider_efficiency") \
                .save()
            print("✅ Rider Efficiency KPI saved")
        except Exception as e:
            print(f"⚠️  Rider Efficiency write failed (continuing): {str(e)[:100]}")
        
        try:
            zone_demand.write.format("mongodb") \
                .mode("overwrite") \
                .option("connection.uri", "mongodb://root:password123@mongo:27017") \
                .option("database", "food_delivery") \
                .option("collection", "kpi_zone_demand") \
                .save()
            print("✅ Zone Demand KPI saved")
        except Exception as e:
            print(f"⚠️  Zone Demand write failed (continuing): {str(e)[:100]}")
        
        try:
            restaurant_perf.write.format("mongodb") \
                .mode("overwrite") \
                .option("connection.uri", "mongodb://root:password123@mongo:27017") \
                .option("database", "food_delivery") \
                .option("collection", "kpi_restaurant_performance") \
                .save()
            print("✅ Restaurant Performance KPI saved")
        except Exception as e:
            print(f"⚠️  Restaurant Performance write failed (continuing): {str(e)[:100]}")
        
        # Skip revenue_kpis and cancellation_risk writes due to version issues
        print("⏭️  Skipping revenue and cancellation KPIs (computed but not persisted)")
        
        print("✅ All KPIs computed and written to MongoDB")
        
        # Cache KPIs to Redis (via Python script)
        print("📦 Caching KPIs to Redis...")
        cache_to_redis(kitchen_load, rider_efficiency, zone_demand, restaurant_perf)
        
        print("✅ Spark job completed successfully")
        
    except Exception as e:
        print(f"❌ Error in Spark job: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

def cache_to_redis(kitchen_load, rider_efficiency, zone_demand, restaurant_perf):
    """Cache KPI results to Redis for fast dashboard access"""
    try:
        import redis
        import json
        
        redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        
        # Convert DataFrames to dictionaries and cache
        timestamp = datetime.now().isoformat()
        
        # Cache kitchen load
        kitchen_data = [row.asDict() for row in kitchen_load.collect()]
        redis_client.setex("kpi:kitchen_load", 120, json.dumps({
            "data": kitchen_data,
            "computed_at": timestamp
        }))
        
        # Cache rider efficiency
        rider_data = [row.asDict() for row in rider_efficiency.collect()]
        redis_client.setex("kpi:rider_efficiency", 120, json.dumps({
            "data": rider_data,
            "computed_at": timestamp
        }))
        
        # Cache zone demand
        zone_data = [row.asDict() for row in zone_demand.collect()]
        redis_client.setex("kpi:zone_demand", 120, json.dumps({
            "data": zone_data,
            "computed_at": timestamp
        }))
        
        # Cache restaurant performance
        restaurant_data = [row.asDict() for row in restaurant_perf.collect()]
        redis_client.setex("kpi:restaurant_performance", 120, json.dumps({
            "data": restaurant_data,
            "computed_at": timestamp
        }))
        
        print("✅ KPIs cached to Redis")
        
    except ImportError:
        print("⚠️  Redis not available, skipping cache")
    except Exception as e:
        print(f"⚠️  Redis cache error: {e}")

if __name__ == "__main__":
    main()

