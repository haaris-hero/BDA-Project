#!/usr/bin/env python3
"""
Spark Job: Compute Real-Time KPIs with Dimension Table Joins

Reads from MongoDB, performs joins with dimension tables,
computes KPIs, writes results to MongoDB and Redis cache.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


# =========================================================
# Dimension Tables
# =========================================================

def create_dimension_tables(spark):
    # dim_restaurant
    restaurant_data = [
        (
            f"R{str(i).zfill(4)}",
            f"Restaurant_{i}",
            ["Biryani", "Pizza", "Burger", "Chinese", "Italian", "Dessert"][i % 6],
            ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"][i % 5],
            10 + (i % 20),
            5.0 + (i % 3) * 0.5,
            4.0 + (i % 2) * 0.5,
            True
        )
        for i in range(1, 51)
    ]

    dim_restaurant = spark.createDataFrame(
        restaurant_data,
        [
            "restaurant_id", "restaurant_name", "cuisine_type", "zone_id",
            "kitchen_capacity", "avg_prep_time", "rating", "active_status"
        ]
    )

    # dim_rider
    rider_data = [
        (
            f"RIDER{str(i).zfill(4)}",
            ["bike", "scooter", "car"][i % 3],
            ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"][i % 5],
            f"2024-01-{(i % 28) + 1:02d}",
            50 + (i * 3),
            4.2 + (i % 3) * 0.3,
            True
        )
        for i in range(1, 101)
    ]

    dim_rider = spark.createDataFrame(
        rider_data,
        [
            "rider_id", "vehicle_type", "zone_id", "joining_date",
            "total_deliveries", "avg_rating", "documents_verified"
        ]
    )

    # dim_zone
    zone_data = [
        ("NORTH", "North Zone", 25.5, "high", 12, 20),
        ("SOUTH", "South Zone", 30.2, "medium", 10, 18),
        ("EAST", "East Zone", 22.8, "high", 11, 22),
        ("WEST", "West Zone", 28.1, "low", 9, 15),
        ("CENTRAL", "Central Zone", 15.3, "high", 15, 25)
    ]

    dim_zone = spark.createDataFrame(
        zone_data,
        [
            "zone_id", "zone_name", "area_km2",
            "avg_traffic_level", "restaurants_count", "riders_count"
        ]
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
        [
            "category_id", "category_name",
            "avg_prep_time", "avg_order_value", "popularity_rank"
        ]
    )

    return dim_restaurant, dim_rider, dim_zone, dim_food_category


# =========================================================
# Main Job
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName("FoodDeliveryKPIs")
        .master("spark://spark-master:7077")
        .config(
            "spark.mongodb.input.uri",
            "mongodb://root:password123@mongo:27017/food_delivery"
        )
        .config(
            "spark.mongodb.output.uri",
            "mongodb://root:password123@mongo:27017/food_delivery"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created")

    try:
        # ================= Read MongoDB =================
        kitchen_df = spark.read.format("mongodb") \
            .option("database", "food_delivery") \
            .option("collection", "kitchen_events") \
            .load()

        rider_df = spark.read.format("mongodb") \
            .option("database", "food_delivery") \
            .option("collection", "rider_events") \
            .load()

        orders_df = spark.read.format("mongodb") \
            .option("database", "food_delivery") \
            .option("collection", "orders_events") \
            .load()

        for df in [kitchen_df, rider_df, orders_df]:
            if "timestamp" in df.columns:
                df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

        print("✅ MongoDB data loaded")

        # ================= Dimensions =================
        dim_restaurant, dim_rider, dim_zone, dim_food_category = \
            create_dimension_tables(spark)

        # ================= KPI 1: Kitchen Load =================
        kitchen_load = (
            kitchen_df
            .join(dim_restaurant, "restaurant_id", "left")
            .groupBy(
                "restaurant_id", "restaurant_name",
                "zone_id", "kitchen_capacity"
            )
            .agg(
                count("order_id").alias("active_orders"),
                avg("prep_delay_minutes").alias("avg_prep_delay")
            )
            .withColumn(
                "load_percentage",
                (col("active_orders") / col("kitchen_capacity")) * 100
            )
            .withColumn("computed_at", current_timestamp())
        )

        # ================= KPI 2: Rider Efficiency =================
        rider_efficiency = (
            rider_df
            .join(dim_rider, "rider_id", "left")
            .join(dim_zone, "zone_id", "left")
            .groupBy(
                "rider_id", "vehicle_type",
                "zone_id", "zone_name", "avg_traffic_level"
            )
            .agg(
                avg("traffic_delay_minutes").alias("avg_traffic_delay"),
                avg("idle_time_minutes").alias("avg_idle_time"),
                sum("trip_count_today").alias("total_trips_today")
            )
            .withColumn(
                "efficiency_score",
                when(col("avg_idle_time") < 5, 100)
                .when(col("avg_idle_time") < 15,
                      100 - (col("avg_idle_time") - 5) * 2)
                .otherwise(
                    100 - (col("avg_idle_time") - 5) * 2
                    - col("avg_traffic_delay") * 1.5
                )
            )
            .withColumn("computed_at", current_timestamp())
        )

        # ================= KPI 3: Zone Demand =================
        zone_demand = (
            orders_df
            .join(dim_zone, "zone_id", "left")
            .join(
                dim_food_category,
                orders_df.food_category == dim_food_category.category_name,
                "left"
            )
            .groupBy(
                "zone_id", "zone_name",
                "food_category", "category_name"
            )
            .agg(
                count("order_id").alias("order_count"),
                sum("final_amount").alias("zone_revenue"),
                avg("delivery_delay_minutes").alias("avg_delivery_delay")
            )
            .withColumn("computed_at", current_timestamp())
        )

        # ================= KPI 4: Restaurant Performance =================
        restaurant_perf = (
            kitchen_df
            .join(orders_df, "order_id", "inner")
            .join(
                dim_restaurant,
                kitchen_df.restaurant_id == dim_restaurant.restaurant_id,
                "left"
            )
            .groupBy(
                "restaurant_id", "restaurant_name",
                "cuisine_type", "zone_id"
            )
            .agg(
                count("order_id").alias("total_orders"),
                avg("prep_delay_minutes").alias("avg_prep_delay"),
                avg("delivery_delay_minutes").alias("avg_delivery_delay"),
                sum("final_amount").alias("total_revenue")
            )
            .withColumn("computed_at", current_timestamp())
        )

        # ================= Write to MongoDB =================
        kitchen_load.write.format("mongodb") \
            .mode("overwrite") \
            .option("collection", "kpi_kitchen_load") \
            .save()

        rider_efficiency.write.format("mongodb") \
            .mode("overwrite") \
            .option("collection", "kpi_rider_efficiency") \
            .save()

        zone_demand.write.format("mongodb") \
            .mode("overwrite") \
            .option("collection", "kpi_zone_demand") \
            .save()

        restaurant_perf.write.format("mongodb") \
            .mode("overwrite") \
            .option("collection", "kpi_restaurant_performance") \
            .save()

        print("✅ KPIs written to MongoDB")

        # ================= Redis Cache =================
        cache_to_redis(
            kitchen_load,
            rider_efficiency,
            zone_demand,
            restaurant_perf
        )

        print("🚀 Spark KPI job completed successfully")

    finally:
        spark.stop()


# =========================================================
# Redis Cache
# =========================================================

def cache_to_redis(kitchen_load, rider_efficiency, zone_demand, restaurant_perf):
    try:
        import redis
        import json

        r = redis.Redis(
            host="redis",
            port=6379,
            db=0,
            decode_responses=True
        )

        ts = datetime.now().isoformat()

        r.setex(
            "kpi:kitchen_load", 120,
            json.dumps({"data": [r.asDict() for r in kitchen_load.collect()], "ts": ts})
        )

        r.setex(
            "kpi:rider_efficiency", 120,
            json.dumps({"data": [r.asDict() for r in rider_efficiency.collect()], "ts": ts})
        )

        r.setex(
            "kpi:zone_demand", 120,
            json.dumps({"data": [r.asDict() for r in zone_demand.collect()], "ts": ts})
        )

        r.setex(
            "kpi:restaurant_performance", 120,
            json.dumps({"data": [r.asDict() for r in restaurant_perf.collect()], "ts": ts})
        )

        print("✅ KPIs cached in Redis")

    except Exception as e:
        print(f"⚠️ Redis cache skipped: {e}")


if __name__ == "__main__":
    main()
