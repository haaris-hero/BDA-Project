from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'spark_transform_and_aggregate',
    default_args=default_args,
    description='Spark Structured Streaming - Real-time KPI Computation',
    schedule_interval='*/1 * * * *',  # Every minute
    catchup=False,
)

def compute_kpis(**context):
    """Compute real-time KPIs using Spark SQL"""
    try:
        spark = SparkSession.builder \
            .appName("FoodDeliveryKPIs") \
            .master("spark://spark-master:7077") \
            .config("spark.mongodb.input.uri", "mongodb://root:password123@mongo:27017/food_delivery") \
            .config("spark.mongodb.output.uri", "mongodb://root:password123@mongo:27017/food_delivery") \
            .getOrCreate()
        
        # Read collections from MongoDB
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
        
        # ===== KPI 1: Real-Time Kitchen Load =====
        kitchen_load = kitchen_df.groupBy("restaurant_id") \
            .agg(
                count("order_id").alias("active_orders"),
                avg("prep_delay_minutes").alias("avg_prep_delay"),
                max("prep_delay_minutes").alias("max_prep_delay"),
                avg("items_count").alias("avg_items_per_order")
            )
        
        # ===== KPI 2: Rider Efficiency Score =====
        rider_efficiency = rider_df.groupBy("rider_id", "zone_id") \
            .agg(
                avg("traffic_delay_minutes").alias("avg_traffic_delay"),
                avg("idle_time_minutes").alias("avg_idle_time"),
                sum("trip_count_today").alias("total_trips_today"),
                max("distance_to_restaurant_km").alias("max_distance")
            ) \
            .withColumn(
                "efficiency_score",
                (100 - col("avg_idle_time")*5 - col("avg_traffic_delay")*2)
            )
        
        # ===== KPI 3: Zone-Wise Demand Trend =====
        zone_demand = orders_df.groupBy("zone_id", "food_category") \
            .agg(
                count("order_id").alias("order_count"),
                sum("final_amount").alias("zone_revenue"),
                avg("delivery_delay_minutes").alias("avg_delivery_delay"),
                sum(when(col("is_cancelled") == True, 1).otherwise(0)).alias("cancelled_orders")
            )
        
        # ===== KPI 4: Cancellation Risk Analysis =====
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
            )
        
        # ===== KPI 5: Restaurant Performance =====
        restaurant_perf = kitchen_df.join(
            orders_df,
            on="order_id",
            how="inner"
        ).groupBy("restaurant_id") \
        .agg(
            count("order_id").alias("total_orders"),
            avg("prep_delay_minutes").alias("avg_prep_delay"),
            avg("delivery_delay_minutes").alias("avg_delivery_delay"),
            avg("customer_rating").alias("avg_rating")
        )
        
        # ===== KPI 6: Revenue Metrics =====
        revenue_kpis = orders_df.groupBy(
            window("timestamp", "1 minute")
        ).agg(
            sum("final_amount").alias("revenue_1min"),
            count("order_id").alias("order_count_1min"),
            avg("discount_amount").alias("avg_discount"),
            sum("discount_amount").alias("total_discount")
        )
        
        # Write KPIs to MongoDB for BI dashboard
        kpis_summary = {
            "kitchen_load": kitchen_load.collect(),
            "rider_efficiency": rider_efficiency.collect(),
            "zone_demand": zone_demand.collect(),
            "cancellation_risk": cancellation_risk.collect(),
            "restaurant_performance": restaurant_perf.collect(),
            "revenue_metrics": revenue_kpis.collect(),
            "computed_at": datetime.now().isoformat()
        }
        
        # Write to MongoDB
        kitchen_load.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "food_delivery") \
            .option("collection", "kpi_kitchen_load") \
            .save()
        
        rider_efficiency.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "food_delivery") \
            .option("collection", "kpi_rider_efficiency") \
            .save()
        
        zone_demand.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "food_delivery") \
            .option("collection", "kpi_zone_demand") \
            .save()
        
        restaurant_perf.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "food_delivery") \
            .option("collection", "kpi_restaurant_performance") \
            .save()
        
        logging.info("KPIs computed and written to MongoDB")
        spark.stop()
        return True
    except Exception as e:
        logging.error(f"Error in Spark job: {e}")
        raise

# Tasks
task_compute_kpis = PythonOperator(
    task_id='compute_analytics_kpis',
    python_callable=compute_kpis,
    dag=dag,
)

task_compute_kpis
