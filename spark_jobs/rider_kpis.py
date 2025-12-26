from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg
from pyspark.sql.types import *

spark = SparkSession.builder.appName("RiderKPIs").getOrCreate()

schema = StructType([
    StructField("rider_id", StringType()),
    StructField("zone_id", StringType()),
    StructField("rider_status", StringType()),
    StructField("traffic_delay_minutes", DoubleType()),
    StructField("pickup_delay_minutes", DoubleType()),
    StructField("dropoff_delay_minutes", DoubleType()),
    StructField("distance_to_restaurant_km", DoubleType()),
    StructField("trip_count_today", IntegerType()),
    StructField("idle_time_minutes", IntegerType())
])

riders = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "rider_events") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .load()

riders = riders.withColumn("payload", from_json(col("payload"), schema)).select("payload.*")

rider_kpis = riders.groupBy("zone_id").agg(
    count("*").alias("active_riders"),
    avg("traffic_delay_minutes").alias("avg_traffic_delay"),
    avg("idle_time_minutes").alias("avg_idle_time")
)

rider_kpis.write.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "rider_kpis") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()

