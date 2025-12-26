from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, avg
from pyspark.sql.types import *

spark = SparkSession.builder.appName("OrderKPIs").getOrCreate()

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("zone_id", StringType()),
    StructField("food_category", StringType()),
    StructField("order_value", DoubleType()),
    StructField("discount_amount", DoubleType()),
    StructField("final_amount", DoubleType()),
    StructField("payment_type", StringType()),
    StructField("estimated_delivery_minutes", DoubleType()),
    StructField("actual_delivery_minutes", DoubleType()),
    StructField("delivery_delay_minutes", DoubleType()),
    StructField("cancellation_probability", DoubleType()),
    StructField("is_cancelled", BooleanType()),
    StructField("customer_rating", DoubleType()),
    StructField("event_time", StringType())
])

orders = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "orders_events") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .load()

orders = orders.withColumn("payload", from_json(col("payload"), schema)).select("payload.*")

order_kpis = orders.groupBy("zone_id").agg(
    count("*").alias("total_orders"),
    sum("final_amount").alias("total_revenue"),
    avg("customer_rating").alias("avg_rating")
)

order_kpis.write.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "order_kpis") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()
