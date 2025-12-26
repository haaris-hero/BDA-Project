from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import *

spark = SparkSession.builder.appName("KitchenKPIs").getOrCreate()

schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("items_count", IntegerType()),
    StructField("prep_delay_minutes", DoubleType()),
    StructField("predicted_prep_delay", DoubleType()),
    StructField("priority_flag", BooleanType()),
    StructField("order_type", StringType())
])

kitchen = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "kitchen_events") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .load()

kitchen = kitchen.withColumn("payload", from_json(col("payload"), schema)).select("payload.*")

kitchen_kpis = kitchen.groupBy("restaurant_id").agg(
    avg("prep_delay_minutes").alias("avg_prep_delay"),
    avg("items_count").alias("avg_items"),
    count("*").alias("orders_processed")
)

kitchen_kpis.write.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "kitchen_kpis") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()

