from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DeliveryKPIs").getOrCreate()

schema = StructType([
    StructField("zone_id", StringType()),
    StructField("estimated_delivery_minutes", DoubleType()),
    StructField("actual_delivery_minutes", DoubleType()),
    StructField("delivery_delay_minutes", DoubleType())
])

orders = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "orders_events") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .load()

orders = orders.withColumn("payload", from_json(col("payload"), schema)).select("payload.*")

delivery_kpis = orders.groupBy("zone_id").agg(
    avg("actual_delivery_minutes").alias("avg_delivery_time"),
    avg("delivery_delay_minutes").alias("avg_delay_minutes"),
    expr("avg(CASE WHEN delivery_delay_minutes > 0 THEN 1 ELSE 0 END)").alias("delay_rate")
)

delivery_kpis.write.format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "delivery_kpis") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()
