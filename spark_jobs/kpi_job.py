from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, when, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

spark = SparkSession.builder \
    .appName("FoodDelivery-KPI-Job") \
    .getOrCreate()

# ------------------------------------------------------------------
# Read raw events from Postgres
# ------------------------------------------------------------------
ordersDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "orders_events") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# ------------------------------------------------------------------
# Define JSON schema of payload
# ------------------------------------------------------------------
payload_schema = StructType([
    StructField("zone_id", StringType(), True),
    StructField("actual_delivery_minutes", DoubleType(), True),
    StructField("final_amount", DoubleType(), True),
    StructField("is_cancelled", BooleanType(), True)
])

# ------------------------------------------------------------------
# Parse JSON string → Struct
# ------------------------------------------------------------------
parsedDF = ordersDF.withColumn(
    "payload_json",
    from_json(col("payload"), payload_schema)
)

# ------------------------------------------------------------------
# Flatten fields
# ------------------------------------------------------------------
flattenedDF = parsedDF.select(
    col("payload_json.zone_id").alias("zone"),
    col("payload_json.actual_delivery_minutes").alias("actual_delivery_minutes"),
    col("payload_json.final_amount").alias("final_amount"),
    col("payload_json.is_cancelled").alias("is_cancelled")
)

# ------------------------------------------------------------------
# Compute KPIs
# ------------------------------------------------------------------
kpiDF = flattenedDF.groupBy("zone").agg(
    avg("actual_delivery_minutes").alias("avg_delivery_time"),
    sum("final_amount").alias("total_revenue"),
    count("*").alias("total_orders"),
    avg(when(col("is_cancelled") == True, 1).otherwise(0)).alias("cancellation_rate")
)

# ------------------------------------------------------------------
# Write KPIs back to Postgres
# ------------------------------------------------------------------
kpiDF.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://superset-postgres:5432/superset") \
    .option("dbtable", "zone_kpis") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()
