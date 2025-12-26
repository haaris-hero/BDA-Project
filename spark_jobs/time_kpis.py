from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("TimeKPIs").getOrCreate()

jdbc_url = "jdbc:postgresql://superset-postgres:5432/superset"
properties = {
    "user": "superset",
    "password": "superset",
    "driver": "org.postgresql.Driver"
}

orders = spark.read.jdbc(
    url=jdbc_url,
    table="orders_events",
    properties=properties
)

schema = StructType() \
    .add("order_id", StringType()) \
    .add("zone", StringType()) \
    .add("delivery_time", IntegerType()) \
    .add("event_time", TimestampType())

parsed = orders.withColumn(
    "json",
    from_json(col("payload"), schema)
)

hourly_kpis = parsed.select(
    hour(col("json.event_time")).alias("hour")
).groupBy("hour").count()

hourly_kpis.write.jdbc(
    url=jdbc_url,
    table="hourly_kpis",
    mode="overwrite",
    properties=properties
)

spark.stop()
