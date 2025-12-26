from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("TopZones").getOrCreate()

jdbc_url = "jdbc:postgresql://superset-postgres:5432/superset"
props = {
    "user": "superset",
    "password": "superset",
    "driver": "org.postgresql.Driver"
}

orders = spark.read.jdbc(
    url=jdbc_url,
    table="orders_events",
    properties=props
)

schema = StructType().add("zone", StringType())

parsed = orders.withColumn(
    "json",
    from_json(col("payload"), schema)
)

top_zones = parsed.select(
    col("json.zone").alias("zone")
).groupBy("zone").count().orderBy(col("count").desc())

top_zones.write.jdbc(
    url=jdbc_url,
    table="top_zones",
    mode="overwrite",
    properties=props
)

spark.stop()
