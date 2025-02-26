from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define schema for clickstream data
click_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceClickstreamProcessor") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_clicks") \
    .load()

# Parse JSON data
clicks_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), click_schema).alias("data")) \
    .select("data.*")

# Write to Cassandra
query = clicks_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "ecommerce") \
    .option("table", "clicks") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()