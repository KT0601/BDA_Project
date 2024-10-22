from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaPySparkStreaming") \
    .getOrCreate()

# Kafka stream configuration in one line
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "172.17.0.3:9092") \
    .option("subscribe", "electricity_topic1") \
    .option("startingOffsets", "earliest").load()

# Define the schema for incoming data
schema = StructType([
    StructField("day", StringType(), True),
    StructField("period", FloatType(), True),
    StructField("nswdemand", FloatType(), True),
    StructField("vicprice", FloatType(), True),
    StructField("vicdemand", FloatType(), True),
    StructField("transfer", FloatType(), True),
    StructField("label", StringType(), True)  # Optional field
])

# Parse JSON messages from Kafka
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Output the parsed stream to the console
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Await termination to keep the stream running
query.awaitTermination()
