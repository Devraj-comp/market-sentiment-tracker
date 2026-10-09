import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from textblob import TextBlob
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_sentiment")

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "market_sentiment")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sentiment_data")

# Define the schema for the incoming Kafka JSON messages
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("source", StringType(), True),
])

def calculate_sentiment(text):
    """
    Analyzes text and returns a polarity score between -1.0 (Negative) and 1.0 (Positive)
    """
    if text is None:
        return 0.0
    return float(TextBlob(text).sentiment.polarity)

# Register the sentiment function as a PySpark UDF
sentiment_udf = udf(calculate_sentiment, FloatType())

def write_to_influxdb(batch_df, batch_id):
    """
    Writes a PySpark micro-batch to InfluxDB
    """
    print(f"Writing batch {batch_id} to InfluxDB...")

    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Collect rows from the DataFrame to write to InfluxDB
    rows = batch_df.collect()

    points = []
    for row in rows:
        # Create a point for each sentiment result
        point = Point("sentiment") \
            .tag("ticker", row.ticker) \
            .field("score", row.sentiment_score)

        # Use current system time to guarantee data appears in recent queries
        # We remove the published_at logic for now to debug the persistence issue
        # point.time(row.published_at, WritePrecision.NS)


        points.append(point)

    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"Successfully wrote {len(points)} points to InfluxDB.")

    client.close()

def main():
    # Initialize Spark Session with Kafka connector
    spark = SparkSession.builder \
        .appName("MarketSentimentAnalyzer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Starting PySpark Sentiment Analysis Engine...")

    # 1. Read stream from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse the binary Kafka value into JSON columns
    json_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Apply Sentiment Analysis
    processed_df = json_df.withColumn(
        "sentiment_score",
        sentiment_udf(col("title"))
    )

    # 4. Output to InfluxDB using foreachBatch
    query = processed_df.writeStream \
        .foreachBatch(write_to_influxdb) \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    print("✨ Streaming analysis started. Data is being persisted to InfluxDB...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
