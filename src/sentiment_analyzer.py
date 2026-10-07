import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from textblob import TextBlob
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_sentiment")

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

def main():
    # Initialize Spark Session with Kafka connector
    # Note: In a real production environment, these packages are provided via --packages
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
    # Kafka delivers values as binary, so we cast to string first
    json_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Apply Sentiment Analysis
    # We analyze the title and description combined
    processed_df = json_df.withColumn(
        "sentiment_score",
        sentiment_udf(col("title")) # Simplified for now, could combine with description
    )

    # 4. Output for debugging (Console)
    # We add a checkpoint location to prevent the SerializedOffset error
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    print("✨ Streaming analysis started. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
