import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_sentiment")

# The tickers/keywords we want to track
TICKERS = ["Nvidia", "Tesla", "Bitcoin", "Apple", "Microsoft", "Ethereum"]

def get_news_data(ticker):
    """
    Fetches latest news for a given ticker from NewsAPI.org
    """
    url = f"https://newsapi.org/v2/everything?q={ticker}&sortBy=publishedAt&language=en&apiKey={NEWS_API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get("articles", [])
        else:
            print(f"Error fetching news for {ticker}: {response.status_code}")
            return []
    except Exception as e:
        print(f"Request failed for {ticker}: {e}")
        return []

def main():
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"🚀 Starting News Producer... Streaming to topic: {KAFKA_TOPIC}")

    try:
        while True:
            for ticker in TICKERS:
                print(f"Fetching news for {ticker}...")
                articles = get_news_data(ticker)

                for article in articles:
                    # Create a structured message for the pipeline
                    message = {
                        "ticker": ticker,
                        "title": article.get("title"),
                        "description": article.get("description"),
                        "url": article.get("url"),
                        "published_at": article.get("publishedAt"),
                        "source": article.get("source", {}).get("name")
                    }

                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, value=message)

                print(f"Sent {len(articles)} articles for {ticker} to Kafka.")

            print("--- Cycle complete. Sleeping for 10 minutes... ---")
            time.sleep(600) # Poll every 10 minutes to avoid API rate limits

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
