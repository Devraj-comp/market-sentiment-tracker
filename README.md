# Real-time Market Sentiment Tracker

A real-time data streaming pipeline that tracks market sentiment by streaming live data from various sources (X/Reddit/News), processing sentiment analysis in real-time, and visualizing the results on a Grafana dashboard.

## 🚀 Architecture
- **Data Ingestion**: Python producers fetching data from APIs $\rightarrow$ **Apache Kafka** (Message Queue)
- **Stream Processing**: **PySpark** or **Faust** (Sentiment Analysis & Aggregation)
- **Storage**: **InfluxDB** or **TimescaleDB** (Time-Series Database)
- **Visualization**: **Grafana** (Real-time Dashboards)

## 🛠 Tech Stack
- **Language**: Python
- **Messaging**: Apache Kafka
- **Processing**: PySpark / Faust
- **Database**: InfluxDB
- **Visualization**: Grafana
- **Containerization**: Docker & Docker Compose

## 📈 Key Features
- Live streaming of social media and news feeds.
- Real-time sentiment scoring (Positive/Negative/Neutral).
- Time-series analysis of sentiment trends for specific tickers/keywords.
- Scalable pipeline architecture using distributed messaging.

## 🚦 Getting Started
*(Instructions to be added as the project develops)*
