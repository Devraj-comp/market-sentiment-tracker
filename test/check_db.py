import os
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

load_dotenv()

client = InfluxDBClient(
    url=os.getenv("INFLUXDB_URL"),
    token=os.getenv("INFLUXDB_TOKEN"),
    org=os.getenv("INFLUXDB_ORG")
)

query_api = client.query_api()

# Flux query to get the last 10 sentiment scores
query = f'''
from(bucket: "{os.getenv("INFLUXDB_BUCKET")}")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "sentiment")
  |> filter(fn: (r) => r._field == "score")
  |> limit(n: 10)
'''

result = query_api.query(org=os.getenv("INFLUXDB_ORG"), query=query)

print(f"{'Time':<30} | {'Ticker':<10} | {'Score':<10}")
print("-" * 55)
for table in result:
    for record in table.records:
        print(f"{str(record.get_time()):<30} | {record.values.get('ticker'):<10} | {record.get_value():<10}")

client.close()