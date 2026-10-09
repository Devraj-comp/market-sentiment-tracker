import os

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point

load_dotenv()

url = os.getenv("INFLUXDB_URL")
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
bucket = os.getenv("INFLUXDB_BUCKET")

print("URL:", url)
print("ORG:", org)
print("BUCKET:", bucket)
print("TOKEN:", token[:8] + "..." if token else None)

client = InfluxDBClient(
    url=url,
    token=token,
    org=org
)

health = client.health()
print("InfluxDB:", health.status)

point = (
    Point("test")
    .tag("source", "python")
    .field("value", 1)
)

write_api = client.write_api()

write_api.write(
    bucket=bucket,
    org=org,
    record=point
)

print("Successfully wrote test point")

client.close()