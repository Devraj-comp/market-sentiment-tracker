import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
TOKEN = os.getenv("INFLUXDB_TOKEN")
ORG = os.getenv("INFLUXDB_ORG", "market_sentiment")
BUCKET = os.getenv("INFLUXDB_BUCKET", "sentiment_data")

print(f"Connecting to: {URL} | Org: {ORG} | Bucket: {BUCKET}")

try:
    client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)

    # 1. Try to list buckets to verify connection/auth
    print("\n--- Checking Buckets ---")
    buckets = client.buckets_api().find_buckets()
    print(f"Found {len(buckets.buckets)} bucket(s):")

    for b in buckets.buckets:
        print(f"  - {b.name}")

    # 2. Write a test point
    print("\n--- Writing Test Point ---")
    write_api = client.write_api(write_options=SYNCHRONOUS)
    point = Point("debug_test").tag("status", "working").field("value", 1.0)
    write_api.write(bucket=BUCKET, record=point)
    print("Test point written successfully!")

    # 3. Read the test point back
    print("\n--- Reading Test Point ---")
    query_api = client.query_api()
    query = f'from(bucket: "{BUCKET}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "debug_test")'
    result = query_api.query(org=ORG, query=query)

    found = False
    for table in result:
        for record in table.records:
            print(f"Found data! Value: {record.get_value()} at {record.get_time()}")
            found = True

    if not found:
        print("Could not find the test point we just wrote.")

    client.close()

except Exception as e:
    print(f"\n❌ ERROR: {e}")