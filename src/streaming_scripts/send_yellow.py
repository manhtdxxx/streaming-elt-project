import os
import pandas as pd
from utils.producer_utils import create_kafka_topic, get_avro_serializer, get_kafka_producer, delivery_report
import time

# === Prepare data ===
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # opt/spark/app/.. -> opt/spark
data_path = os.path.join(root_path, "data", "2024", "yellow")  # opt/spark/data/2024/yellow
file_path = os.path.join(data_path, "yellow_tripdata_2024-01.parquet")
df = pd.read_parquet(file_path)
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])  # ensure datetime to sort
df = df.sort_values(by='tpep_pickup_datetime')

# === Load schema ===
schema_path = os.path.join(os.path.dirname(__file__), "avro_schema", "yellow.avsc")
with open(schema_path, "r") as f:
    schema_str = f.read()

# === Config ===
kafka_bootstrap_servers = "kafka:29092"
schema_registry_url = "http://schema-registry:8081"
kafka_topic = "yellow-taxi"
n_partitions = 2


# === Set up Kafka ===
create_kafka_topic(bootstrap_servers=kafka_bootstrap_servers, topic_name=kafka_topic, num_partitions=n_partitions)
avro_serializer = get_avro_serializer(schema_registry_url=schema_registry_url, schema_str=schema_str)
producer = get_kafka_producer(bootstrap_servers=kafka_bootstrap_servers, avro_serializer=avro_serializer)


# === Main Streaming Logic ===
long_fields = [
    "VendorID", "RatecodeID", "PULocationID", "DOLocationID",
    "passenger_count", "payment_type"
]

for idx, row in df.iterrows():
    data = row.to_dict()

    for key, value in data.items():
        if isinstance(value, pd.Timestamp):
            data[key] = value.isoformat(timespec="seconds")
        elif pd.isna(value):
            data[key] = None
        elif key in long_fields:
            data[key] = int(value)

    try:
        producer.produce(topic=kafka_topic, value=data, on_delivery=delivery_report)
        if idx % 1 == 0:
            producer.poll(0)
    except Exception as e:
        print(f"Error producing message: {e}")

    time.sleep(1)

producer.flush()
print("END")
