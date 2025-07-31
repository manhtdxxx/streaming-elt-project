import pandas as pd
import time
import os
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer
import json


# === Data ===
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
data_path = os.path.join(root_path, "data", "2024", "green")
file_path = os.path.join(data_path, "green_tripdata_2024-01.parquet")
df = pd.read_parquet(file_path)
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])  # ensure datetime to sort
df = df.sort_values(by='lpep_pickup_datetime')

# === Config ===
bootstrap_servers = "localhost:9092"
topic_name = "green-taxi-no-arvo"
n_partitions = 3


# === Create Topic ===
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
cluster_metadata = admin_client.list_topics()
existing_topics = cluster_metadata.topics.keys()

if topic_name in existing_topics:
    print(f"Topic '{topic_name}' already exists.")
else:
    topic = NewTopic(topic=topic_name, num_partitions=n_partitions, replication_factor=1)
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created with {n_partitions} partitions.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")


# === Init Producer ===
producer = SerializingProducer({
    'bootstrap.servers': bootstrap_servers,
    'key.serializer': StringSerializer("utf-8"),
    'value.serializer': StringSerializer("utf-8")
})


# === Send data to topic ===
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

long_fields = [
    "VendorID", "RatecodeID", "PULocationID", "DOLocationID",
    "passenger_count", "payment_type", "trip_type"
]

for index, row in df.iterrows():
    data = row.to_dict()

    for key, value in data.items():
         # convert Timestamp to ISO string for serialization
        if isinstance(value, pd.Timestamp):
            data[key] = value.isoformat(timespec="seconds")
        # convert NaN to None, equals to null in Arvo
        elif pd.isna(value):
            data[key] = None
        # convert to Long to fit schema_str
        elif key in long_fields:
            data[key] = int(value)

    # create key for message for partition by hash(key)
    # key_str = str(f"{data.get('VendorID')}_{data.get('lpep_pickup_datetime')}")
    
    data_str = json.dumps(data)  # convert from dict to json-formatted str for serialization

    # Produce message
    producer.produce(topic=topic_name, value=data_str, on_delivery=delivery_report)
    # Get callback
    producer.poll(0)

    time.sleep(1)


# === Cleanup ===
producer.flush()
