from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json

# === Config ===
bootstrap_servers = "localhost:9092"
schema_registry_url = "http://localhost:8081"
topic_name = "green-taxi"
group_id = "green-taxi-consumer"

# === Init Arvo Deserializer ===
schema_registry_conf = {'url': schema_registry_url}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=None,  # schema fetched from registry by schema_id in message
    from_dict=lambda obj, ctx: obj
)

# === Init Consumer ===
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'key.deserializer': StringDeserializer("utf-8"),
    'value.deserializer': avro_deserializer,
    'group.id': group_id,  # define id for consumer group
    'auto.offset.reset': 'earliest',  # when no commit, start from message of lowest offset
    # 'auto.offset.reset': 'latest', # when no commit, start from messages sent after trigger consumer
    'enable.auto.commit': True, # automatically save the latest offset for later continuation
    'auto.commit.interval.ms': 5000, # automatically commit after 5s
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([topic_name])

# === Get data from topic ===
print("Waiting for messages... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        key = msg.key()
        value = msg.value()  # returns dict

        print("Message received:")
        print(json.dumps(value, indent=2))  # Pretty print

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
