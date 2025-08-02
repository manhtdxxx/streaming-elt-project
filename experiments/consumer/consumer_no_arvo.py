from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
import json

# === Kafka config ===
bootstrap_servers = "localhost:9092"
topic_name = "green-taxi-no-arvo"
group_id = "green-taxi-consumer-no-avro"

# === Init Consumer ===
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'key.deserializer': StringDeserializer("utf-8"),
    'value.deserializer': StringDeserializer("utf-8"),
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
        msg = consumer.poll(1.0)  # timeout 1s
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Message received
        key = msg.key()
        value_str = msg.value() # returns string
        value = json.loads(value_str)  # parse JSON string to dict

        print("Message received:")
        print(json.dumps(value, indent=2))  # pretty print

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
