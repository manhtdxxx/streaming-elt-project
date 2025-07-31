import pandas as pd
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer, SerializationContext


def create_kafka_topic(bootstrap_servers, topic_name, num_partitions=2, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    existing_topics = admin_client.list_topics().topics.keys()

    if topic_name not in existing_topics:
        topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            admin_client.create_topics([topic])
            print(f"Created topic '{topic_name}' with {num_partitions} partitions.")
        except Exception as e:
            print(f"Failed to create topic '{topic_name}': {e}")
    else:
        print(f"Topic '{topic_name}' already exists.")


def get_avro_serializer(schema_registry_url: str, schema_str: str):
    client = SchemaRegistryClient({'url': schema_registry_url})

    def obj_to_dict(obj, ctx: SerializationContext):
        return obj  # row.to_dict() is already a dict

    return AvroSerializer(client, schema_str, obj_to_dict)


def get_kafka_producer(bootstrap_servers: str, avro_serializer: AvroSerializer) -> SerializingProducer:
    return SerializingProducer({
        'bootstrap.servers': bootstrap_servers,
        'key.serializer': StringSerializer("utf-8"),
        'value.serializer': avro_serializer
    })


def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")