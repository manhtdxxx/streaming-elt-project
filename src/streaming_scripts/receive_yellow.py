import os
from utils.consumer_utils import create_spark_session, read_kafka_stream, extract_avro_payload


# === App & Kafka Config ===
app_name = "YellowConsumerApp"
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "yellow-taxi"

# === PostgreSQL Config ===
pg_url = "jdbc:postgresql://postgres-dwh:5432/taxi_dwh"
pg_properties = {"user": "postgres", "password": "123456", "driver": "org.postgresql.Driver"}
pg_table = "raw.yellow_taxi"

# === Load Avro schema ===
schema_path = os.path.join(os.path.dirname(__file__), "avro_schema", "yellow.avsc")
with open(schema_path, "r") as f:
    schema_str = f.read()


# === Define batch write logic ===
def write_to_postgres(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            print(f"[INFO] Batch {batch_id} is empty, skipping.")
            return

        col_sorted = [
            "trip_id", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"
        ]

        df = batch_df.select(*col_sorted)
        record_count = df.count()

        print(f"[INFO] Writing batch {batch_id} with {record_count} records")
        df.write.jdbc(url=pg_url, table=pg_table, mode="append", properties=pg_properties)
        print(f"[SUCCESS] Batch {batch_id} written to PostgreSQL")

    except Exception as e:
        print(f"[ERROR] Failed to write batch {batch_id}: {str(e)}")


# === Main Streaming Logic ===
try:
    spark = create_spark_session(app_name=app_name)
    df_stream = read_kafka_stream(spark, kafka_bootstrap_servers, kafka_topic)

    id_cols = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
    timestamp_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    df_value = extract_avro_payload(df_stream, schema_str, timestamp_cols=timestamp_cols, id_cols=id_cols, taxi_type="yellow")

    query = df_value.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "./tmp/yellow_checkpoints") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

except Exception as e:
    print(f"[ERROR] Streaming query failed: {str(e)}")
    spark.stop()
