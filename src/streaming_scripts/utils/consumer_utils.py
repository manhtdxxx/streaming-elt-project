from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import *
from pyspark.sql.types import *

# no need to config jars because put jars in /opt/bitnami/spark/jars/ will automatically load them
def create_spark_session(app_name: str, cores_max: str = "2", executor_cores: str = "1", executor_memory: str = "512m") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.cores.max", cores_max) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    # .option("kafka.group.id", consumer_group_id) -> do not recommend
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def extract_avro_payload(df_stream, schema_str: str, timestamp_cols=None, id_cols=None, taxi_type=None):
    timestamp_cols = timestamp_cols or []

    # remove 5 first bytes
    df = df_stream \
        .selectExpr("substring(value, 6, length(value) - 5) as value_binary") \
        .select(from_avro(col("value_binary").cast("binary"), jsonFormatSchema=schema_str).alias("data")) \
        .select("data.*")

    if timestamp_cols:
        for col_name in timestamp_cols:
            df = df.withColumn(col_name, to_timestamp(col_name))

    if id_cols and taxi_type:
        df = df.withColumn("trip_id", sha2(concat_ws("||", lit(taxi_type), *[col(c) for c in id_cols]), 256))

    return df