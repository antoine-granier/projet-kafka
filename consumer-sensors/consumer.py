# from kafka import KafkaConsumer
# from pymongo import MongoClient
# import json

# consumer = KafkaConsumer(
#     'iot-sensors',
#     bootstrap_servers='kafka:9092',
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# client = MongoClient('mongo', 27017)
# db = client.kafka_data

# for message in consumer:
#     print(f"Received: {message.value}")
#     db[message.topic].insert_one(message.value)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder \
    .appName("RealTimeAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", IntegerType())
])

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "iot-sensors") \
  .load()

df = df.selectExpr("CAST(value AS STRING)")
sensor_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = sensor_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
