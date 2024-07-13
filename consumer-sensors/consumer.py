from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from prometheus_client import start_http_server, Gauge

start_http_server(8000)

temperature_gauge = Gauge('sensor_temperature', 'Temperature of the sensor', ['location'])

spark = SparkSession.builder \
    .appName("RealTimeAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

schema = StructType([
    StructField("location", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", IntegerType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iot-sensors") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")
sensor_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

def update_metrics(rows):
    for row in rows:
        try:
            temperature_gauge.labels(location=row['location']).set(row['temperature'])
        except Exception as e:
            print(f"Error updating metrics: {e}")

def process_batch(batch_df, batch_id):
    rows = batch_df.collect()
    update_metrics(rows)

query = sensor_df.writeStream.foreachBatch(process_batch).start()

avg_df = sensor_df.agg(avg("temperature").alias("avg_temperature"))

raw_query = sensor_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

avg_query = avg_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
raw_query.awaitTermination()
avg_query.awaitTermination()
