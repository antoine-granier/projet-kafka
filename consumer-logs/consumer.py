from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'web-logs',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = MongoClient('mongo', 27017)
db = client.kafka_data

for message in consumer:
    print(f"Received: {message.value}")
    db[message.topic].insert_one(message.value)
