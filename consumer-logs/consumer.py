from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from prometheus_client import start_http_server, Counter, Summary

start_http_server(8000)

message_counter = Counter('kafka_messages_total', 'Total number of messages received from Kafka')
processing_time = Summary('message_processing_seconds', 'Time spent processing message')

# Configurer le consommateur Kafka
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

@processing_time.time()
def process_message(message):
    """Process and store the message in MongoDB."""
    print(f"Received: {message.value}")
    db[message.topic].insert_one(message.value)
    message_counter.inc()

for message in consumer:
    process_message(message)
