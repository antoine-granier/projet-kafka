from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    return {
        'transaction_id': random.randint(1000, 9999),
        'amount': random.uniform(10.0, 500.0),
        'timestamp': int(time.time()),
        'from_account': random.randint(100000, 999999),
        'to_account': random.randint(100000, 999999)
    }

while True:
    transaction = generate_transaction()
    producer.send('financial-transactions', value=transaction)
    print(f"Sent: {transaction}")
    time.sleep(1)
