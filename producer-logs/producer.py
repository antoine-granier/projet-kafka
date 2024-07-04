from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_web_log():
    return {
        'ip': f'192.168.{random.randint(0, 255)}.{random.randint(0, 255)}',
        'timestamp': int(time.time()),
        'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
        'url': random.choice(['/home', '/login', '/logout', '/products', '/cart']),
        'response_time': random.uniform(0.1, 1.5)
    }

while True:
    log = generate_web_log()
    producer.send('web-logs', value=log)
    print(f"Sent: {log}")
    time.sleep(1)
