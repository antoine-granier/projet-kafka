import requests
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://api.weatherapi.com/v1/current.json"
API_KEY = "7a3f216fa20a449787790846240807"

def fetch_weather_data(location):
    params = {
        'key': API_KEY,
        'q': location
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            'location': data['location']['name'],
            'temperature': data['current']['temp_c'],
            'humidity': data['current']['humidity'],
            'timestamp': data['location']['localtime_epoch']
        }
    else:
        return None

locations = ['Paris', 'London', 'New York']

while True:
    weather_data = []
    for location in locations:
        data = fetch_weather_data(location)
        if data:
            weather_data.append(data)
            print(f"Fetched data for {location}: {data}")
        else:
            print(f"Failed to fetch data for {location}")
    
    if weather_data:
        for data in weather_data:
            producer.send('iot-sensors', value=data)
            print(f"Sent: {data}")
    
    time.sleep(15)
