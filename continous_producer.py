from kafka import KafkaProducer
import requests
import time
import json

# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_data_to_kafka(topic, data):
    # Send data to Kafka topic
    producer.send(topic, value=data)
    producer.flush()
    print("Data sent to Kafka topic:", topic)

# Example POST request
def make_post_request():
    url = "http://localhost:8000/pin/"
    data = {
        'key1': 'value1',
        'key2': 'value2'
    }
    while True:
        response = requests.post(url, json=data)
        send_data_to_kafka('PinterestDataTopic', response.json())
        time.sleep(1)  # wait for 1 second

# Make the POST request and send data to Kafka
make_post_request()
