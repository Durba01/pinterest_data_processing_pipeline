from kafka import KafkaProducer
import requests

# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092")

def send_data_to_kafka(topic, data):
    # Convert data to bytes
    data_bytes = data.encode("utf-8")
    
    # Send data to Kafka topic
    producer.send(topic, value=data_bytes)
    producer.flush()
    print("Data sent to Kafka topic:", topic)

# Example POST request
def make_post_request():
    url = "http://localhost:8000/pin/"
    data = {
        'key1': 'value1',
        'key2': 'value2'
    }
    response = requests.post(url, json=data)
    
    # Send response content to Kafka
    send_data_to_kafka('PinterestDataTopic', response.content.decode('utf-8'))

# Make the POST request and send data to Kafka
make_post_request()
