from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'PinterestDataTopic', 
    bootstrap_servers='localhost:9092',
    group_id='batch-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

batch_size = 100
batch = []

for message in consumer:
    print('Received a message')  # This line will print a message for every message received
    batch.append(message.value)  
    if len(batch) >= batch_size:
        print(f'Processing {len(batch)} messages.')
        batch = []
