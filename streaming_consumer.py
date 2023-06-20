from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'PinterestDataTopic', 
    bootstrap_servers='localhost:9092',
    group_id='stream-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

for message in consumer:
    print(f'Processing: {message.value}')
