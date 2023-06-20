import boto3
import json
from kafka import KafkaConsumer

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'PinterestDataTopic',
    bootstrap_servers='localhost:9092',
    group_id='batch-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Initialize S3 client with your AWS credentials
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIA5NWFYUTIXNEUXPAZ',
    aws_secret_access_key='utsiIeFMNHv3hR4O7LZDNnldvBqllGmxUFp80EAY',
    region_name='us-west-1'
)

# Create S3 Bucket name
bucket_name = 'pinterest-data-aadf86c2-7783-4f44-a981-f51072a2ba2f'

# Consume messages from Kafka
for message in consumer:
    # Assuming that the messages you're getting from Kafka are JSON serializable
    # JSON file to be stored in S3
    file_name = f"message_{message.offset}.json"
    with open(file_name, 'w') as file:
        json.dump(message.value, file)

    # Upload the file to S3
    s3.upload_file(file_name, bucket_name, file_name)

    print(f'Successfully uploaded {file_name} to {bucket_name}')
