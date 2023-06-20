import boto3
import uuid

# Create a session using your AWS credentials
session = boto3.session.Session(
    aws_access_key_id='AKIA5NWFYUTIXNEUXPAZ',
    aws_secret_access_key='utsiIeFMNHv3hR4O7LZDNnldvBqllGmxUFp80EAY',
    region_name='us-west-1'  # example: 'us-east-1'
)

# Create an S3 client
s3 = session.client('s3')

# Generate a UUID
bucket_name = 'pinterest-data-' + str(uuid.uuid4())

# Create a bucket with the generated name
s3.create_bucket(
    Bucket=bucket_name,
    CreateBucketConfiguration={
        'LocationConstraint': 'us-west-1'
    }
)

print(f'Bucket {bucket_name} created successfully.')
