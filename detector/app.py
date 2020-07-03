"""Example Kafka consumer."""

import json
import os

from kafka import KafkaConsumer, KafkaProducer
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
LEGIT_TOPIC = os.environ.get('LEGIT_TOPIC')
FRAUD_TOPIC = os.environ.get('FRAUD_TOPIC')

# Implement logic based on user spend_type:
def is_suspicious(transaction: dict) -> bool:
    """Determine whether a transaction is suspicious."""
    return transaction['amount'] >= 900
    # spend_type == 0: (1,100)
    # spend_type == 1: (1, 500)
    # spend_type == 2: (1, 1000)

def upload_file_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload(local_file, bucket, s3_file)
        print("Upload successful")
        return True
    except FileNotFoundError:
        print("File not found!")
        return False
    except NoCredentialsError:
        print("Crednetials not available")
        return False

def upload_list_to_s3(bucket_nm, output_file_nm, output_data):
    s3 = boto3.resource('s3')
    upload_object = s3.Object(bucket_nm, output_file_nm)
    upload_object.put(Body=(bytes(json.dumps(output_data, indent=2).encode('UTF-8'))))


if __name__ == '__main__':
    # Push messages to Transactions Topic
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    # Consumer: Read from Transactions Topic
    consumer = KafkaConsumer(
        TRANSACTIONS_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    
    raw_messages = []
    # As we read messages pushed from producer to the consumer - classify: 
    for message in consumer:
        raw_messages.append(message.value)
        transaction: dict = message.value
        topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
        producer.send(topic, value=transaction)
        print(topic, transaction)  # DEBUG
        # Load to S3 Bucket:
        if len(raw_messages) > 0 and len(raw_messages) % 1000 == 0:
            utc_timestamp = datetime.utcnow().timestamp()
            upload_list_to_s3('kafka-fraud-detector', f'transactions_{utc_timestamp}.json', raw_messages)
            raw_messages = []
