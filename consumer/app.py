"""Example Kafka consumer."""

import json
import os

# from kafka import KafkaConsumer, KafkaProducer
from confluent_kafka.avro import AvroConsumer, AvroProducer
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime


KAFKA_BROKER_URL = str(os.environ.get('KAFKA_BROKER_URL'))
TRANSACTIONS_TOPIC = str(os.environ.get('TRANSACTIONS_TOPIC'))
SCHEMA_REGISTRY_URL = str(os.environ.get('SCHEMA_REGISTRY_URL'))
LEGIT_TOPIC = os.environ.get('LEGIT_TOPIC')
FRAUD_TOPIC = os.environ.get('FRAUD_TOPIC')

# Implement logic based on user spend_type:
def is_suspicious(transaction: dict) -> bool:
    """Determine whether a transaction is suspicious."""
    if transaction['customerSpendType'] == 0:
        return transaction['amount'] > 100
    elif transaction['customerSpendType'] == 1:
        return transaction['amount'] > 500
    elif transaction['customerSpendType'] == 2:
        return transaction['amount'] > 1000

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

def consume_batch(self, num_messages=1, timeout=None):
    """
    This is an overriden method from confluent_kafka.Consumer class. This handles batch of message
    deserialization using avro schema

    :param int num_messages: number of messages to read in one batch (default=1)
    :param float timeout: Poll timeout in seconds (default: indefinite)
    :returns: list of messages objects with deserialized key and value as dict objects
    :rtype: Message
    """
    messages_out = []
    if timeout is None:
        timeout = -1
    messages = super(AvroConsumer, self).consume(num_messages=num_messages, timeout=timeout)
    if messages is None:
        return None
    else:
        for m in messages:
            if not m.value() and not m.key():
                return messages
            if not m.error():
                if m.value() is not None:
                    decoded_value = self._serializer.decode_message(m.value())
                    m.set_value(decoded_value)
                if m.key() is not None:
                    decoded_key = self._serializer.decode_message(m.key())
                    m.set_key(decoded_key)
                messages_out.append(m)
    #print(len(message))
    return messages_out


if __name__ == '__main__':

    default_group_name = "default-consumer-group"

    # Push messages to Transactions Topic
    # producer = AvroProducer(bootstrap_servers=KAFKA_BROKER_URL, value_serializer=lambda value: json.dumps(value).encode())

    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER_URL,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "group.id": default_group_name,
        "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    # Consumer: Read from Transactions Topic
    print("Created Consumer")

    consumer.subscribe([TRANSACTIONS_TOPIC])
    print(f"Consumer subscribed to {TRANSACTIONS_TOPIC}")
    
    raw_messages = []

    # As we read messages pushed from producer to the consumer - classify: 
    while True:
        try:
            message = consumer.poll(5)
            print(f"Polled for message: {message}")
        except SerializerError as e:
            # print(f"Exception while trying to poll messages: {e}")
            print("Message deserialization failed for {}: {}".format(message, e))
            # break
        if message is not None:
            print(f"Successfully polled records from KAFKA TOPIC: {TRANSACTIONS_TOPIC}")
            print(f"Message Value: {message.value()}")
            consumer.commit()
            raw_messages.append(message.value())

            print(f"Raw Messages Length: {len(raw_messages)}")

            transaction: dict = message.value()
            topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
            # producer.send(topic, value=transaction)
            
            print(topic, transaction)  # DEBUG
            
            # Load to S3 Bucket:
            if len(raw_messages) > 0 and len(raw_messages) % 1000 == 0:
                utc_timestamp = datetime.utcnow().timestamp()
                upload_list_to_s3('kafka-fraud-detector', f'transactions_{utc_timestamp}.json', raw_messages)
                raw_messages = []
        elif message is None:
            print("No new messages found at this time.")
            continue
        elif message.error():
            print("AvroConsumer error: {}".format(message.error()))
            continue

        print(message.value())

    consumer.close()
