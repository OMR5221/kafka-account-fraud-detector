"""Producer: Creates fake transactions into a Kafka topic."""
# Core:
import os
from time import sleep
import json
import uuid

# Third Party:
# from kafka import KafkaProducer
from confluent_kafka.avro import AvroProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Local:
from transactions import create_random_transaction
from user_accounts import create_user_accounts
from utils.load_avro_schema_file import load_avro_schema_file


TRANSACTIONS_TOPIC = str(os.environ.get('TRANSACTIONS_TOPIC'))
KAFKA_BROKER_URL = str(os.environ.get('KAFKA_BROKER_URL'))
SCHEMA_REGISTRY_URL = str(os.environ.get('SCHEMA_REGISTRY_URL'))
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND


if __name__ == '__main__':

    # Create a set of random user accounts:
    create_user_accounts()

    # Get Avro Schema file:
    key_schema, value_schema = load_avro_schema_file('create-account-action-request.avsc')
    
    print(f"Key Schema: {key_schema}")
    print(f"Value Schema: {value_schema}")

    # Set Producer Config Options
    producer_config = {
        "bootstrap.servers": KAFKA_BROKER_URL,
        "schema.registry.url": SCHEMA_REGISTRY_URL
    }

    producer = AvroProducer(producer_config, 
                            default_key_schema=key_schema, 
                            default_value_schema=value_schema)

    while True:

        transaction: dict = create_random_transaction()
        
        try:
            # Used to decide the partition the data will go to:
            key = str(transaction['sourceAcct'])
            producer.produce(topic=TRANSACTIONS_TOPIC, key=key, value=transaction)
            # producer.send(TRANSACTIONS_TOPIC, value=transaction)
        except Exception as e:
            print(f"Exception while producing transaction {transaction} to Topic {TRANSACTIONS_TOPIC}: {e}")
        else:
            print(f"Successfully produced transaction {transaction} to Topic {TRANSACTIONS_TOPIC}")
            sleep(SLEEP_TIME)
        
        producer.flush()
