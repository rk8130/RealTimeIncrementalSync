from datetime import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import json

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import mysql.connector



# Define a delivery report callback to log the message delivery result.

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))




# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'YXVAJ4PHZNL5SVMA',
    'sasl.password': 'NWyUC/CXlmeVKB5N+ZAJb7erBDyRoLMftsXCWcs1lbH8x10l81d6F37mrS8rJNCD'
}

# Schema for User record value
schema_registry_client=SchemaRegistryClient({
    'url': 'https://psrc-lo3do.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('U2VGBECL4ZMIH54P', 'QAAP/kLS1XXOMRJ2xkViG6gslGaLVdFrR9ET5JCb2XKk2GQrHmBM6h5ASRTDRCOJ')
})

# Fetch the schema by subject name
subject_name = 'product_updates-value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str


# Avro serializer for User record value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)



# Define the serializiing producer
producer=SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer,
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password']
})


# Define MySQL configuration
mysql_config={
    'host':'localhost',
    'user':'root',
    'password':'12345',
    'database':'project2'
}

# Load the last read timestamp from the config file
config_data = {}

def load_last_timestamp():
    try:
        with open('config.json') as f:
            config_data = json.load(f)
            return config_data.get('last_read_timestamp', '1900-01-01 00:00:00')
    except FileNotFoundError:
        return '1900-01-01 00:00:00'


# Save the last read timestamp to the config file
def save_last_timestamp(timestamp):
    config_data = {'last_read_timestamp': timestamp}
    with open('config.json', 'w') as f:
        json.dump(config_data, f)



while True:
    try:
        mydb = mysql.connector.connect(**mysql_config)
        mycursor = mydb.cursor()

        last_read_timestamp = load_last_timestamp()   # string format

        # Query for new or updated records
        query = "SELECT * FROM product WHERE last_updated > '{}'".format(last_read_timestamp)
        mycursor.execute(query)
        rows = mycursor.fetchall()

        last_read_timestamp = datetime.strptime(last_read_timestamp, "%Y-%m-%d %H:%M:%S")  # convert to datetime object

        if rows:
            for row in rows:
                columns = [column[0] for column in mycursor.description]
                value = dict(zip(columns, row))
                key = str(value['id'])

                producer.produce(
                    topic='product_updates',
                    key=key,
                    value=value,
                    on_delivery=delivery_report
                )
                print(value)
                producer.flush()

                last_read_timestamp=max(last_read_timestamp, value['last_updated'])

            
            save_last_timestamp(last_read_timestamp.strftime("%Y-%m-%d %H:%M:%S"))  # convert to string format

            mycursor.close()
            mydb.close()

        else:
            print("No new records. Retrying...")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        mycursor.close()
        mydb.close()

    # Wait before checking again
    sleep(10)
