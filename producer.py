import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import mysql.connector


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
    'basic.auth.user.info': '{}:{}'.format('U2VGBECL4ZMIH54Pl', 'QAAP/kLS1XXOMRJ2xkViG6gslGaLVdFrR9ET5JCb2XKk2GQrHmBM6h5ASRTDRCOJ')
})

# Fetch the schema by subject name
subject_name = 'product_updates-value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str


# Avro serializer for User record value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_str, schema_registry_client)

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

mydb = mysql.connector.connect(
    host=mysql_config['host'], 
    user=mysql_config['user'], 
    passwd=mysql_config['password'], 
    database=mysql_config['database']
)

mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM product")
rows=mycursor.fetchall()
# Extracting the first column (id) and the rest of the columns (value)
id = [row[0] for row in rows]  # First column
value = [row[1:] for row in rows]  # All columns except the first one
mycursor.close()
mydb.close()






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