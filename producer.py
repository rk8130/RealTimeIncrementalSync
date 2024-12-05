import datetime
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


# Connect to MySQL database
mydb = mysql.connector.connect(
    host=mysql_config['host'], 
    user=mysql_config['user'], 
    passwd=mysql_config['password'], 
    database=mysql_config['database']
)

mycursor = mydb.cursor()


# Load the last read timestamp from the config file
config_data = {}

try:
    with open('config.json') as f:
        config_data = json.load(f)
        last_read_timestamp = config_data.get('last_read_timestamp')
except FileNotFoundError:
    pass

# Set a default value for last_read_timestamp
if last_read_timestamp is None:
    last_read_timestamp = '1900-01-01 00:00:00'


query = "SELECT * FROM product WHERE last_updated > '{}'".format(last_read_timestamp)

# Fetch the data from MySQL database
mycursor.execute(query)

rows=mycursor.fetchall()


for row in rows:
    key = str(row[0])
    value = row[:]
    print(key,":",value)
    # producer.produce(topic='product_updates', key=key, value=value, on_delivery=delivery_report)
    # producer.poll(0)
    # producer.flush()

# Fetch any remaining rows to consume the result
mycursor.fetchall()

query = "SELECT MAX(last_updated) FROM product"
mycursor.execute(query)

# Fetch the result
result = mycursor.fetchone()
max_date = result[0]  # Assuming the result is a single value

# Convert datetime object to string representation
max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S")

# Update the value in the config.json file
config_data['last_read_timestamp'] = max_date_str

with open('config.json', 'w') as file:
    json.dump(config_data, file)

# Close the cursor and database connection
mycursor.close()
mydb.close()

print('All messages are delivered')  