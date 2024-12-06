# RealTimeInventorySync

**RealTimeInventorySync** is a real-time data streaming system for the e-commerce platform **BuyOnline**. This system streams product updates from a MySQL database to Kafka and deserializes the data into separate JSON files. The project leverages **Kafka**, **MySQL**, **python** and **Avro serialization** for efficient, scalable, and real-time data streaming.

## Project Overview
The project is designed to track product updates in real-time and stream the changes to downstream systems for analysis, monitoring, and reporting. The architecture consists of a **Kafka producer** that fetches **incremental product data** from a **MySQL database**, serializes it into Avro format, and streams it to a **Kafka topic**. **Kafka consumers** then deserialize the Avro data and append it to separate JSON files.

## Key Features:
- **Incremental Data Fetching**: Fetches new and updated records from the MySQL database based on the last update timestamp.
- **Avro Serialization**: Product data is serialized using Avro to ensure efficient, compact, and schema-based data storage.
- **Kafka Streaming**: Utilizes Kafka to handle multi-partitioned topics for scalable and real-time data streaming.
- **JSON File Storage**: Consumers deserialize Avro data and append product updates to separate JSON files.
- **Real-Time Product Monitoring**: The system enables real-time updates for product information, enabling faster analytics and business intelligence.

## Technologies Used
- **Kafka**: Real-time message broker for streaming product data.
- **MySQL**: Database storing product information such as ID, name, category, price, and last updated timestamp.
- **Avro**: Serialization format used for efficient data transmission between the producer and consumer.
- **Python**: The primary programming language used for implementing the producer, consumer, and MySQL integration.
- **Confluent Kafka**: Kafka library used for producing and consuming messages.
- **JSON**: Format for storing product update records on the consumer side.

## Prerequisites
1. **Kafka Cluster**: Set up a Kafka cluster (either locally or using Confluent Cloud).
2. **MySQL Database**: Set up a MySQL database with a product table containing columns for id, name, category, price, and last_updated.
3. **Python**: Ensure Python 3.x is installed.
4. **confluent-kafka** library installed:  
   ```bash  
   pip install confluent-kafka  
   ```  
5. **pandas** library installed:  
   ```bash  
   pip install pandas  
   ```  
6. **mysql-connector-python** library installed:  
   ```bash  
   pip install mysql-connector-python  
   ```

## Getting Started

### Step 1: Setting Up Kafka Cluster

1. Install Kafka or set up a Kafka cluster in Confluent Kafka.
2. Create a topic in Kafka (e.g., `product_updates`).

### Step 2: Configure the Producer and Consumer

1. Update the **topic name** and **Kafka server configurations** in `producer.py` and `consumer.py`.
2. Update **MySQL credentials** in `producer.py` for your database.

### Step 3: Create a table

1. Set up a MySQL database (e.g., `project`)
2. Create table (e.g., `product`) and populate table with sample data.

### Step 4: Run the Producer

Execute the producer to start streaming data to the Kafka topic:

```bash
python producer.py  
```

### Step 4: Run the Consumer(s)

Start one or more consumers to subscribe to the topic (e.g., `product_updates`) and consume data:

```bash
python consumer.py  
```

## How It Works

### Producer Workflow:

1. The Kafka producer queries the MySQL database for new or updated product records.
2. The product data is serialized using Avro and sent to a Kafka topic (product_updates).
3. The producer tracks the last_updated timestamp to fetch only new or updated records in subsequent runs.

### Consumer Workflow:

1. The Kafka consumers subscribes to the `product_updates` Kafka topic.
2. Upon receiving a message, the consumer deserializes the Avro data and processes the product information.
3. Each update is appended to a JSON file, creating a log of product changes.
