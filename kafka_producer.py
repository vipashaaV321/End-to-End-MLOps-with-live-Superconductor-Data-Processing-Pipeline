import pandas as pd
from confluent_kafka import Producer
import json
import time

# Read CSV file from local system or bucket
csv_file_path = 'train.csv'
df = pd.read_csv(csv_file_path)

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
}

# Initialize Kafka producer
producer = Producer(producer_config)

# Kafka topic
kafka_topic = 'superconductivity_data'

# Delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce messages to Kafka topic
for index, row in df.iterrows():
    data = row.to_dict()
    producer.produce(
        kafka_topic, 
        key=str(index),  # optional: using the index as the key
        value=json.dumps(data).encode('utf-8'), 
        callback=delivery_report
    )
    # Poll to handle delivery reports (callbacks)
    producer.poll(0)
    time.sleep(0.05)

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()

print("Data has been successfully ingested into Kafka.")

