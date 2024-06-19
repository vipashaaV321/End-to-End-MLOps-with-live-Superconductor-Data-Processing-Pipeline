



import os
from confluent_kafka import Consumer, KafkaException, KafkaError
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# Kafka broker(s) comma-separated list
bootstrap_servers = 'localhost:9092'

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my_consumer_group',  # Specify your consumer group here
    'auto.offset.reset': 'earliest'   # Read from the beginning of the topic
}

# Create Kafka consumer
consumer = Consumer(consumer_conf)

# Subscribe to a topic
topic = 'superconductivity_data'  # Replace with your topic name
consumer.subscribe([topic])

# Initialize an empty list to store messages
messages = []
c=0
try:
    while True:
        # Poll for messages
        msg = consumer.poll(timeout=1.0)  # Adjust timeout as needed

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, continue polling
                continue
            else:
                # Handle other errors
                raise KafkaException(msg.error())

        # Decode message value
        message_value = msg.value().decode('utf-8')
        c+=1

        print(message_value,"no",c)
        
        # Append message to list
        messages.append(message_value)

except KeyboardInterrupt:
    pass

except KafkaException as e:
    print(f'KafkaException: {e}')

finally:
    # Close the consumer
    consumer.close()

# If messages were consumed
if messages:
    # Create a DataFrame from the messages
    df = pd.DataFrame(messages, columns=['message'])

    # Convert DataFrame to Arrow Table
    table = pa.Table.from_pandas(df)

    # Generate dynamic file name with current date
    current_date = datetime.now().strftime('%d_%m_%Y')
    filename = f'{current_date}_filename.parquet'

    print("file name generated:",filename)
    # Specify the output directory
    output_dir = 'kafka_parquet_op'  # Replace with your desired directory path

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Write Arrow Table to Parquet file
    parquet_file = os.path.join(output_dir, filename)
    pq.write_table(table, parquet_file)

    print(f'Converted messages to Parquet file: {parquet_file}')
else:
    print('No messages consumed.')
