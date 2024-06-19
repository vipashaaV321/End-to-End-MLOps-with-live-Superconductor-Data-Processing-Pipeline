

# query.awaitTermination()
from confluent_kafka import Consumer, KafkaException, KafkaError

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

        # Process message
        print('Received message: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass

except KafkaException as e:
    print(f'KafkaException: {e}')

finally:
    # Close the consumer
    consumer.close()
