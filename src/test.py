from kafka import KafkaProducer
import json
import time

# Function to serialize data to JSON format
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka server details
server = 'localhost:9092'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# Test the connection
producer.bootstrap_connected()
print("Connected to Kafka server!")
