# src/scripts/send_data.py
import json
import pandas as pd
from kafka import KafkaProducer
import time

# Function to serialize data to JSON format
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka server and topic details
server = 'localhost:9092'
topic = 'green-trips'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# Read the dataset
df = pd.read_csv('./green_tripdata_2019-10.csv.gz', usecols=[
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
])

# Send data to Kafka
start_time = time.time()

for index, row in df.iterrows():
    data = row.to_dict()  # Convert the row to a dictionary
    producer.send(topic, data)

producer.flush()
end_time = time.time()

print(f"Time taken to send data: {round(end_time - start_time)} seconds")
