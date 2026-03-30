'''Reads data stream from kafka topics'''

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transaction_events',
    'user_events',
    bootstrap_servers='localhost:9094',
    group_id='my_consumer_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"Topic: {message.topic}, Value: {message.value}")
