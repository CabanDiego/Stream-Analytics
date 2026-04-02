"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse
from datetime import datetime, timezone


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    # TODO: Implement

    #Seperating passed string to obtain every topic
    topic_list = [t.strip() for t in topic.split(",")]

    #Consumer object
    consumer = KafkaConsumer(
        bootstrap_servers='kafka:9092',
        group_id='real_time_consumer_group',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    #Subscribing to every topic listed
    consumer.subscribe(topic_list)
    #Starting time and looping through messages in topics and saving them to a list
    start_time = time.time()
    messages = []
    while time.time() - start_time < batch_duration_sec:
        #Using poll to get all new messages that arrived in the last second
        poll_msg = consumer.poll(timeout_ms=1000)
        #Looping through each message and appending to the list
        for msg in poll_msg.values():
            for m in msg:
                messages.append({
                    "Topic": m.topic,
                    "Data": m.value
                })

    #Creating new landing zone file based on the current time
    os.makedirs(output_path, exist_ok=True)
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_path, f"{timestamp_str}_batch.json")

    with open(file_path, "w")as f:
        json.dump(messages, f, indent=2)
    
    consumer.close()

    return len(messages)


if __name__ == "__main__":
    # TODO: Parse args and call consume_batch
    parser = argparse.ArgumentParser(description="Kafka consumer")
    parser.add_argument("--topics", type=str, required=True, help="Comma separated Kafka topics")
    parser.add_argument("--duration", type=int, default=40, help="Batch duration in seconds")
    parser.add_argument("--output", type=str, default="/opt/spark-data/landing", help="Output directory for JSON files")
    
    args = parser.parse_args()
    
    count = consume_batch(args.topics, args.duration, args.output)
    print(f"{count} messages were recieved")