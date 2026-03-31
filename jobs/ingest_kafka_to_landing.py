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

    topic_list = [t.strip() for t in topic.split(",")]

    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9094',
        group_id='real_time_consumer_group',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    consumer.subscribe(topic_list)

    start_time = time.time()
    messages = []
    while time.time() - start_time < batch_duration_sec:
        for msg in consumer:
            messages.append({
                "Topic"  : msg.topic,
                "Data" : msg.value
            })
            if time.time() - start_time >= batch_duration_sec:
                break

    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, "raw_data.json")

    with open(file_path, "a")as f:
        json.dump(messages, f)
    
    consumer.close()

    return len(messages)


if __name__ == "__main__":
    # TODO: Parse args and call consume_batch
    parser = argparse.ArgumentParser(description="Kafka batch consumer")
    parser.add_argument("--topics", type=str, required=True, help="Comma-separated Kafka topics")
    parser.add_argument("--duration", type=int, default=40, help="Batch duration in seconds")
    parser.add_argument("--output", type=str, default="./data/landing", help="Output directory for JSON files")
    
    args = parser.parse_args()
    
    count = consume_batch(args.topics, args.duration, args.output)
    print(f"{count} messages were recieved")