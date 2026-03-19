"""
Q3: Kafka Consumer - counts trips with trip_distance > 5.0 km.
Run outside Docker: uses localhost:9092
"""
import json

from kafka import KafkaConsumer, TopicPartition

TOPIC = "green-trips"

consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=5000,
)
consumer.assign([TopicPartition(TOPIC, 0)])
consumer.seek_to_beginning()

count_over_5 = 0
total = 0

print("Reading messages...")
for message in consumer:
    record = message.value
    total += 1
    dist = record.get("trip_distance")
    if dist is not None and float(dist) > 5.0:
        count_over_5 += 1

print(f"Total trips: {total}")
print(f"Trips with trip_distance > 5.0: {count_over_5}")
