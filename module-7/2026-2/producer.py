"""
Q2: Kafka Producer - sends green taxi trip data to 'green-trips' topic.
Run outside Docker: uses localhost:9092
"""
import json
from time import time

import pandas as pd
from kafka import KafkaProducer

TOPIC = "green-trips"
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def main():
    print("Loading data...")
    df = pd.read_parquet(DATA_URL, columns=COLUMNS)

    # Convert datetime columns to strings for JSON serialization
    df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Fill NaN with None for JSON compatibility
    df = df.where(pd.notnull(df), None)

    print(f"Rows to send: {len(df)}")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer,
    )

    t0 = time()
    for row in df.to_dict(orient="records"):
        producer.send(TOPIC, value=row)
    producer.flush()
    t1 = time()

    print(f"Took: {t1 - t0:.2f} seconds")


if __name__ == "__main__":
    main()
