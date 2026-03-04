import json
import math
from time import time

import pandas as pd
from kafka import KafkaProducer

TOPIC = "green-trips"
DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]


def json_serializer(data):
    return json.dumps(data, allow_nan=False).encode("utf-8")


def sanitize_record(record):
    clean = {}
    for k, v in record.items():
        if isinstance(v, float) and math.isnan(v):
            clean[k] = None
        else:
            clean[k] = v
    return clean


def main() -> None:
    df = pd.read_csv(DATA_URL, compression="gzip", low_memory=False)
    df = df[COLUMNS]

    producer = KafkaProducer(
        bootstrap_servers=["redpanda-1:29092"],
        value_serializer=json_serializer,
    )

    t0 = time()
    for row in df.to_dict(orient="records"):
        producer.send(TOPIC, value=sanitize_record(row))
    producer.flush()
    t1 = time()

    print(f"Rows sent: {len(df)}")
    print(f"Took: {t1 - t0:.2f} seconds")


if __name__ == "__main__":
    main()
