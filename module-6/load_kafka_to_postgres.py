#!/usr/bin/env python3
import json
import psycopg2
from kafka import KafkaConsumer

def load_to_postgres():
    # Connect to Postgres
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='postgres'
    )
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS green_trips_raw (
            lpep_pickup_datetime TEXT,
            lpep_dropoff_datetime TEXT,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count BIGINT,
            trip_distance DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION
        )
    """)
    conn.commit()

    # Clear existing data
    cur.execute("DELETE FROM green_trips_raw")
    conn.commit()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        'green-trips',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='postgres-loader',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Stop after 10 seconds of no messages
    )

    count = 0
    for message in consumer:
        data = message.value

        cur.execute("""
            INSERT INTO green_trips_raw
            (lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID,
             passenger_count, trip_distance, tip_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data['lpep_pickup_datetime'],
            data['lpep_dropoff_datetime'],
            int(data['PULocationID']),
            int(data['DOLocationID']),
            int(data['passenger_count']),
            float(data['trip_distance']),
            float(data['tip_amount'])
        ))

        count += 1
        if count % 1000 == 0:
            conn.commit()
            print(f"Loaded {count} records...")

    conn.commit()
    print(f"\nTotal records loaded: {count}")

    cur.close()
    conn.close()
    consumer.close()

if __name__ == '__main__':
    load_to_postgres()
