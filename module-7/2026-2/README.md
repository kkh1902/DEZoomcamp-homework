# Module 7 Homework (2026-2)

Streaming with Kafka (Redpanda) and PyFlink using Green Taxi Trip data (October 2025).

## Setup

```bash
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

## Question 1 - Redpanda version

```bash
docker exec -it workshop-redpanda-1 rpk version
```

Answer
```
v24.2.18
```

## Question 2 - Send data to Redpanda

```bash
# Create topic
docker exec -it workshop-redpanda-1 rpk topic create green-trips

# Install deps
pip install kafka-python pandas pyarrow

# Run producer (from this directory)
python producer.py
```

Answer
```
10 seconds
```

## Question 3 - Consumer: trips with distance > 5 km

```bash
python consumer.py
```

Answer
```
8506
```

## Questions 4-6 - PyFlink

Copy job files to workshop src directory:
```bash
cp src/job/*.py /path/to/07-streaming/workshop/src/job/
```

Create PostgreSQL tables:
```bash
docker exec -i workshop-postgres-1 psql -U postgres < setup_postgres.sql
```

### Q4 - 5-minute tumbling window (PULocationID)

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_location.py
```

```sql
SELECT "PULocationID", num_trips
FROM q4_tumbling_location
ORDER BY num_trips DESC
LIMIT 3;
```

Answer
```
74
```

### Q5 - Session window (5-min gap)

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_window.py
```

```sql
SELECT "PULocationID", num_trips
FROM q5_session_window
ORDER BY num_trips DESC
LIMIT 3;
```

Answer
```
81
```

### Q6 - 1-hour tumbling window (total tips)

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6_hourly_tips.py
```

```sql
SELECT window_start, total_tip
FROM q6_hourly_tips
ORDER BY total_tip DESC
LIMIT 3;
```

Answer
```
2025-10-16 18:00:00
```
