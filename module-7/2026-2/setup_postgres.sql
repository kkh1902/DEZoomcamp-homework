-- Run before submitting Flink jobs:
-- docker exec -it workshop-postgres-1 psql -U postgres -f /path/to/setup_postgres.sql

-- Q4: 5-minute tumbling window per PULocationID
DROP TABLE IF EXISTS q4_tumbling_location;
CREATE TABLE q4_tumbling_location (
    window_start TIMESTAMP,
    "PULocationID" INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, "PULocationID")
);

-- Q5: Session window per PULocationID
DROP TABLE IF EXISTS q5_session_window;
CREATE TABLE q5_session_window (
    "PULocationID" INT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    num_trips BIGINT
);

-- Q6: Hourly tip totals
DROP TABLE IF EXISTS q6_hourly_tips;
CREATE TABLE q6_hourly_tips (
    window_start TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
