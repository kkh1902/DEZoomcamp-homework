"""
Q6: 1-hour tumbling window - total tip_amount per hour (all locations).
Submit: docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6_hourly_tips.py

PostgreSQL setup (run before submitting job):
  CREATE TABLE q6_hourly_tips (
      window_start TIMESTAMP,
      total_tip DOUBLE,
      PRIMARY KEY (window_start)
  );

Query for answer:
  SELECT window_start, total_tip
  FROM q6_hourly_tips
  ORDER BY total_tip DESC
  LIMIT 3;
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_source(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """)


def create_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE q6_hourly_tips (
            window_start TIMESTAMP(3),
            total_tip DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q6_hourly_tips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_source(t_env)
    create_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO q6_hourly_tips
        SELECT
            window_start,
            SUM(tip_amount) AS total_tip
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start
    """).wait()


if __name__ == "__main__":
    main()
