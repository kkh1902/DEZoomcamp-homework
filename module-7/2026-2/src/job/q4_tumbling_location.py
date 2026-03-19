"""
Q4: 5-minute tumbling window - count trips per PULocationID.
Submit: docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_location.py

PostgreSQL setup (run before submitting job):
  CREATE TABLE q4_tumbling_location (
      window_start TIMESTAMP,
      PULocationID INT,
      num_trips BIGINT,
      PRIMARY KEY (window_start, PULocationID)
  );
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
        CREATE TABLE q4_tumbling_location (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q4_tumbling_location',
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
        INSERT INTO q4_tumbling_location
        SELECT
            window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        WHERE PULocationID IS NOT NULL
        GROUP BY window_start, PULocationID
    """).wait()


if __name__ == "__main__":
    main()
