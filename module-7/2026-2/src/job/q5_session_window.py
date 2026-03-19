"""
Q5: Session window (5-min gap) per PULocationID - find longest session.
Submit: docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_window.py

PostgreSQL setup (run before submitting job):
  CREATE TABLE q5_session_window (
      PULocationID INT,
      session_start TIMESTAMP,
      session_end TIMESTAMP,
      num_trips BIGINT
  );

Query for answer:
  SELECT PULocationID, num_trips
  FROM q5_session_window
  ORDER BY num_trips DESC
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
        CREATE TABLE q5_session_window (
            PULocationID INT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q5_session_window',
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
        INSERT INTO q5_session_window
        SELECT
            PULocationID,
            SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS session_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTE) AS session_end,
            COUNT(*) AS num_trips
        FROM green_trips
        WHERE PULocationID IS NOT NULL
        GROUP BY
            SESSION(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """).wait()


if __name__ == "__main__":
    main()
