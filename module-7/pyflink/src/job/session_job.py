from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_green_trips_source_kafka(t_env):
    """
    Kafka source table for green-trips
    - dropoff_time: event-time 컬럼
    - watermark: dropoff_time - 5 seconds (요구사항 충족)
    - proc_time: Processing Time (세션 종료를 위해 사용)
    """
    table_name = "green_trips"

    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID STRING,
            DOLocationID STRING,
            passenger_count STRING,
            trip_distance STRING,
            tip_amount STRING,

            -- event time
            dropoff_time AS TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR dropoff_time AS dropoff_time - INTERVAL '5' SECOND,

            -- processing time (세션 종료용)
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_results_sink(t_env):
    sink_ddl = """
        CREATE TABLE session_results (
            PULocationID BIGINT,
            DOLocationID BIGINT,
            trip_count BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'session_results',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(sink_ddl)



def green_trips_session_window():
    """
    Question 5: Sessionization Window (5 minutes)
    - Session window: 5 minutes
    - Watermark: dropoff_time - 5 seconds
    - Use Processing Time to close sessions (과거 데이터 대응)
    """

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # tables
    source_table = create_green_trips_source_kafka(t_env)
    create_session_results_sink(t_env)

    print("=" * 80)
    print("Starting Session Window Job (5 minutes)")
    print("Kafka topic : green-trips")
    print("Sink table  : session_results")
    print("=" * 80)

    table_result = t_env.execute_sql("""
    INSERT INTO session_results
    SELECT
        CAST(PULocationID AS BIGINT) AS PULocationID,
        CAST(DOLocationID AS BIGINT) AS DOLocationID,
        COUNT(*) AS trip_count
    FROM green_trips
    GROUP BY
        CAST(PULocationID AS BIGINT),
        CAST(DOLocationID AS BIGINT),
        SESSION(proc_time, INTERVAL '5' MINUTE);

                        
    """)






if __name__ == "__main__":
    green_trips_session_window()
