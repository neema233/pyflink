from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_location INTEGER,
            dropoff_location INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_trips BIGINT,
            PRIMARY KEY (pickup_location, dropoff_location, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'localhost:9092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def session_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Watermark strategy based on lpep_dropoff_datetime
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))  # 5 seconds tolerance
        .with_timestamp_assigner(
            # Use the dropoff datetime as the event time
            lambda event, timestamp: event[1]  # lpep_dropoff_datetime is at index 1
        )
    )

    try:
        # Create Kafka source table
        source_table = create_events_source(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        # Execute the session window aggregation query
        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            PULocationID AS pickup_location,
            DOLocationID AS dropoff_location,
            MIN(event_timestamp) AS session_start,
            MAX(event_timestamp) AS session_end,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                TABLE {source_table},
                DESCRIPTOR(event_watermark),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY PULocationID, DOLocationID, SESSION(event_watermark, INTERVAL '5' MINUTE);
        """).wait()

    except Exception as e:
        print("Error during aggregation:", str(e))

if __name__ == '__main__':
    session_aggregation()
