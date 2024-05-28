from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

# Initialize the streaming execution environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Define the Kafka source table
t_env.execute_sql("""
    CREATE TABLE kafka_source (
        input_amount BIGINT,
        from_currency STRING,
        to_currency STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'test-topic',
        'properties.bootstrap.servers' = 'localhost:9092',b 
        'properties.group.id' = 'test-group',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
""")

# Query the Kafka source table
result_table = t_env.sql_query("SELECT * FROM kafka_source")

# Convert the result table to a DataStream and print to the console
result_stream = t_env.to_append_stream(result_table)
result_stream.print()

# Execute the Flink job
env.execute("Kafka to Console")
