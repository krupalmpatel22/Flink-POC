from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# create a streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

table_env.get_config().set("pipeline.jars", "file:///C:/Users/krupa/Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar")
table_env.get_config().set("pipeline.classpaths", "file:///C:/Users/krupa/Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar")

KAFKA_CONNECT= """
CREATE TABLE KafkaTable (
  `input_amount` BIGINT,
  `from_currency` STRING,
  `to_currency` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
)
"""

table_env.execute_sql(KAFKA_CONNECT)
print("Kafka table created successfully")
table = table_env.from_path("KafkaTable")

table.print_schema()
table.execute().print()
