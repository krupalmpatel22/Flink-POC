from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import call, col
import os

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)

print(os.getcwd())
table_env.get_config().set("pipeline.jars", "file:///opt/flink/scripts/flink-sql-connector-kafka-3.1.0-1.18.jar")
table_env.get_config().set("pipeline.classpaths", "file:///opt/flink/scripts/flink-sql-connector-kafka-3.1.0-1.18.jar")


config = table_env.get_config().get_configuration()
pipeline_jars = config.get_string("pipeline.jars", "")
pipeline_classpaths = config.get_string("pipeline.classpaths", "")

print(f"Pipeline Jars: {pipeline_jars}")
print(f"Pipeline Classpaths: {pipeline_classpaths}")

print("Table Environment created successfully")
 
KAFKA_CONNECT= """
CREATE TABLE KafkaTable (
  `number_1` BIGINT,
  `number_2` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'number_topic',
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

result = table.select(
    col("number_1"), 
    col("number_2"), 
    (col("number_1") + col("number_2")).alias("sum")
)
print("Result table created successfully")
result.print_schema()
result.execute().print()