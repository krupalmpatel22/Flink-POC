from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import call, col
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.types import DataTypes
from currency_converter import CurrencyConverter
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.common import Row

###############################
#    UDF
###############################
# @udf(result_type=DataTypes.ROW([DataTypes.FIELD('number_1', DataTypes.BIGINT()),
#                                 DataTypes.FIELD('number_2', DataTypes.BIGINT()),
#                                 DataTypes.FIELD('sum', DataTypes.BIGINT())]))
# def add(numbers: Row) -> Row:
#     num1, num2 = numbers
#     sum = num1 + num2
#     return Row(num1, num2, sum)

@udf(result_type=DataTypes.BIGINT())
def add(num1: int, num2: int) -> int:
    return num1 + num2

# create a streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)

# env_settings = EnvironmentSettings.in_streaming_mode()
# table_env = TableEnvironment.create(env_settings)

table_env.get_config().set("pipeline.jars", "file:///C:/Users/krupa/Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar")
table_env.get_config().set("pipeline.classpaths", "file:///C:/Users/krupa/Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar")
# table_env.set_python_requirements(
#     requirements_file_path="C:/Users/krupa/OneDrive/Desktop/Flink-POC/requirements.txt",
#     requirements_cache_dir="C:/Users/krupa/OneDrive/Desktop/Flink-POC/.venv"
# )
# table_env.get_config().set_python_executable("C:/Users/krupa/OneDrive/Desktop/Flink-POC/.venv/Scripts/python.exe")
print("Table Environment created successfully")

# creating temporary function
table_env.register_function("add", add)
print("Temporary function created successfully")
 
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
# table.execute().print()

# result = table.map(add).alias("number_1", "number_2", "sum")

result = table.select(
    col("number_1"), 
    col("number_2"), 
    call("add", col("number_1"), col("number_2")).alias("sum")
)

# result = table.select(
#     col("number_1"), 
#     col("number_2"), 
#     (col("number_1") + col("number_2")).alias("sum")
# )
print("Result table created successfully")
result.print_schema()
result.execute().print()


# PRINT_SINK = """
# CREATE TABLE PrintSink (
#   `number_1` BIGINT,
#   `number_2` BIGINT,
#   `sum` BIGINT
# ) WITH (
#   'connector' = 'print'
# )
# """

# table_env.execute_sql(PRINT_SINK)
# print("Print sink created successfully")
# result.execute_insert("PrintSink").wait()

# table_env.execute("Kafka Flink Job")