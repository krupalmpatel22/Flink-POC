from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import call, col
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.types import DataTypes
from currency_converter import CurrencyConverter
  
c = CurrencyConverter()
@udf(result_type=DataTypes.DOUBLE()) 
def convert_currency(amount, from_currency, to_currency):    
    converted_amount = c.convert(amount, from_currency, to_currency)
    # converted_amount = amount * 2
    return round(converted_amount, 2)


# create a streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)

table_env.get_config().set("pipeline.jars", "file:///C:/Users/krupa/Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar")
table_env.get_config().set("pipeline.classpaths", "file:///C:/Users/krupa/Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar")
# table_env.set_python_requirements(
#     requirements_file_path="C:/Users/krupa/OneDrive/Desktop/Flink-POC/requirements.txt",
#     requirements_cache_dir="C:/Users/krupa/OneDrive/Desktop/Flink-POC/.venv"
# )
# table_env.get_config().set_python_executable("C:/Users/krupa/OneDrive/Desktop/Flink-POC/.venv/Scripts/python.exe")

table_env.register_function("cc", convert_currency)

KAFKA_CONNECT= """
CREATE TABLE KafkaTable (
  `input_amount` BIGINT,
  `from_currency` STRING,
  `to_currency` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'test_topic',
  'properties.bootstrap.servers' = '192.168.29.89:9092',
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
# print("New table created successfully")
# table = table_env.from_elements([(21, "CAD", "INR"), (100, "AUD", "USD")], ['input_amount', 'from_currency', 'to_currency'])

# print("Table created...")
# table.execute().print()

revenue = table.select(
    col("input_amount"), 
    col("from_currency"), 
    col("to_currency"),
    call("cc", col("input_amount"), col("from_currency"), col("to_currency")).alias("output_amount"),
    call("cc", 1, col("from_currency"), col("to_currency")).alias("rate")
)

revenue.print_schema()
revenue.execute().print()


# KAFKA_SINK= """
# CREATE TABLE sink (
#   `input_amount` BIGINT,
#   `from_currency` STRING,
#   `to_currency` STRING,
#   `output_amount` DOUBLE,
#   `rate` DOUBLE
# ) WITH (
#   'connector' = 'kafka',
#   'topic' = 'currency_conversion_output',
#   'properties.bootstrap.servers' = 'localhost:9092',
#   'properties.group.id' = 'testGroup',
#   'scan.startup.mode' = 'earliest-offset',
#   'format' = 'json'
# )
# """

# table_env.execute_sql(KAFKA_SINK)
# print("Kafka sink table created successfully")

# revenue.execute_insert("sink").wait()