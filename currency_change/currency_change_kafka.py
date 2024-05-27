from currency_converter import CurrencyConverter
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import call, col
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.types import DataTypes
from Config import config

@udf(result_type=DataTypes.BIGINT())
def convert_currency(amount, from_currency, to_currency):
    """
        Convert the amount from one currency to another.
        param amount: The amount to convert.
        param from_currency: The currency of the amount.
        param to_currency: The currency to convert to.
    """
    c = CurrencyConverter()
    converted_amount = c.convert(amount, from_currency, to_currency)
    return  int(converted_amount)

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

print("Table Environment created...")

table_env.create_temporary_function("cc", convert_currency)

print("Temporary function created...")

SQL_QUERY = f"""
    CREATE TABLE currency_change_input (
        input_amount BIGINT,
        from_currency STRING,
        to_currency STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{config.get("INPUT_TOPIC")}',
        'properties.bootstrap.servers' = '{config.get("BOOTSTRAP_SERVER")}',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.get("API_KEY")}" password="{config.get("API_SECRET")}";',
        'format' = 'json'
    );
"""
print(SQL_QUERY)

table_env.execute_sql(SQL_QUERY)

print("Input table created...")
table = table_env.from_path("currency_change_input")
table.print_schema()

revenue = table.select(col("input_amount"), col("from_currency"), col("to_currency"), call("cc", col("input_amount"), col("from_currency"), col("to_currency")).alias("output_amount"), call("cc", 1, col("from_currency"), col("to_currency")).alias("rate"))
print("computed completed...")
revenue.print_schema()
revenue.execute().print()

# SQL_QUERY = f"""
#     CREATE TABLE currency_change_output (
#         input_amount BIGINT,
#         from_currency STRING,
#         to_currency STRING,
#         output_amount BIGINT,
#         rate DOUBLE
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = '{config.get("OUTPUT_TOPIC")}',
#         'properties.bootstrap.servers' = '{config.get("BOOTSTRAP_SERVER")}',
#         'properties.security.protocol' = 'SASL_SSL',
#         'properties.sasl.mechanism' = 'PLAIN',
#         'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.get("API_KEY")}" password="{config.get("API_SECRET")}";',
#         'format' = 'json'
#     );
# """
# print(SQL_QUERY)

# table_env.execute_sql(SQL_QUERY)

# print("Output table created...")
# revenue.execute_insert("currency_change_output")


