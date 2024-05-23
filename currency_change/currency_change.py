# Getting data from table API and calling a function to convert currency
from currency_converter import CurrencyConverter
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import call, col
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.types import DataTypes

@udf(result_type=DataTypes.BIGINT())
def convert_currency(amount, from_currency, to_currency):
    c = CurrencyConverter()
    converted_amount = c.convert(amount, from_currency, to_currency)
    return converted_amount

# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

print("Table Environment created...")

table_env.create_temporary_function("cc", convert_currency)

print("Temporary function created...")

table = table_env.from_elements([(21, "USD", "INR"), (100, "EUR", "USD")], ['input_amount', 'from_currency', 'to_currency'])
#table = table_env.from_elements([(21, 12), (100, 200)], ['a', 'b'])
print("Table created...")
table.execute().print()

revenue = table.select(col("input_amount"), col("from_currency"), col("to_currency"), call("cc", col("input_amount"), col("from_currency"), col("to_currency")).alias("output_amount"), call("cc", 1, col("from_currency"), col("to_currency")).alias("rate"))
print("Revenue computed...")
revenue.print_schema()
revenue.execute().print()

