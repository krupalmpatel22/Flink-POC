from currency_converter import CurrencyConverter
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import call, col
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.types import DataTypes

@udf(result_type=DataTypes.DOUBLE())
def convert_currency(amount, from_currency, to_currency):
    """
        Convert the amount from one currency to another.
        param amount: The amount to convert.
        param from_currency: The currency of the amount.
        param to_currency: The currency to convert to.
    """
    c = CurrencyConverter()
    converted_amount = c.convert(amount, from_currency, to_currency)
    # Ensure the returned value is an integer
    return converted_amount

# Create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

print("Table Environment created...")

# Register the UDF
table_env.create_temporary_function("cc", convert_currency)

print("Temporary function created...")

# Create a table with test data
table = table_env.from_elements([(21, "USD", "INR"), (100, "EUR", "USD")], ['input_amount', 'from_currency', 'to_currency'])

print("Table created...")
table.execute().print()

# Use the UDF in a select statement
revenue = table.select(
    col("input_amount"), 
    col("from_currency"), 
    col("to_currency"), 
    call("cc", col("input_amount"), col("from_currency"), col("to_currency")).alias("output_amount"),
    call("cc", 1, col("from_currency"), col("to_currency")).alias("rate")
)

print("computed completed...")
revenue.print_schema()
revenue.execute().print()
