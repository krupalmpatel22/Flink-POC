import os
import subprocess

# Verify that pyflink is installed
try:
    import pyflink
    print("pyflink is installed.")
except ImportError as e:
    print(f"Error: {e}")
    raise

# Define the UDF
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, call

@udf(result_type=DataTypes.BIGINT())
def add(num1: int) -> int:
    return num1 + 10

# Create a batch TableEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)

print("Table Environment created...")

# Register the UDF
table_env.create_temporary_function("add_func", add)

print("Temporary function created...")

# Create a table with test data
table = table_env.from_elements(
    [(10, "USD", "INR"), (100, "AUD", "USD")],
    DataTypes.ROW([
        DataTypes.FIELD("input_amount", DataTypes.INT()),
        DataTypes.FIELD("from_currency", DataTypes.STRING()),
        DataTypes.FIELD("to_currency", DataTypes.STRING())
    ])
)

print("Table created...")

# Use the UDF in a select statement
result = table.select(
    col("input_amount"), 
    col("from_currency"), 
    col("to_currency"), 
    (col("input_amount") * 2).alias("output_amount_one"),
    call("add_func", col("input_amount")).alias("output_amount_two")
)

# Execute and print the result
print("Executing the table transformation...")
result.execute().print()
