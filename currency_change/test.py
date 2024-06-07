from pyflink.table import EnvironmentSettings, TableEnvironment
from Config import config

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

print("Table Environment created...")

SQL_QUERY = f"""
    CREATE TABLE currency_change_input (
        input_amount BIGINT,
        from_currency STRING,
        to_currency STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{config.get("INPUT_TOPIC")}',
        'properties.bootstrap.servers' = '{config.get("BOOTSTRAP_SERVER")}',
        'properties.group.id' = 'demoGroup',
        'scan.startup.mode' = 'earliest-offset',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.get("API_KEY")}" password="{config.get("API_SECRET")}";',
        'format' = 'json'
    );
"""

try:
    print(SQL_QUERY)
    table_env.execute_sql(SQL_QUERY)
    print("Input table created...")
    table = table_env.from_path("currency_change_input")
    table.print_schema()
    print(table.to_pandas())
except Exception as e:
    print("An error occurred:", e)



