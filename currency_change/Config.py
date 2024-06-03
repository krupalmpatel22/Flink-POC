config = {
    "API_KEY": "2FOSBHNVBHXHIQ36",
    "API_SECRET": "xLfjtCBKMJMmL/z4BScPkZF5TVeyREDmf7s2DJQqhc8lkeuBetDC3gRm+nXCk7Q1",
    "BOOTSTRAP_SERVER": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    "INPUT_TOPIC": "test",
    "OUTPUT_TOPIC": "currency_change_output",
}

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