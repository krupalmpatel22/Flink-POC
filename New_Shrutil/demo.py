import sys

import argparse
from typing import Iterable

from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema

from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table.expressions import call, col
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
import json

from pyflink.table import StreamTableEnvironment

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///C:/Users/shrut/Downloads/flink-sql-connector-kinesis-1.15.2.jar")
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)


    # Define the file path from which to read the JSON data
    json_file_path = r'C:\D-Drive\PythonProject\New_Shrutil\Data Storage\file1_20240607_170826.json'

    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Print the data to verify

    data_stream = env.from_collection(data,
                                      type_info=Types.ROW_NAMED(
                                          ["a", "b"],
                                          [Types.STRING(), Types.INT()]))

    data_stream.print()

    t = table_env.from_data_stream(data_stream).alias('uuid','balance_value')

    # register the Table object as a view and query it
    table_env.create_temporary_view("InputTable", t)
    res_table = table_env.sql_query("SELECT * FROM InputTable")

    # interpret the insert-only Table as a DataStream again
    res_ds = table_env.to_data_stream(res_table)

    # add a printing sink and execute in DataStream API
    res_ds.print()
    #
    #
    # x = table.select(col("uuid"),
    # col("balance_value"))
    #
    # x.print_schema()
    #
    #
    #
    # # Define the schema for the Table
    # table_env.create_temporary_view("source_table", table)
    #
    #
    # # Define the Kafka sink table
    # kafka_ddl = """
    # CREATE TABLE kafka_sink (
    #     uuid STRING,
    #     balance_value INT
    #
    # ) WITH (
    #     'connector' = 'kafka',
    #     'topic' = 'quickstart',
    #     'properties.bootstrap.servers' = 'localhost:9092',
    #     'properties.group.id' = 'testGroup',
    #     'format' = 'json',
    #     'scan.startup.mode' = 'earliest-offset'
    # )
    # """
    #
    # # Execute the DDL to create the Kafka sink table
    # table_env.execute_sql(kafka_ddl)
    #
    # # Write the data from the source table to the Kafka sink table
    # table_env.execute_sql("INSERT INTO kafka_sink SELECT uuid, balance_value FROM source_table")
    #
    # # Execute the Flink job
    env.execute("Read JSON File Data Stream Example")