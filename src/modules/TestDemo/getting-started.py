"""
getting-started.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kafka topic
    3. Creates a sink table writing to another Kafka topic
    4. Inserts the source table data into the sink table after enrichment
"""

from pyflink.table import EnvironmentSettings, TableEnvironment
import os
import json

# 1. Creates a Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

statement_set = table_env.create_statement_set()

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda

is_local = True if os.environ.get("IS_LOCAL") else False

if is_local:
    # Only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///" + CURRENT_DIR + "/lib/flink-connector-kafka_2.11-1.14.6.jar",
    )

def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))

def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]

def create_source_table(table_name, topic_name, region):
    return """ CREATE TABLE {0} (
                ticker VARCHAR(6),
                price DOUBLE,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
              )
              WITH (
                'connector' = 'kafka',
                'topic' = '{1}',
                'properties.bootstrap.servers' = '20fc37abfb6f4c0735c1ceb70634e2c34544741abedf1e08dac604e693b5a302:9092',
                'properties.group.id' = 'flink-consumer-group',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, topic_name, region
    )

def create_print_table(table_name,output_topic, output_region):
    return """ CREATE TABLE {0} (
                ticker VARCHAR(6),
                price DOUBLE,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
              )
              WITH (
                'connector' = 'print'
              ) """.format(
        table_name
    )

def create_sink_table(table_name, topic_name, region):
    return """ CREATE TABLE {0} (
                ticker VARCHAR(6),
                price DOUBLE,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
              )
              WITH (
                'connector' = 'kafka',
                'topic' = '{1}',
                'properties.bootstrap.servers' = '20fc37abfb6f4c0735c1ceb70634e2c34544741abedf1e08dac604e693b5a302:9092',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, topic_name, region
    )

def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    producer_property_group_key = "producer.config.0"

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"

    output_stream_key = "output.stream.name"
    output_region_key = "aws.region"

    # Tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # Get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, producer_property_group_key)

    input_topic = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]

    output_topic = output_property_map[output_stream_key]
    output_region = output_property_map[output_region_key]

    print("1",input_table_name,input_topic,input_region)
    # 2. Creates a source table from a Kafka Data Stream
    table_env.execute_sql(
        create_source_table(input_table_name, input_topic, input_region)
    )
    table_env.execute_sql(
        f"SELECT * FROM {input_table_name};"
    ).print()

    # print("2", output_table_name, output_topic, output_region)
    # 3. Creates a sink table writing to another Kafka Data Stream
    # table_env.execute_sql(
    #     create_print_table(output_table_name, output_topic, output_region)
    # ).print()



    # 4. Inserts the source table data into the sink table
    # table_result = table_env.execute_sql("INSERT INTO {0} SELECT * FROM {1}"
    #                                      .format(output_table_name, input_table_name))
    #
    # table_result.wait()
    # if is_local:
    #     table_result.wait()
    #
    # else:
    #     # Get job status through TableResult
    #     print(table_result.get_job_client().get_job_status())

if __name__ == "__main__":
    main()
