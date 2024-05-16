import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from Config.api_config import config
import sys

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print("Environment created successfully.")
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            r'resource\flink-sql-connector-kafka_2.13-1.19.0.jar')
    print(f"Kafka connector jar path: {kafka_jar}")
    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))

    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = f"""
    CREATE TABLE sales_usd (
        seller_id STRING,
        amount_usd DOUBLE,
        sale_ts TIMESTAMP
    )
    WITH (
        'connector' = 'kafka',
        'topic' = '{config.get('INPUT_TOPIC')}',
        'properties.bootstrap.servers' = '{config.get('BOOTSTRAP_SERVER')}',
        'format' = 'json',
        'properties.group.id' = 'demoGroup',
        'scan.startup.mode' = 'earliest-offset',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username={config.get('API_KEY')} password={config.get('API_SECRET')};',
        'sink.partitioner' = 'fixed'
    )
    """

    print(src_ddl)

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('sales_usd')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    sql = """
        SELECT
          seller_id,
          SUM(amount_usd * 0.85) AS window_sales
        FROM sales_usd
        GROUP BY
          seller_id
    """

    print(sql)

    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()
    sys.exit(0)
    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = f"""
        CREATE TABLE sales_euros (
            seller_id VARCHAR,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales_euros',
            'properties.bootstrap.servers' = {config.get('BOOTSTRAP_SERVER')},
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('sales_euros').wait()

    tbl_env.execute('windowed-sales-euros')


if __name__ == '__main__':
    main()