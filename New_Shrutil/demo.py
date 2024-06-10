import json
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(r"file:///C:/Users/shrut/Downloads/flink-sql-connector-kafka_2.11-1.13.0.jar")

    env.set_parallelism(1)
    # Define the file path from which to read the JSON data
    type_info = Types.ROW_NAMED(
        ["uuid", "balance_value", "timestamp"],
        [Types.STRING(), Types.INT(), Types.STRING()]
    )

    json_file_path = r'C:\D-Drive\PythonProject\New_Shrutil\Data Storage\file1_20240607_170826.json'
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    ds = env.from_collection(data,
                             type_info=type_info)
    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()

    kafka_producer = FlinkKafkaProducer(
        topic='flink_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()