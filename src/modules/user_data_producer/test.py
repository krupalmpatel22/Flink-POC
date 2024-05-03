from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
#from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.common import TypeInformation
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pymongo import MongoClient
from pyflink.common import Configuration
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)  # Set the parallelism
    # config = Configuration()

    # Set the address and port of the JobManager
    # config.set_string("jobmanager.rpc.address", "26e924112b85")
    # config.set_string("jobmanager.rpc.address", "flink-jobmanager")
    # config.set_string("jobmanager.rpc.port", "6123")

    # env.configure(config)

    # env.add_jars("/opt/flink/lib/flink-connector-kafka_2.11-1.14.6.jar",
    #                "/opt/flink/lib/kafka-clients-2.4.1.jar")
    #
    # properties = {
    #     'bootstrap.servers': 'localhost:9092',
    #     'group.id': 'test-group',
    #     'key.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
    #     'value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
    #     'auto.offset.reset': 'earliest'
    # }
    source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics("test") \
        .set_group_id("default-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    # Create a data stream source from the defined Kafka source
    data_stream = env.from_source(
        source=source,
        watermark_strategy=watermark_strategy,  # Apply the watermark strategy
        source_name="Kafka Source"
    )

    def string_to_json(record):
        print(record)
        # try:
        #     # Convert the string record to a dictionary
        #     record_dict = json.loads(record)
        #     return record_dict
        # except json.decoder.JSONDecodeError as e:
        #     # Handle JSONDecodeError
        #     print("Error decoding JSON for record:", record)
        #     print("JSONDecodeError:", e)
        #     return None
    def store_to_mongodb(record):
        print(type(record))
        # client = MongoClient('mongodb+srv://shrutilthoria50:iQh9DkjSCq7FTRZX@cluster0.uvn3bbk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
        # db = client['IBM_Project']  # Replace 'your_database' with your database name
        # collection = db['flinkPOC']  # Replace 'your_collection' with your collection name
        # collection.insert_one(record)

    dict_stream = data_stream.map(string_to_json)

    # Filter out records that failed to parse as JSON
    valid_dict_stream = dict_stream.filter(lambda x: x is not None)

    # Print the resulting dictionary stream
    valid_dict_stream.print()

    try:
        env.execute("Test Kafka Integration")
    except Exception as e:
        print("Job failed:", e)

if __name__ == "__main__":
    main()
