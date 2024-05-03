from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
#from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common import TypeInformation
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # env.add_jars("file:///C:/Users/BAPS/Downloads/jar_files/flink-connector-kafka_2.11-1.14.6.jar",
    #                "file:///C:/Users/BAPS/Downloads/jar_files/kafka-clients-2.4.1.jar")
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
        .set_topics("quickstart") \
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

    # Print the data stream to the console
    data_stream.print()

    try:
        env.execute("Test Kafka Integration")
    except Exception as e:
        print("Job failed:", e)

if __name__ == "__main__":
    main()
