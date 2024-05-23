import os
from Config import config
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, KafkaOffsetsInitializer
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray
from pyflink.common.serialization import SimpleStringSchema

def word_count(kafka_brokers, kafka_topic, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Construct the path to the Kafka connector JAR file
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-connector-kafka_2.11-1.19.0.jar')
    kafka_jar_sql = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka_2.11-1.19.0.jar')

    # Set the pipeline.jars configuration option
    env.add_jars("file://{}".format(kafka_jar))
    env.add_jars("file://{}".format(kafka_jar_sql))

    print("Kafka connector JAR added to the classpath.")

    # Configure Kafka connection properties
    conf = {
        'bootstrap.servers': config.get('BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': config.get('API_KEY'),
        'sasl.password': config.get('API_SECRET'),
        'group.id': 'default',
        'auto.offset.reset': 'latest'
    }

    java_topics = to_jarray(get_gateway().jvm.java.lang.String, kafka_topic)

    # Define the Kafka source using KafkaSourceBuilder
    kafka_consumer = FlinkKafkaConsumer(
        topics=java_topics,
        deserialization_schema=SimpleStringSchema(),  # Use SimpleStringSchema here
        properties=conf
    )

    print("Kafka Source Created.")

    ds = env.add_source(kafka_consumer)

    # ds = env.from_source(
    #     source=kafka_source_builder,
    #     watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    #     source_name="kafka_source"
    # )

    def split(line):
        yield from line.split()

    # compute word count
    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    kafka_topics = [config.get('TOPIC')]
    print(f"Kafka topics: {kafka_topics}")
    word_count(kafka_brokers=config.get('BOOTSTRAP_SERVER'), kafka_topic=kafka_topics, output_path=None)
