from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
import json
from pyflink.common.serialization import SimpleStringSchema

# Define a UDF to add two numbers
class AddNumbers(MapFunction):
    def map(self, value):
        number1, number2 = value
        return number1 + number2

def main():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Set up Kafka consumer
    kafka_serde_info = Types.ROW([Types.INT(), Types.INT()])
    kafka_consumer = FlinkKafkaConsumer(
        topics='number_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'flink-group'}
    )
    kafka_consumer.set_start_from_latest()

    # Read from Kafka
    kafka_stream = env.add_source(kafka_consumer)

    # Deserialize JSON messages
    deserialized_stream = kafka_stream.map(lambda x: json.loads(x)).map(
        lambda x: (x['number_1'], x['number_2']), output_type=kafka_serde_info)

    # Apply UDF to add numbers
    result_stream = deserialized_stream.map(AddNumbers(), output_type=Types.INT())

    # Print the results
    result_stream.print()

    # Execute the job
    env.execute("Kafka Flink Add Numbers Job")

if __name__ == '__main__':
    main()
