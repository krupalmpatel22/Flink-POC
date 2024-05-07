from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import RuntimeContext, SinkFunction
from pyflink.common.typeinfo import Types
from pymongo import MongoClient
from pyflink.common import WatermarkStrategy
import json

# Custom SinkFunction for bulk insertion to MongoDB
class MongoDBBulkSink(SinkFunction):
    def __init__(self, uri, db_name, collection_name):
        super().__init__()
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.records_buffer = []

    def open(self, runtime_context: RuntimeContext):
        # Open MongoDB connection
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def invoke(self, value, context):
        # Parse JSON string to dictionary
        record = json.loads(value)
        # Buffer records for bulk insertion
        self.records_buffer.append(record)
        if len(self.records_buffer) >= 1000:  # Insert every 1000 records
            self.collection.insert_many(self.records_buffer)
            self.records_buffer = []

    def close(self):
        # Close MongoDB connection
        self.client.close()

def main():
    # Initialize Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Define Kafka consumer properties
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-group',
        'auto.offset.reset': 'latest'
    }

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    # Create Kafka consumer with STRING deserialization schema
    kafka_source = env.from_source(
        source=kafka_props,
        watermark_strategy=watermark_strategy,  # Apply the watermark strategy
        source_name="Kafka Source"
    )

    # Add Kafka source to the execution environment
    data_stream = env.add_source(kafka_source)

    # Add MongoDB bulk sink to the data stream
    mongo_sink = MongoDBBulkSink('mongodb://localhost:27017', 'my_database', 'my_collection')
    data_stream.add_sink(mongo_sink)

    # Execute the Flink job
    env.execute("Kafka to MongoDB Bulk Insertion")

if __name__ == "__main__":
    main()
