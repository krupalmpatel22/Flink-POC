from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pymongo import MongoClient

import json

# Function to transform a string record to a JSON dictionary
def string_to_json(record):
    return json.loads(record)

# Function to store records in bulk to MongoDB

def insert_to_mongo(records):
    client = MongoClient('mongodb+srv://shrutilthoria50:iQh9DkjSCq7FTRZX@cluster0.uvn3bbk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
    db = client['IBM_Project']  # Replace 'your_database' with your database name
    collection = db['flinkPOC']  # Replace 'your_collection' with your collection name

    # Perform a bulk insert operation
    try:
        collection.insert_one(records)
        print("Bulk data inserted successfully")
    except Exception as e:
        print("Error inserting bulk data:", e)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)  # Set the parallelism

    # Define Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics("test3") \
        .set_group_id("default-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Define watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    # Create a data stream source from the defined Kafka source
    data_stream = env.from_source(
        source=source,
        watermark_strategy=watermark_strategy,  # Apply the watermark strategy
        source_name="Kafka Source"
    )

    # Transform the string records to JSON dictionaries
    dict_stream = data_stream.map(string_to_json)

    # Store records in bulk to MongoDB
    dict_stream.map(insert_to_mongo)

    try:
        env.execute("Test Kafka Integration")
    except Exception as e:
        print("Job failed:", e)

if __name__ == "__main__":
    main()
