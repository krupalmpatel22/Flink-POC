from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from utils.mongo_utils import MongoUtils
import json
import re
import logging
import os
class FlinkToMongo:
    def __init__(self):
        self.mongo_utils=MongoUtils()
        self.logger = logging.getLogger(__name__)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def string_to_json(self,record):
        try:
            return json.loads(record)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON record: %s", record)
            print("Failed to parse JSON record: %s", record)
            return None

    def enrich_data(self,data):
        for key, value in data.items():
            if value == "":
                data[key] = None

        if 'ingredients' in data and 'INGREDIENTS:' in data['ingredients']:
            # Use regular expression to remove 'INGREDIENTS:' prefix
            data['ingredients'] = re.sub(r'^INGREDIENTS: \s*', '', data['ingredients'])

        return data

    def insert_to_mongo(self,records):
        try:
            collection = self.mongo_utils.mongo_connection(db_name='IBM_Project', collection_name='flinkPOC')
            self.mongo_utils.insert_data(collection, records)
            self.logger.info("Data inserted successfully")
            print("Data inserted successfully")
        except Exception as e:
            self.logger.error("Error inserting data: %s", e)
            print("Error inserting data: %s", e)

    def main(self):
        env = StreamExecutionEnvironment.get_execution_environment()

        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        env.add_jars("file:///" + CURRENT_DIR + "/lib/flink-connector-kafka_2.11-1.14.6.jar")

        env.set_parallelism(1)  # Set the parallelism

        # Define Kafka source
        source = KafkaSource.builder() \
            .set_bootstrap_servers(os.getenv('KAFKA_HOST')) \
            .set_topics("user_data") \
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
        dict_stream = data_stream.map(self.string_to_json).filter(lambda x: x is not None)

        enriched_dict_stream = dict_stream.map(self.enrich_data)
        # Store records in bulk to MongoDB
        enriched_dict_stream.map(self.insert_to_mongo)
        try:
            env.execute("Test Kafka Integration")

        except Exception as e:
            print("Job failed:", e)


obj = FlinkToMongo()
obj.main()
