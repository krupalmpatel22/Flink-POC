from api_config import config
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

import random

print("Setting up Kafka client")

config_dict = {
    "bootstrap.servers": config["BOOTSTRAP_SERVER"],
    "sasl.mechanisms": "PLAIN",
    "security.protocol": "SASL_SSL",
    "session.timeout.ms": "45000",
    "sasl.username": config["API_KEY"],
    "sasl.password": config["API_SECRET"],
}

client_config = config_dict

# setting up the producer
producer = Producer(client_config)

srconfig = {
    "url": config["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": f"{config['SCHEMA_REGISTRY_KEY']}:{config['SCHEMA_REGISTRY_SECRET']}"
}

# setting up the schema registry connection
schema_registry_client = SchemaRegistryClient(srconfig)

# schema for producer matching one in SPY topic in Confluent Cloud
schema_str = """{
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "bid_timestamp": {
      "description": "The string type is used for strings of text.",
      "type": "string"
    },
    "price": {
      "description": "JSON number type.",
      "type": "number"
    },
    "symbol": {
      "description": "The string type is used for strings of text.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}"""

def serialize_custom_data(data, ctx):
    return data

# setting up the JSON serializer
json_serializer = JSONSerializer(
    schema_str, schema_registry_client, serialize_custom_data
)
def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f"delivered new event from producer")

for i in range(1):
    data = {
        "bid_timestamp": str(random.randint(1, 5)),
        "price": str(random.randint(1, 100)),
        "symbol": "Squadron"
    }

    serialized_data = json_serializer(data, SerializationContext("key", MessageField.VALUE))
    print(serialized_data)

    producer.produce(
        topic=config["TOPIC"],
        value=serialized_data,
        on_delivery=delivery_report
    )

producer.flush()