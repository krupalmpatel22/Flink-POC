from confluent_kafka import Producer
from avro import schema, io
import json
from io import BytesIO  # Import BytesIO from the io module
import uuid

conf = {
    'bootstrap.servers': "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': "YLYZDXDPFW723WDI",
    'sasl.password': "xikZoi5RXb+UEj96+/lA3vHgBvPjLrIecpkccLAM+ISZDA6ars9nKyF5dHc/OBpx"
}

avro_schema = """
{
  "type": "record",
  "name": "sampleRecord",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "marks", "type": "double"},
    {"name": "name", "type": "string"}
  ]
}
"""

schema = schema.Parse(avro_schema)

producer = Producer(conf)

topic = 'topic_0'

for i in range(1):
    try:
        data = {"id": 0, "marks": 100.0, "name": "John Doe"}
        print(data)
        datum_writer = io.DatumWriter(schema)
        bytes_writer = BytesIO()  # Use BytesIO from the io module
        encoder = io.BinaryEncoder(bytes_writer)
        datum_writer.write(data, encoder)
        raw_bytes = bytes_writer.getvalue()
        print(raw_bytes)
        producer.produce(topic=topic, value=raw_bytes, key=uuid.uuid4())
    except Exception as e:
        print('Message serialization failed: {}'.format(e))
        break
    else:
        print('Message sent: {}'.format(i))

producer.flush()
