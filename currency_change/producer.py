from kafka import KafkaProducer
import json
from faker import Faker
import random
faker = Faker()

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the Kafka topic
topic_name = 'test'

# Function to send messages to Kafka
def send_message(producer, topic, message):
    producer.send(topic, message)
    producer.flush()
    print(f"Message sent: {message}")

foo = ["INR", "USD", "CAD", "EUR", "AUD"]
# Example messages to send
for i in range(100):
    message = {
        "input_amount": random.randint(1, 100),
        "from_currency": random.choice(foo),
        "to_currency": random.choice(foo)
        }

    send_message(producer, topic_name, message)
# Send messages to Kafka
# for message in messages:
#     send_message(producer, topic_name, message)

# Close the producer
producer.close()
