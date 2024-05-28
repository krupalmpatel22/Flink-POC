from kafka import KafkaProducer
import json

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the Kafka topic
topic_name = 'user_behavior'

# Function to send messages to Kafka
def send_message(producer, topic, message):
    producer.send(topic, message)
    producer.flush()
    print(f"Message sent: {message}")

# Example messages to send
message = {
    "input_amount": 90,
    "from_currency": "INR",
    "to_currency": "CAD"
    }

send_message(producer, topic_name, message)
# Send messages to Kafka
# for message in messages:
#     send_message(producer, topic_name, message)

# Close the producer
producer.close()
