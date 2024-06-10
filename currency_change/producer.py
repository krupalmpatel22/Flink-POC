from confluent_kafka import Producer
import json
import random
from faker import Faker

# Initialize Faker for generating random data
faker = Faker()

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Create a Kafka producer
producer = Producer(kafka_config)

# Function to send a message to a Kafka topic
def send_message(producer, topic, message):
    producer.produce(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print(f"Message sent: {message}")

# Function to generate random transaction data
def generate_transaction():
    currencies = ["INR", "USD", "CAD", "EUR", "AUD"]
    transaction = {
        "transaction_id": faker.uuid4(),
        "input_amount": random.randint(1, 1000),
        "from_currency": random.choice(currencies),
        "to_currency": random.choice(currencies),
        "timestamp": faker.date_time().isoformat()
    }
    return transaction

if __name__ == '__main__':
    # Kafka topic name
    topic_name = 'transactions'

    # Generate and send 100 random transactions
    for _ in range(100):
        transaction = generate_transaction()
        send_message(producer, topic_name, transaction)

    # Close the producer
    producer.flush()

