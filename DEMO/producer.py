import json
import random
import logging

from kafka import KafkaProducer
from Config import BOOTSTRAP_SERVER, INPUT_TOPIC

"""
Log levels:
    DEBUG: Detailed information, typically of interest only when diagnosing problems.
    INFO: Confirmation that things are working as expected.
    WARNING: An indication that something unexpected happened, or indicative of some problem in the near future
    ERROR: Due to a more serious problem, the software has not been able to perform some function.
    CRITICAL: A serious error, indicating that the program itself may be unable to continue running.
Log format:
    %(asctime)s: Time when the log message was created
    %(levelname)s: Level of the log message
    %(message)s: The log message
Log file:
    logs\producer.log
Log Configuration:
    Log file is in append mode
    Log level is set to INFO
    Log format is set as per the above format
    Log messages are written to the log file
    Log messages are also printed to the console
"""
logging.basicConfig(
    filename='logs\producer.log',
    filemode='a',  # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class Producer:
    def __init__(self):
        """
        Initializes Kafka Producer with the given bootstrap server and input topic
        """
        self.producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic_name = INPUT_TOPIC
        logging.info("Kafka Producer initialized with bootstrap server %s and topic %s", BOOTSTRAP_SERVER, INPUT_TOPIC)

    def get_message(self):
        """
        Generates a random message with input_amount, from_currency, to_currency
        """
        foo = ["INR", "USD", "CAD", "EUR", "AUD"]
        message = {
            "input_amount": random.randint(1, 100),
            "from_currency": random.choice(foo),
            "to_currency": random.choice(foo)
        }
        logging.info("Generated message: %s", message)
        return message

    def send_message(self, message):
        """
        Sends the message to the input topic
            param message: The message to be sent
        """
        self.producer.send(self.topic_name, message)
        self.producer.flush()
        logging.info("Message sent: %s", message)
        logging.info("Topic: %s, Partitions: %s", self.topic_name, self.producer.partitions_for(self.topic_name))

    def close(self):
        """
        Closes the Kafka Producer
        """
        self.producer.close()
        logging.info("Kafka Producer closed", )

if __name__ == "__main__":
    producer = Producer()
    for i in range(100):
        producer.send_message(producer.get_message())
    producer.close()
