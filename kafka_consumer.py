import json
from kafka import KafkaConsumer

class KafkaDataConsumer:
    def __init__(self, topic, bootstrap_servers):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers
        )
        self.data = []

    def consume_data(self):
        for message in self.consumer:
            self.data.append(message.value)
            if len(self.data) > 1000:  # Keep only the latest 1000 messages
                self.data.pop(0)
    
    def get_data(self):
        return self.data


if __name__ == '__main__':
    c = KafkaDataConsumer('output_topic', ['localhost:9092'])
    c.consume_data()
    print(c.get_data())