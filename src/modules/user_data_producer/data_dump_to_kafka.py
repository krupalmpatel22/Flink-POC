from confluent_kafka import Producer
import csv
import json

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def read_csv_and_produce(csv_file, topic, bootstrap_servers):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    with open(csv_file, newline='', encoding='utf-8') as csvfile:  # Specify encoding='utf-8'
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Serialize the dictionary to JSON and encode it to bytes
            record_value = json.dumps(row).encode('utf-8')
            producer.produce(topic, value=record_value, callback=delivery_report)
            producer.poll(0.5)  # Poll to handle delivery reports

    producer.flush()

if __name__ == "__main__":
    csv_file = r"D:\DemoData\sample_data_main.csv"  # Path to your CSV file
    kafka_topic = 'test3'  # Kafka topic to produce messages to
    bootstrap_servers = 'localhost:9092'  # Kafka broker address

    read_csv_and_produce(csv_file, kafka_topic, bootstrap_servers)
