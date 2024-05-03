from confluent_kafka import Producer
import csv

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def read_csv_and_produce(csv_file, topic, bootstrap_servers):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Assuming the CSV file has headers and each row is a dictionary
            # Modify the following line according to your CSV structure
            record_value = ','.join([f'{key}:{value}' for key, value in row.items()])
            producer.produce(topic, value=record_value.encode('utf-8'), callback=delivery_report)
            producer.poll(0.5)  # Poll to handle delivery reports

    producer.flush()

if __name__ == "__main__":
    csv_file = r"D:\Flink POC\D\fda_approved_food_items_w_nutrient_info.csv\sample_data_main_subpart_1.csv"  # Path to your CSV file
    kafka_topic = 'test'  # Kafka topic to produce messages to
    bootstrap_servers = 'localhost:9092'  # Kafka broker address

    read_csv_and_produce(csv_file, kafka_topic, bootstrap_servers)
