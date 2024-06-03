from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='kafka:9092')

for i in range(10):
    message = f"Test message {i}"
    producer.send('number_topic', message.encode('utf-8'))
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
producer.close()
