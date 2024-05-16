from confluent_kafka import Consumer
from Config.api_config import config
from confluent_kafka import KafkaError

conf = {'bootstrap.servers': config.get('BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': config.get('API_KEY'),
        'sasl.password': config.get('API_SECRET'),
        'group.id': 'default',
        'auto.offset.reset': 'latest'
        }

consumer = Consumer(conf)

consumer.subscribe([config['OUTPUT_TOPIC']])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        print('Waiting for message or error in poll()...')
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print('Received message: {}'.format(msg.value().decode('utf-8')))

consumer.close()
