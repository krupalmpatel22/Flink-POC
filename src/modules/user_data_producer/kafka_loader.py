import schedule # type: ignore
import time 
from data_generator import DataGenerator
from kafka import KafkaProducer

kafka_nodes = ""
my_topic = ""

def gen_data():
    prod = KafkaProducer(bootstrap_servers=kafka_nodes)
    mydata = DataGenerator().generate()
    prod.send(topic=my_topic, value=mydata)
    prod.flush()

if __name__ == '__main__':
    gen_data()
    schedule.every(10).seconds.do(gen_data)

    while True:
        schedule.run_pending()
        time.sleep(1)