./bin/flink run --python scripts/test.py
./bin/flink run --python scripts/currency_change.py
./bin/flink run --python scripts/test.py
./bin/flink run --python scripts/test2.py

pip install apache-flink==1.18
pip install ruamel.yaml CurrencyConverter apache-flink
pip install CurrencyConverter
pip install
pip install


bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic number_topic --from-beginning
