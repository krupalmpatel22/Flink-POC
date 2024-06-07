import sys

import argparse
from typing import Iterable

from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema

from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
import json

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Define the file path from which to read the JSON data
    json_file_path = '/opt/flink/scripts/file1_20240607_170826.json'

    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Print the data to verify

    data_stream = env.from_collection(data,
                                      type_info=Types.MAP(Types.STRING(), Types.STRING()))

    data_stream.print()


    # Execute the Flink job
    try:
        env.execute("Read JSON File Data Stream Example")
    except Exception as e:
        print(f"Job execution failed: {e}")
