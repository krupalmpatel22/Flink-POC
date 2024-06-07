from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    # write all the data to one file
    env.set_parallelism(1)



    # define the source
    # data_stream = env.from_collection(
    #     [
    #         {"uuid": "0dc38a63-796b-4687-b70e-31c39e03b304"},
    #         {"uuid": "0dc38a63-796b-4687-b70e-31c39e03b304"}
    #     ],
    #     type_info=Types.MAP(Types.STRING(), Types.STRING())
    # )
    json_type_info = Types.ROW(
        [Types.STRING(),  # uuid
         Types.INT(),  # balance_value (assuming it's an int)
         Types.STRING()  # timestamp
         ]
    )

    data_stream = env.from_collection(
        [
            {
                "uuid": "e53d163e-b258-4377-a49b-2bc27d96d8e1",
                "balance_value": 95617,
                "timestamp": "2024-06-07T17:08:26.763037"
            }
        ],
        type_info=Types.MAP(Types.STRING(), Types.STRING())
    )


    # json_type_info = Types.OBJECT_ARRAY[
    #     Types.MAP(Types.STRING(), Types.STRING()),
    #     Types.MAP(Types.STRING(), Types.STRING())
    # ]
    #
    #
    # data_stream = env.from_collection(
    #     [
    #         {"uuid": "0dc38a63-796b-4687-b70e-31c39e03b304"},
    #         {"uuid": "0dc38a63-796b-4687-b70e-31c39e03b304"}
    #     ],
    #     type_info=Types.MAP(Types.STRING(), json_type_info)
    # )

    # Print the data stream to verify
    print(data_stream.print())

    env.execute("Dictionary Data Stream Example")