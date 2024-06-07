import json
import uuid
from datetime import datetime
import string
import random
import os
import time

class DataGenerator:
    def __init__(self):
        pass

    def generate_random_string(self,length):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for _ in range(length))

    def generate_data(self,num_entries):
        data = []
        for _ in range(num_entries):
            entry = {
                "uuid": str(uuid.uuid4()),
                "balance_value": random.randint(1, 100000),
                "timestamp": datetime.now().isoformat()
            }
            data.append(entry)
        return data

    def json_create(self, num_entries, directory, file_index):
        os.makedirs(directory, exist_ok=True)

        # Generate a timestamp for the file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"file{file_index}_{timestamp}.json"
        file_path = os.path.join(directory, file_name)
        data = self.generate_data(num_entries)
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

        print(f'Data saved to {file_path}')


obj = DataGenerator()
destination_directory = r'C:\D-Drive\PythonProject\New_Shrutil\Data Storage'
for i in range(1, 6):
    obj.json_create(num_entries=10, directory=destination_directory, file_index=i)
    time.sleep(60)
