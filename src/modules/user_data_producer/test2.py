import csv
import json

def csv_to_json(csv_file_path):
    # List to store dictionaries representing each row of the CSV data
    data = []

    # Read the CSV file and convert each row to a dictionary
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            print(row)
            # Append the row dictionary to the data list
            data.append(row)


    # Serialize the list of dictionaries to JSON format
    json_data = json.dumps(data, indent=4)
    return json_data

# Example usage:
csv_file_path = r"D:\DemoData\sample_data_main.csv"  # Replace with the path to your CSV file
json_data = csv_to_json(csv_file_path)
print(json_data)
