from pymongo import MongoClient
import os


class MongoUtils:
    def __init__(self):
        pass

    def mongo_conection(self, db_name, collection_name):
        try:
            client = MongoClient(os.getenv('MONGO_URL'))
            db = client[db_name]  # Replace 'your_database' with your database name
            collection = db[collection_name]  # Replace 'your_collection' with your collection name
            return collection
        except Exception as e:
            print('Mongo Connection Error : ',e)

    def insert_data(self, collection, records):
        try :
            collection.insert_one(records)
        except Exception as e:
            print('Inser Data Error : ',e)
