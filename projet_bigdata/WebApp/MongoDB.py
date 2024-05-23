import time
from datetime import datetime

from pymongo import MongoClient
from pymongo import errors
import pandas as pd

def connect_to_mongodb(database_url, database_name, collection_name):
    try:
        client = MongoClient(database_url)
        db = client[database_name]
        collection = db[collection_name]
        return collection
    except errors.ConnectionFailure as e:
        print("Failed to connect to MongoDB:", e)
        return None



#test_sentiment_page
def retrieve_data(collection):
    try:
        cursor = collection.find()
        df = pd.DataFrame(list(cursor))
        return df
    except Exception as e:
        print("Failed to retrieve data:", e)
        return None

#visualisation_page
def get_realtime_data(collection):
    try:
        cursor = collection.find().sort([("_id", -1)])
        time.sleep(3)
        df = pd.DataFrame(list(cursor))
        return df
    except Exception as e:
        print("Failed to retrieve real-time data:", e)
        return None
