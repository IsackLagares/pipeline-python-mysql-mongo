import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!\n\n")
    except Exception as e:
        print(e)

    return client

def create_db(client,db_name):
    db = client[db_name]

    return db

def create_collection(db,collection_name):
    collection = db[collection_name]

    return collection

def extract_api(url):
    return requests.get(url).json()

def insert_data(collection,data):
    docs = collection.insert_many(data)
    return docs


if __name__ == '__main__':
    # Load File .env
    load_dotenv('../.env')

    uri = os.getenv("MONGODB_URI")

    client = connect_mongo(uri)
    database = create_db(client,'db_desafio')
    collection = create_collection(database,'col_desafio')

    data = extract_api('https://labdados.com/produtos')
    print(f'Total de Dados Extraídos: {len(data)}')

    docs = insert_data(collection,data)
    print(f'Foram incluídos {len(docs.inserted_ids)} dados')

    client.close()


