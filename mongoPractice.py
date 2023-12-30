from pymongo.mongo_client import MongoClient
import pandas as pd
import json

uri = "mongodb+srv://riddu:RCeka2SEB0dfMeuC@soumalla.028f1.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri)

# print("success")

database=client.mongoForPackage

collection=database["session"]

data={

    "projectname":"Mongo_Python-Package",
    "projectdate":"30-12-2023",
    "projecttime":"2.55 A.M"
}

#print(data)

database.collection.insert_one(data)

all_record=collection.find()

for record in all_record:
    print(record)


collection.insert_many(
    [
        {

            "projectname":"Mongo_Python-Package",
            "projectdate":"30-12-2023",
            "projecttime":"2.55 A.M"
        },

        {

            "projectname":"Project-With-Complete-MLOps",
            "projectdate":"39-12-2023",
            "projecttime":"12.55 A.M"
        }
        
    ]
)


class mongodb_operation:

    def __init__(self,client_url: str, database_name: str, collection_name: str=None):
        self.client_url=client_url
        self.database_name=database_name
        self.collection_name=collection_name

    def create_client(self):
        client=MongoClient(self.client_url)
        return client


    def create_database(self):
        client=self.create_client()
        database=client[self.database_name]
        return database
        
    def create_collection(self,collection=None):
        database=self.create_database()
        collection=database[collection]
        return collection

    def insert_record(self,record:dict,collection_name:str):
        if type(record)==list:
            for data in record:
                if type(data)!= dict:
                    raise TypeError("record must be in the dict")
            collection=self.create_collection(collection_name)
            collection.insert_many(record)
        elif type(record)==dict:
            collection=self.create_collection(collection_name)
            collection.insert_one(record)

    def bulk_insert(self,datafile:str,collection_name:str=None):
        self.path=datafile

        if self.path.endswith('.csv'):
            data=pd.read_csv(self.path,encoding='utf-8')

        elif self.path.endswith('.xlsx'):
            data=pd.read_excel(self.path,encoding='utf-8')
            
        datajson=json.loads(data.to_json(orient='record'))
        collection=self.create_collection()
        collection.insert_many(datajson)




#call the mongodb connection 
client_url =  "mongodb+srv://riddu:RCeka2SEB0dfMeuC@soumalla.028f1.mongodb.net/?retryWrites=true&w=majority"
database = "mynewdatabase"
collection_name = "myoldcolumn"


mongo=mongodb_operation(client_url,database,collection_name)

mongo.insert_record({

            "projectname":"Mongo_Python-Package",
            "projectdate":"30-12-2023",
            "projecttime":"2.55 A.M"
        },collection_name)

mongo.insert_record([
        {

            "projectname":"Mongo_Python-Package",
            "projectdate":"30-12-2023",
            "projecttime":"2.55 A.M"
        },

        {

            "projectname":"Project-With-Complete-MLOps",
            "projectdate":"39-12-2023",
            "projecttime":"12.55 A.M"
        }
        
    ],collection_name)