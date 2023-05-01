import csv
import json
import pymongo
from pymongo import MongoClient
from bson import json_util
import numpy


# 5 Minutes auto check for new data
#import schedule
import time

# Performance optimization
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent import futures

# Connect to MongoDB
client = MongoClient("mongodb+srv://jili8919:123jiashu@cluster0.3zvhedb.mongodb.net/test")
db = client["RawData"]


# # V2 by Jiashu: execution time is too long #
# # 循环读取 CSV 文件中的每一行数据，并将其插入到 MongoDB 中
# with open('SendData_raw.csv', 'r') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         collection1.insert_one(row)

# with open('RecvData_raw.csv', 'r') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         collection2.insert_one(row)

# with open('OtherData_raw.csv', 'r') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         collection3.insert_one(row)

    
        
# # V3 Use insert_many to
# collection = db["SendData"]
# collection.insert_many(data1)
# collection = db["RecvData"]
# collection.insert_many(data2)
# collection = db["OtherData"]
# collection.insert_many(data3)

#Insert data into MongoDB collection
# collection = db["SendData"]
# collection.insert_many(data1)
# collection = db["RecvData"]
# collection.insert_many(data2)
# collection = db["OtherData"]
# collection.insert_many(data3)

# V4 Batch insert data to improve efficiency

# Defines the schema for the SendData collection and create index
# Defines the schema for the SendData, RecvData, and OtherData collections and create index
SendData_schema = {
    "send_time": int,
    "node": int,
    "packet_number": object,
    "label": bool
}
RecvData_schema = {
    "send_time": int,
    "node": int,
    "packet_number": object,
    "label": bool
}
drops_schema = {
    "drop_time": int,
    "node": int,
    "drops": float
}
energyConsumption_schema ={
    "dutycycle_time": int,
    "node": int,
    "energyConsumption": float
}
parentChange_schema ={
    "parentChange_time": int,
    "node": int,
    "parentChange": int
}
SendData_collection = db["SendData"]
SendData_collection.create_index([("send_time", pymongo.ASCENDING)])
SendData_collection.create_index([("node", pymongo.ASCENDING)])
SendData_collection.create_index([("packet_number", pymongo.ASCENDING)])
SendData_collection.create_index([("label", pymongo.ASCENDING)])
#SendData_collection.create_index([("send_time", pymongo.ASCENDING), ("node", pymongo.ASCENDING)], unique=True)
RecvData_collection = db["RecvData"]
RecvData_collection.create_index([("send_time", pymongo.ASCENDING)])
RecvData_collection.create_index([("node", pymongo.ASCENDING)])
RecvData_collection.create_index([("packet_number", pymongo.ASCENDING)])
RecvData_collection.create_index([("label", pymongo.ASCENDING)])
#RecvData_collection.create_index([("send_time", pymongo.ASCENDING), ("node", pymongo.ASCENDING)], unique=True)
Drops_collection = db["Drops"]
Drops_collection.create_index([("node", pymongo.ASCENDING)])
Drops_collection.create_index([("drops", pymongo.ASCENDING)])
Drops_collection.create_index([("drop_time", pymongo.ASCENDING)])
#Drops_collection.create_index([("node", pymongo.ASCENDING), ("drops", pymongo.ASCENDING), ("drop_time", pymongo.ASCENDING)])
energyConsumption_collection = db["energyConsumption"]
energyConsumption_collection.create_index([("node", pymongo.ASCENDING)])
energyConsumption_collection.create_index([("energyConsumption", pymongo.ASCENDING)])
energyConsumption_collection.create_index([("dutycycle_time", pymongo.ASCENDING)])
#energyConsumption_collection.create_index([("node", pymongo.ASCENDING), ("energyConsumption", pymongo.ASCENDING), ("dutycycle_time", pymongo.ASCENDING)])
ParentChange_collection = db["parentChange"]
ParentChange_collection.create_index([("node", pymongo.ASCENDING)])
ParentChange_collection.create_index([("parentChange", pymongo.ASCENDING)])
ParentChange_collection.create_index([("parentChange_time", pymongo.ASCENDING)])
#ParentChange_collection.create_index([("node", pymongo.ASCENDING), ("parentChange", pymongo.ASCENDING), ("parentChange_time", pymongo.ASCENDING)])
# Define the insert data function
def insert_data(collection, rows):
    for row in rows:
        row["send_time"] = int(row["send_time"])
        row["node"] = int(row["node"])
        row["packet_number"] = str(row["packet_number"])
        row["label"] = bool(row["label"])
    json_rows = [json.loads(json_util.dumps(row)) for row in rows]
    collection.insert_many(json_rows)
def insert_drops(collection, rows):
    for row in rows:
        row["node"] = int(row["node"])
        row["drops"] = float(row["drops"])
        row["drop_time"] = int(row["drop_time"])
    json_rows = [json.loads(json_util.dumps(row)) for row in rows]
    collection.insert_many(json_rows)
def insert_parentChange(collection, rows):
    for row in rows:
        row["node"] = int(row["node"])
        row["parentChange"] = int(row["parentChange"])
        row["parentChange_time"] = int(row["parentChange_time"])
    json_rows = [json.loads(json_util.dumps(row)) for row in rows]
    collection.insert_many(json_rows)
def insert_energyConsumption(collection, rows):
    for row in rows:
        row["node"] = int(row["node"])
        row["energyConsumption"] = float(row["energyConsumption"])
        row["dutycycle_time"] = int(row["dutycycle_time"])
    json_rows = [json.loads(json_util.dumps(row)) for row in rows]
    collection.insert_many(json_rows)
# Insert data from CSV file into MongoDB concurrently using multiple threads
with ThreadPoolExecutor() as executor:
    futures = []
    with open('clean_data/SendData.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        data =  [row for row in reader]
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            rows = data[i:i+batch_size]
            future = executor.submit(insert_data, SendData_collection, rows)
        for future in as_completed([future]):
            pass
        futures = []

    with open('clean_data/RecvData.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        data =  [row for row in reader]
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            rows = data[i:i+batch_size]
            future = executor.submit(insert_data, RecvData_collection, rows)
        for future in as_completed([future]):
            pass
        futures = []

    with open('clean_data/drops.csv','r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]
        batch_size = 10
        for i in range(0, len(data), batch_size):
            rows = data[i:i+batch_size]
            future = executor.submit(insert_drops, Drops_collection, rows)
        for future in as_completed([future]):
            pass

    with open('clean_data/energyConsumption.csv','r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]
        batch_size = 10
        for i in range(0, len(data), batch_size):
            rows = data[i:i+batch_size]
            future = executor.submit(insert_energyConsumption, energyConsumption_collection, rows)
        for future in as_completed([future]):
            pass     
    
    with open('clean_data/parentChange.csv','r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]
        batch_size = 10
        for i in range(0, len(data), batch_size):
            rows = data[i:i+batch_size]
            future = executor.submit(insert_parentChange, ParentChange_collection, rows)
        for future in as_completed([future]):
            pass   

    #with open('clean_data/OtherData.csv', 'r') as csvfile:
     #   reader = csv.DictReader(csvfile)
      #  data = [row for row in reader]
       # batch_size = 1000
        #for i in range(0, len(data), batch_size):
         #   rows = data[i:i+batch_size]
          #  future = executor.submit(insert_data, db["OtherData"], rows)
        #for future in as_completed([future]):
         #   pass


print("Successful import data")
# Close the MongoDB connection
client.close()