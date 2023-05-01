# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
# Read the .testlog file
data = pd.read_csv("raw_data\cooja.testlog", delimiter="\t", header=None)

# Save the data as a CSV file
data.to_csv("raw_data\sample.csv", index=False)

# Read CSV file
data = pd.read_csv("raw_data\sample.csv")

# Create three CSV file objects and add headers
sendData = open("raw_data\SendData_raw.csv", "w")
sendData.write("send_time,node,packet\n")
RecvData = open("raw_data\RecvData_raw.csv", "w")
RecvData.write("send_time,node,packet\n")
OtherData = open("raw_data\OtherData_raw.csv", "w")
OtherData.write("send_time,node,packet\n")

# Iterate over each row
for index, row in data.iterrows():
    # Get the third element
    third_element = row[2]
    
    # Write lines into different CSV files based on the beginning of the third element
    if third_element.startswith("SendData"):
        sendData.write("{},{},{}\n".format(row[0], row[1], row[2]))
    elif third_element.startswith("RecvData"):
        RecvData.write("{},{},{}\n".format(row[0], row[1], row[2]))
    else:
        OtherData.write("{},{},{}\n".format(row[0], row[1], row[2]))
    
# Close the CSV file
sendData.close()
RecvData.close()
OtherData.close()

data = pd.read_csv("raw_data\SendData_raw.csv")

# Data column splitting and conversion

data['packet_type'] = data['packet'].str.split(' ').str[0]
data['packet_number'] = data['packet'].str.split(' ').str[1]

data['node'] = data['node'].str.split(':').str[1]

# Delete unnecessary data columns

del data['packet']
del data['packet_type']

# Data replacement
#data['packet_type'].replace("SendData", False, inplace=True)

# Data type conversion (preparing for mongodb)
#data['packet_type'] = data['packet_type'].astype('bool')
data['send_time'] = data['send_time'].astype('int')
data['node'] = data['node'].astype('int64')

# Increase the label to indicate whether the data is processed

data['label'] = False
data.to_csv('clean_data\SendData.csv', index=False)

raw = pd.read_csv("clean_data\SendData.csv")

raw.info()

data = pd.read_csv("raw_data\RecvData_raw.csv")

# Data column splitting and conversion

data['packet_type'] = data['packet'].str.split(' ').str[0]
data['packet_number'] = data['packet'].str.split(' ').str[1]

data['node'] = data['node'].str.split(':').str[1]

# Delete unnecessary data columns

del data['packet']
del data['packet_type']

# Data replacement
#data['packet_type'].replace("RecvData", True, inplace=True)

# Data type conversion (preparing for mongodb)
#data['packet_type'] = data['packet_type'].astype('bool')
data['node'] = data['node'].astype('int64')
data['send_time'] = data['send_time'].astype('int')
# Increase the label to indicate whether the data is processed

data['label'] = False
data.to_csv('clean_data\RecvData.csv', index=False)

raw = pd.read_csv("clean_data\RecvData.csv")

raw.info()

# Define a function to extract the value of the dutycycle attribute from a string
def extract_dutycycle(data):
    match = re.search(r"dutycycle:\s*(\d+(?:\s+\d+)*)", data)
    if match:
        dutycycle_list = match.group(1).split()
        return int(dutycycle_list[-1])
    return None

# Define a function to extract the value of the parentChange property from a string
def extract_parent_change(data):
    match = re.search(r"parentChange\s*(\d+)", data)
    if match:
        return int(match.group(1))
    return None

# Define a function to extract the value of the drops attribute from a string
def extract_drops(data):
    match = re.search(r"drops\s*(\d+)", data)
    if match:
        return int(match.group(1))
    return None

# Read OtherData CSV file
data = pd.read_csv("raw_data\OtherData_raw.csv")

# Create a dictionary to store row data with the same id
node_dict = {}

# Iterate through each row and extract the attributes from the "packet" column
for index, row in data.iterrows():
    # Extract node
    node_str = row["node"]
    match = re.search(r"ID:(\d+)", node_str)
    if match:
        node = int(match.group(1))
        
        # Extract the values of dutycycle, parentChange, energyConsumption attributes
        dutycycle = extract_dutycycle(row["packet"])
        parent_change = extract_parent_change(row["packet"])
        drops = extract_drops(row["packet"])
        
        # Add the attribute value to the list corresponding to the id in the dictionary
        if node not in node_dict:
            node_dict[node] = {"drops": None, "drop_time": None, "parentChange": None, 
                               "parentChange_time": None, "dutycycle": None, "dutycycle_time": None}
        if dutycycle is not None:
            node_dict[node]["energyConsumption"] = dutycycle
            node_dict[node]["dutycycle_time"] = row["send_time"]
        if parent_change is not None:
            node_dict[node]["parentChange"] = parent_change
            node_dict[node]["parentChange_time"] = row["send_time"]
        if drops is not None:
            node_dict[node]["drops"] = drops
            node_dict[node]["drop_time"] = row["send_time"]
        
                
# Convert dictionary to DataFrame object for each csv file
result_drops = pd.DataFrame.from_dict(node_dict, orient="index", columns=["drops", "drop_time"])
result_drops = result_drops.reset_index().rename(columns={"index": "node"})
result_drops.fillna(value=-1, inplace=True)
result_drops['drop_time'] = result_drops['drop_time'].astype('int')
result_drops.to_csv("clean_data\drops.csv", index=False)

result_parentChange = pd.DataFrame.from_dict(node_dict, orient="index", columns=["parentChange", "parentChange_time"])
result_parentChange.rename_axis("node", inplace=True)
result_parentChange.fillna(value=-1, inplace=True)
result_parentChange['parentChange'] = result_parentChange['parentChange'].astype('int64')
result_parentChange['parentChange_time'] = result_parentChange['parentChange_time'].astype('int')
result_parentChange.to_csv("clean_data\parentChange.csv", index=True)

result_energyConsumption = pd.DataFrame.from_dict(node_dict, orient="index", columns=["energyConsumption", "dutycycle_time"])
result_energyConsumption.rename_axis("node", inplace=True)
result_energyConsumption.fillna(value=-1, inplace=True)
result_energyConsumption['dutycycle_time'] = result_energyConsumption['dutycycle_time'].astype('int')
result_energyConsumption.to_csv("clean_data\energyConsumption.csv", index=True)