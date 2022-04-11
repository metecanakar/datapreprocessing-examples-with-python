#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 17:30:41 2022

@author: mete
"""
import json
import pandas as pd


# create a pandas df with special characters
d = {"Name": ["John", "Jane", "This name has türkish special characters 'Özgür Ğ. r' with single quote"],
     "Age": [29, 23, 31]}
df = pd.DataFrame(data=d)


# READ PD DF AND RETURN AS A STRING
# if you have special characters make sure force_ascii to be False
json_str = df.to_json(indent=4, force_ascii=False)
print(json_str)

# READ PD DF AND WRITE TO A FILE
json_str = df.to_json("../data/output/json_parsing", indent=4, force_ascii=False)

# LOADS A JSON STRING AS A PYTHON DICTIONARY
json_dict = json.loads(json_str)

# loads a file (containing json) as a Dictionary
#json_file = open('filepath', 'r', encoding='utf-8')
#json.load(file_reader)
#json_file.close()


# Creates a String in json format from a python dictionary
json_str_after_dumps = json.dumps(json_dict)

# Writes a python dictionary into a file
with open("/home/mete/SpyderProjects/pandas-df-to-json/using_dump_written.json", "w",
             encoding="utf-8") as file2:
    json.dump(json_dict, file2, indent=4, ensure_ascii=False)

