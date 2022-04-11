"""
PARSE PANDAS DF TO JSON. EITHER ASSIGN TO A VARIABLE OR WRITE TO DISK.
Make sure Pycharm working directory is set to src folder
(e.g. /home/mete/PycharmProjects/datapreprocessing-examples-with-python/datapreprocessing-examples-with-python/src).
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
df.to_json("../data/output/json_parsing/json_parsed.json", indent=4, force_ascii=False)

# LOADS A JSON STRING AS A PYTHON DICTIONARY
json_dict = json.loads(json_str)