"""
Parse pandas df to JSON. Either assign the parsed JSON to a variable or write to disk.
Make sure Pycharm working directory is set to src folder
(e.g. /home/mete/PycharmProjects/datapreprocessing-examples-with-python/datapreprocessing-examples-with-python/src).
"""

import pandas as pd

# create a pandas df with special characters
d = {"Name": ["John", "Jane", "This name has türkish special characters 'Özgür Ğ. r' with single quote"],
     "Age": [29, 23, 31]}
df = pd.DataFrame(data=d)

# read pd df and return as a string
# if you have special characters make sure force_ascii to be False
json_str = df.to_json(indent=4, force_ascii=False)
print(json_str)

# read pd df and write to a file
df.to_json("../data/output/json_parsing/json_parsed.json", indent=4, force_ascii=False)
