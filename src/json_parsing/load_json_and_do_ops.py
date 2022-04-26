"""
Load JSON.
Either load JSON string or a JSON file.
Once loaded do some operations such as converting back to string or writing to disk.
Make sure Pycharm working directory is set to src folder
(e.g. /home/mete/PycharmProjects/datapreprocessing-examples-with-python/datapreprocessing-examples-with-python/src).
"""

import json

json_file_path = "../data/input/input_file.json"
input_json_str = """
{
        "Name":{
            "0":"John",
            "1":"Jane",
            "2":"This name has türkish special characters 'Özgür Ğ. r' with single quote"
        },
        "Age":{
            "0":29,
            "1":23,
            "2":31
        }
}
"""

# loads a JSON string as a python dictionary
json_dict = json.loads(input_json_str)
print(f"Loaded json dict: {json_dict}")

# loads a file (containing json) as a Dictionary
with open(json_file_path, 'r', encoding='utf-8') as file_reader:
    json_dict_from_file = json.load(file_reader)
    print(f"Loaded json dict from file: {json_dict_from_file}")

# Creates a String in json format from a python dictionary
json_str_after_dumps = json.dumps(json_dict)
print(f"JSON string created from a python dictionary: {json_str_after_dumps}")

# Writes a python dictionary into a file
with open("../data/output/json_parsing/using_dump_written.json", "w",
          encoding="utf-8") as file2:
    json.dump(json_dict, file2, indent=4, ensure_ascii=False)
