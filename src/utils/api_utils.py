import json

def get_api_key(path, key_name):
    # Open and read the JSON file
    with open(path, 'r') as file:
        data = json.load(file)

    # Extract the "key" value
    return data[key_name]