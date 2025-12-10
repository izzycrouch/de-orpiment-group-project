from src.utils.storage_data import read_data,storage_data
import json
from datetime import datetime

def covert_json(data):
    file_content = data.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    for key, value in json_content.items():
        try:
            json_content[key] = datetime.fromisoformat(value)
        except Exception as e:
            raise e
    return json_content

def convert_dict(data):
    for key,value in data.items():
        data[key] = value.isoformat()
    json_content = json.dumps(data)
    encoded_json = json_content.encode('utf-8')
    return encoded_json


def get_latest(bucket_name,file_name = 'latest.json'):
    try:
        data = read_data(bucket_name,file_name)
    except:
        return False
    json_dict = covert_json(data)
    return json_dict

def save_latest(data,bucket_name,file_name = 'latest.json'):
    data = convert_dict(data)
    storage_data(data, bucket_name, file_name)
