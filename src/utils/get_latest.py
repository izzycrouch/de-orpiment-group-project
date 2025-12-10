from src.utils.storage_data import read_data,storage_data
import json
from datetime import datetime

def get_datetime_dict(input_json_str: str):
    
    output_dict = json.loads(input_json_str)
    
    for key, value in output_dict.items():
        try:
            output_dict[key] = datetime.fromisoformat(value)
        except Exception as e:
            raise e('Value is not a valid datetime.')
    
    return output_dict


def convert_to_enconded_json(input_dict: dict):
    for key,value in input_dict.items():
        input_dict[key] = value.isoformat()
    
    json_content = json.dumps(input_dict)
    encoded_json = json_content.encode('utf-8')
    
    return encoded_json


def get_latest(bucket_name, file_name = 'latest.json'):
    try:
        data = read_data(bucket_name,file_name)
        file_content = data.get()['Body'].read().decode('utf-8')
    except:
        return False
    json_dict = get_datetime_dict(file_content)
    return json_dict


def save_latest(data,bucket_name,file_name = 'latest.json'):
    data = convert_to_enconded_json(data)
    storage_data(data, bucket_name, file_name)
