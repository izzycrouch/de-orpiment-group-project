from src.utils.storage_data import read_data,storage_data
import json
from datetime import datetime

# json_str format: {'table_name': 'timestamp_of_extraction'}
def get_datetime_dict(input_json_str: str):
    
    output_dict = json.loads(input_json_str)
    
    for key, value in output_dict.items():
        try:
            output_dict[key] = datetime.fromisoformat(value)
        except:
            raise ValueError('Value cannot be converted to datetime datatype.')
    
    return output_dict

# input_dict format: {'table_name': datetime}
def convert_to_enconded_json(input_dict: dict):
    for key,value in input_dict.items():
        try:
            input_dict[key] = value.isoformat()
        except:
            raise TypeError('Value is not a datetime datatype.')
    
    bytes_output = json.dumps(input_dict).encode('utf-8')
    
    return bytes_output


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
