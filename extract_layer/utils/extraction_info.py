from extract_layer.utils.save_data import save_data, read_data
import json
from datetime import datetime

# json_str format: {'table_name': 'timestamp_of_extraction'}
def convert_extraction_info_to_dict(input_json_str: str):
    
    output_dict = json.loads(input_json_str)
    
    for key, value in output_dict.items():
        try:
            output_dict[key] = datetime.fromisoformat(value)
        except:
            raise ValueError('Value cannot be converted to datetime datatype.')
    
    return output_dict

# input_dict format: {'table_name': datetime}
def convert_dict_to_bytes(input_dict: dict):
    
    for key,value in input_dict.items():
        try:
            input_dict[key] = value.isoformat()
        except:
            raise TypeError('Value is not a datetime datatype.')
    
    bytes_output = json.dumps(input_dict).encode('utf-8')
    
    return bytes_output


def get_latest_extraction_info(bucket_name: str, file_name: str = 'latest.json'):
    
    object_metadata = read_data(bucket_name, file_name)
    object_body = object_metadata['Body'].read().decode('utf-8')
    
    datetime_dict = convert_extraction_info_to_dict(object_body)
    
    return datetime_dict


def save_new_extraction_info(input_dict: dict, bucket_name: str, file_name: str = 'latest.json'):
    
    encoded_dict = convert_dict_to_bytes(input_dict)
    
    save_data(data=encoded_dict, bucket=bucket_name, file_name=file_name)
