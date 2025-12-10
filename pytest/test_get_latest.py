import pytest
from src.utils.get_latest import get_datetime_dict, convert_to_enconded_json
from datetime import datetime


class TestGetDatetimeDict:
    
    def test_get_datetime_dict_returns_isinstance_dictionary(self):
        test_data = "{\"test_table_name\" : \"2025-12-10T11:28:35.000372\"}"
        result = get_datetime_dict(test_data)

        assert isinstance(result, dict)
    
    def test_get_datetime_dict_returns_correct_key(self):
        test_data = "{\"test_table_name\" : \"2025-12-10T11:28:35.000372\"}"
        result = get_datetime_dict(test_data)

        keys_list = list(result.keys())

        assert keys_list == ['test_table_name']

    def test_get_datetime_dict_returns_value_isinstance_datetime(self):
        test_data = "{\"test_table_name\" : \"2025-12-10T11:28:35.000372\"}"
        result = get_datetime_dict(test_data)

        assert isinstance(result['test_table_name'], datetime)
    
    def test_get_datetime_dict_raises_error_when_input_value_is_not_valid_datetime(self):
        test_data = "{\"test_table_name\" : \"test_date_time\"}"
        
        with pytest.raises(ValueError, match='Value cannot be converted to datetime datatype.'):
            get_datetime_dict(test_data)


class TestConvertToEncodedJson:
    
    def test_convert_to_encoded_json_returns_isinstance_bytes(self):
        test_data = {'test_key': datetime.now()}
        
        result = convert_to_enconded_json(test_data)

        assert isinstance(result, bytes)
    
    def test_convert_to_encoded_json_raises_error_if_value_cant_be_converted_to_datetime(self):
        test_data = {'test_key': 'test_date_time'}
        
        with pytest.raises(TypeError, match='Value is not a datetime datatype.'):
            convert_to_enconded_json(test_data)

