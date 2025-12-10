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
        
        with pytest.raises(Exception):
            get_datetime_dict(test_data)


# class TestConvertToEncodedJson:
#     def test_convert_dict_returns_isinstance_dictionary(self):
#         test_data = 
        
#         result = convert_to_enconded_json(test_data)
