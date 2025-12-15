import pytest
from extract_layer.utils.extraction_info import convert_extraction_info_to_dict, convert_dict_to_bytes, get_latest_extraction_info
from datetime import datetime
from moto import mock_aws
import boto3
import json


class TestConvertExtractionInfoToDict:

    def test_convert_extraction_info_to_dict_returns_isinstance_dictionary(self):
        test_data = "{\"test_table_name\" : \"2025-12-10T11:28:35.000372\"}"
        result = convert_extraction_info_to_dict(test_data)

        assert isinstance(result, dict)


    def test_convert_extraction_info_to_dict_returns_correct_key(self):
        test_data = "{\"test_table_name\" : \"2025-12-10T11:28:35.000372\"}"
        result = convert_extraction_info_to_dict(test_data)

        keys_list = list(result.keys())

        assert keys_list == ['test_table_name']


    def test_convert_extraction_info_to_dict_returns_value_isinstance_datetime(self):
        test_data = "{\"test_table_name\" : \"2025-12-10T11:28:35.000372\"}"
        result = convert_extraction_info_to_dict(test_data)

        assert isinstance(result['test_table_name'], datetime)

    def test_convert_extraction_info_to_dict_raises_error_when_input_value_is_not_valid_datetime(self):
        test_data = "{\"test_table_name\" : \"test_date_time\"}"

        with pytest.raises(ValueError, match='Value cannot be converted to datetime datatype.'):
            convert_extraction_info_to_dict(test_data)


class TestConvertDictToBytes:

    def test_convert_dict_to_bytes_returns_isinstance_bytes(self):
        test_data = {'test_key': datetime(2025, 12, 10, 10, 10, 10, 0000)}

        result = convert_dict_to_bytes(test_data)

        assert isinstance(result, bytes)


    def test_convert_dict_to_bytes_raises_error_if_value_cant_be_converted_to_datetime(self):
        test_data = {'test_key': 'test_date_time'}

        with pytest.raises(TypeError, match='Value is not a datetime datatype.'):
            convert_dict_to_bytes(test_data)


@pytest.fixture
def mock_s3_bucket():
    with mock_aws():
        yield boto3.client('s3', region_name='eu-west-2')


class TestGetLatestExtractionInfo:

    @mock_aws
    def test_get_latest_returns_isinstance_dictionary(self, mock_s3_bucket):
        test_bucket = 'test_bucket'
        test_key = 'test-key.json'

        test_dict = {'test_key': "2025-12-10T10:10:10.0000"}
        test_body = json.dumps(test_dict).encode('utf-8')

        mock_s3_bucket.create_bucket(Bucket=test_bucket, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        mock_s3_bucket.put_object(Bucket=test_bucket, Key=test_key, Body=test_body)

        result = get_latest_extraction_info(bucket_name=test_bucket, file_name=test_key)

        assert isinstance(result, dict)


    def test_get_latest_returns_correct_dictionary_key(self, mock_s3_bucket):
        test_bucket = 'test_bucket'
        test_key = 'test-key.json'

        test_dict = {'test_key': "2025-12-10T10:10:10.0000"}
        test_body = json.dumps(test_dict).encode('utf-8')

        mock_s3_bucket.create_bucket(Bucket=test_bucket, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        mock_s3_bucket.put_object(Bucket=test_bucket, Key=test_key, Body=test_body)

        result = get_latest_extraction_info(bucket_name=test_bucket, file_name=test_key)
        list_keys = list(result.keys())

        assert list_keys == ['test_key']


    def test_get_latest_returns_correct_dictionary_value(self, mock_s3_bucket):
        test_bucket = 'test_bucket'
        test_key = 'test-key.json'

        test_dict = {'test_key': "2025-12-10T10:10:10.0000"}
        test_body = json.dumps(test_dict).encode('utf-8')

        mock_s3_bucket.create_bucket(Bucket=test_bucket, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        mock_s3_bucket.put_object(Bucket=test_bucket, Key=test_key, Body=test_body)

        result = get_latest_extraction_info(bucket_name=test_bucket, file_name=test_key)

        assert result['test_key'] == datetime(2025, 12, 10, 10, 10, 10, 0000)