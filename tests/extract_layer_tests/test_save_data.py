import boto3
import pytest
from moto import mock_aws
from botocore.exceptions import ClientError
from extract_layer.utils.save_data import save_data, read_data

@pytest.fixture
def mock_s3_bucket():
    with mock_aws():
        yield boto3.client('s3', region_name='eu-west-2')

class TestSaveData:
    
    @mock_aws
    def test_save_date_saves_input_into_s3_bucket(self, mock_s3_bucket):
        
        bucket_name = "test-bucket"
        file_name = "example.json"
        input_data = b"hello world"
        
        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        
        save_data(data=input_data, bucket_name=bucket_name, file_name=file_name)
        response = mock_s3_bucket.get_object(Bucket=bucket_name, Key=file_name)
        
        saved_data = response["Body"].read()

        assert saved_data == input_data


    def test_save_data_returns_data_as_isinstance_bytes(self, mock_s3_bucket):
        
        bucket_name = "test-bucket"
        file_name = "example.json"
        input_data = b"hello world"
        
        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        
        save_data(data=input_data, bucket_name=bucket_name, file_name=file_name)
        response = mock_s3_bucket.get_object(Bucket=bucket_name, Key=file_name)
        
        saved_data = response["Body"].read()

        assert isinstance(saved_data, bytes)


    def test_save_data_raises_error_when_bucket_doesnt_exist(self):
        bucket_name = "non-existent"
        file_name = "example.json"
        data = b"hello world"

        with pytest.raises(ClientError):
            save_data(data=data, bucket_name=bucket_name, file_name=file_name)


class TestReadData:
    @mock_aws
    def test_read_data_returns_input_data_after_being_saved(self, mock_s3_bucket):
        bucket_name = "test-bucket"
        file_name = "example.json"
        input_data = b"hello world"
        
        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        
        save_data(data=input_data, bucket_name=bucket_name, file_name=file_name)
        result = read_data(bucket_name=bucket_name, file_name=file_name)["Body"].read()
        
        assert result == input_data
    
    def test_read_data_returns_dict_of_metadata(self, mock_s3_bucket):
        bucket_name = "test-bucket"
        file_name = "example.json"
        input_data = b"hello world"
        
        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        
        save_data(data=input_data, bucket_name=bucket_name, file_name=file_name)
        result = read_data(bucket_name=bucket_name, file_name=file_name)

        assert isinstance(result, dict)


    def test_read_data_raises_error_when_bucket_doesnt_exist(self):
        bucket_name = "test-bucket"
        file_name = "example.json"

        with pytest.raises(ClientError):
            read_data(bucket_name=bucket_name, file_name=file_name)
    

    def test_read_data_raises_error_if_file_name_not_founc(self, mock_s3_bucket):
        bucket_name = "test-bucket"
        file_name = "example.json"

        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        with pytest.raises(ClientError):
            read_data(bucket_name=bucket_name, file_name=file_name)
    
    



