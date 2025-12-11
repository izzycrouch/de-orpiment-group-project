import pytest
import boto3
from moto import mock_aws
from zip_lambda_func.zip_lambda import lambda_handler

@pytest.fixture
def mock_s3_bucket():
    with mock_aws():
        yield boto3.client('s3', region_name='eu-west-2') 

@mock_aws
class TestZipLambdaHandler:

    def test_zip_lambda_handler_saves_zip_file_into_s3_bucket(self, mock_s3_bucket):
        bucket_name = "test-bucket"
        input_file_name = "example.py"
        input_data = b"hello world"

        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_file_name, Body=input_data)

        event={'bucket': bucket_name}
        lambda_handler(event)

        list_objects = mock_s3_bucket.list_objects_v2(Bucket=bucket_name)

        bucket_contents = list_objects['Contents']

        content_keys = [content['Key'] for content in bucket_contents]

        assert len(bucket_contents) == 2
        assert content_keys == ['example.py', 'example.zip']


    def test_zip_lambda_handler_saves_zip_files_into_s3_bucket_for_multiple_files(self, mock_s3_bucket):
        bucket_name = "test-bucket"
        
        input_file_name_1 = "example.py"
        input_data_1 = b"hello world"

        input_file_name_2 = "test.py"
        input_data_2 = b"test data"

        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_file_name_1, Body=input_data_1)
        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_file_name_2, Body=input_data_2)
        
        event={'bucket': bucket_name}
        lambda_handler(event)

        list_objects = mock_s3_bucket.list_objects_v2(Bucket=bucket_name)

        bucket_contents = list_objects['Contents']

        content_keys = [content['Key'] for content in bucket_contents]
    
        assert len(bucket_contents) == 4
        assert content_keys == ['example.py', 'example.zip', 'test.py', 'test.zip']
    

    def test_zip_lambda_handler_combines_files_if_in_a_folder_to_a_single_zip_file(self, mock_s3_bucket):
        bucket_name = "test-bucket"
        
        input_object_path_1 = "utils/example.py"
        input_data_1 = b"hello world"

        input_object_path_2 = "utils/test.py"
        input_data_2 = b"test data"

        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_object_path_1, Body=input_data_1)
        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_object_path_2, Body=input_data_2)
        
        event={'bucket': bucket_name}
        lambda_handler(event)

        list_objects = mock_s3_bucket.list_objects_v2(Bucket=bucket_name)

        bucket_contents = list_objects['Contents']

        content_keys = [content['Key'] for content in bucket_contents]

        print(content_keys)
        assert len(bucket_contents) == 3
        assert content_keys == ['utils.zip', 'utils/example.py', 'utils/test.py']
        

    def test_zip_lambda_handler_works_for_single_files_and_folders_as_expected(self, mock_s3_bucket):
        bucket_name = "test-bucket"

        input_object_path = "example.py"
        input_data = b"hello world"
        
        input_object_path_1 = "utils/example.py"
        input_data_1 = b"hello world"

        input_object_path_2 = "utils/test.py"
        input_data_2 = b"test data"

        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_object_path, Body=input_data)
        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_object_path_1, Body=input_data_1)
        mock_s3_bucket.put_object(Bucket=bucket_name, Key=input_object_path_2, Body=input_data_2)
        
        event={'bucket': bucket_name}
        lambda_handler(event)

        list_objects = mock_s3_bucket.list_objects_v2(Bucket=bucket_name)

        bucket_contents = list_objects['Contents']

        content_keys = [content['Key'] for content in bucket_contents]

        assert len(bucket_contents) == 5
        assert content_keys == ['example.py','example.zip' ,'utils.zip', 'utils/example.py', 'utils/test.py']
    
    def test_zip_lambda_handler_raises_error_if_bucket_is_empty(self, mock_s3_bucket):
        bucket_name = "test-bucket"

        mock_s3_bucket.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        event={'bucket': bucket_name}
        with pytest.raises(Exception):
            lambda_handler(event)