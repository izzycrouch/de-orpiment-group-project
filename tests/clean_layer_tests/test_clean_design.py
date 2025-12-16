from clean_layer.clean_design import clean_design
import pandas as pd
import pytest
import boto3
from moto import mock_aws
from datetime import datetime
from io import BytesIO

@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

class TestCleanDesign:
    def test_clean_design_returns_df(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert isinstance(result, pd.DataFrame)
    

    def test_clean_design_returns_correct_datatypes_for_columns(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)
      
        assert result['design_id'].dtype == 'int64'
        assert result['created_at'].dtype == 'datetime64[ns]'
        assert result['design_name'].dtype == 'string'
        assert result['file_location'].dtype == 'string'
        assert result['file_name'].dtype == 'string'
        assert result['last_updated'].dtype == 'datetime64[ns]'
    

    def test_clean_design_returns_correct_datatypes_for_design_id_when_input_as_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : '1', 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)
      
        assert result['design_id'].dtype == 'int64'
    

    def test_clean_design_returns_correct_datatypes_for_created_at_when_input_as_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)
      
        assert result['created_at'].dtype == 'datetime64[ns]'
    

    def test_clean_design_removes_data_if_design_id_datatype_cant_be_cast_as_int(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 'test', 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_design_removes_data_if_created_at_datatype_cant_be_cast_as_datetime(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : 'test',
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_design_removes_data_if_design_name_datatype_cant_be_cast_as_srting(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : None, 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.empty

    def test_clean_design_strips_strings(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : ' test ', 
                      'file_location' : ' /test ',
                      'file_name' : ' test.json ',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result['design_name'].iloc[0] == 'test'
        assert result['file_location'].iloc[0] == '/test'
        assert result['file_name'].iloc[0] == 'test.json'
    
    def test_clean_design_only_keeps_first_occurance_when_there_is_a_duplicate_design_id(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')},
                      {'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-16 15:51:20.825099'),
                      'design_name' : 'test2', 
                      'file_location' : '/test2',
                      'file_name' : 'test2.json',
                      'last_updated' : datetime.fromisoformat('2025-12-16 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.shape == (1, 6)
        assert result['design_name'].iloc[0] == 'test'
    
    def test_clean_design_removes_rows_where_created_at_and_last_updated_is_after_current_time(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2050-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')},
                      {'design_id' : 2, 
                      'created_at' : datetime.fromisoformat('2025-12-16 15:51:20.825099'),
                      'design_name' : 'test2', 
                      'file_location' : '/test2',
                      'file_name' : 'test2.json',
                      'last_updated' : datetime.fromisoformat('2050-12-16 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.empty
    
    def test_clean_design_removes_rows_where_file_location_invalid(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : 'test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.empty
    
    def test_clean_design_removes_rows_where_file_name_invalid(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = "test-bucket"
        file_name = "design/example.parquet"
        test_data = [{'design_id' : 1, 
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test', 
                      'file_location' : '/test',
                      'file_name' : 'test',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_design(file_path=file_name, bucket_name=bucket_name)

        assert result.empty