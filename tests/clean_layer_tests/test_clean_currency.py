from clean_layer.clean_currency import clean_currency
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

class TestCleanCurrency:
    def test_clean_currency_returns_df(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert isinstance(result, pd.DataFrame)

    
    def test_clean_currency_returns_df_with_correct_column_datatypes(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result['currency_id'].dtype == 'int64'
        assert result['currency_code'].dtype == 'string'
        assert result['created_at'].dtype == 'datetime64[ns]'
        assert result['last_updated'].dtype == 'datetime64[ns]'
    

    def test_clean_currency_returns_df_with_correct_column_datatypes_if_currency_id_input_as_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : '1', 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result['currency_id'].dtype == 'int64'

    
    def test_clean_currency_returns_df_with_correct_column_datatypes_if_date_input_as_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result['created_at'].dtype == 'datetime64[ns]'
    

    def test_clean_currency_removes_data_if_currency_id_cant_be_made_into_an_int(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 'test', 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result.empty
    

    def test_clean_currency_removes_data_if_date_cant_be_made_into_an_datetime_type(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : 'test',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_currency_removes_data_if_currency_id_is_duplicated(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')},
                      {'currency_id' : 1, 
                      'currency_code' : 'GBP',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result['currency_code'].iloc[0] == 'USD'
        assert result.shape == (1, 4)
    

    def test_clean_currency_removes_data_if_last_updated_before_created_at(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-14 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result.empty
    

    def test_clean_currency_removes_data_if_created_at_after_current_datetime(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2050-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result.empty
    

    def test_clean_currency_removes_data_if_last_updated_after_current_datetime(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2050-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result.empty

    
    def test_clean_currency_returns_stripped_string_for_currency_code(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : ' USD ',
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result['currency_code'].iloc[0] == 'USD'


    def test_clean_currency_returns_df_with_upper_currency_code_if_valid(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'usd',
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result['currency_code'].iloc[0] == 'USD'
    

    def test_clean_currency_removes_data_if_currency_code_not_valid(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'currency_id' : 1, 
                      'currency_code' : 'test',
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        
        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_currency(file_path=file_name, bucket_name=bucket_name)

        assert result.empty