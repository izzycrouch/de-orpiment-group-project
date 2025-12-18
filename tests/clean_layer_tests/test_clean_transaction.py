from clean_layer.clean_func.clean_transcation import clean_transcation
import pandas as pd
import pytest
import boto3
from moto import mock_aws
from datetime import datetime, date
from io import BytesIO

@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

class TestCleanTransaction:
    def test_transaction_returns_df(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert isinstance(result, pd.DataFrame)
    

    def test_clean_transaction_returns_df_with_correct_datatypes_for_columns(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result['transaction_id'].dtype == 'int64'
        assert result['transaction_type'].dtype == 'string'
        assert result['sales_order_id'].dtype == 'int64'
        assert result['purchase_order_id'].dtype == 'int64'
        assert result['created_at'].dtype == 'datetime64[ns]'
        assert result['last_updated'].dtype == 'datetime64[ns]'
    

    def test_clean_transaction_returns_correct_datatype_for_ids_if_input_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : '1',
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result['transaction_id'].dtype == 'int64'
    
    \
    def test_clean_transaction_removes_data_if_id_cant_be_cast_as_int(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 'test',
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result.empty
    

    def test_clean_transaction_returns_correct_datatype_for_date_if_input_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : '1',
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result['created_at'].dtype == 'datetime64[ns]'

    
    def test_clean_transaction_removes_data_if_datatype_for_date_cant_be_cast_as_datetime(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : '1',
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : 'test',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_transaction_removes_data_with_duplicated_transaction_id(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')},
                      {'transaction_id' : 1,
                      'transaction_type' : 'PURCHASE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result.shape == (1, 6)
        assert result['transaction_type'].iloc[0] == 'SALE'
    
    
    def test_clean_transaction_makes_transaction_type_upper_string_if_valid(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'sale',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)

        assert result['transaction_type'].iloc[0] == 'SALE'


    def test_clean_transaction_removes_data_if_transaction_type_not_valid(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'TEST',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)
        
        assert result.empty


    def test_clean_transaction_removes_data_if_created_at_after_current_time(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2050-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)
        
        assert result.empty
    

    def test_clean_transaction_removes_data_if_last_updated_after_current_time(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2050-12-15 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)
        
        assert result.empty

    
    def test_clean_transaction_removes_data_if_last_updated_before_created_at(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'transaction_id' : 1,
                      'transaction_type' : 'SALE',
                      'sales_order_id' : 1,
                      'purchase_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-14 15:51:20.825099')}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_transcation(file_path=file_name, bucket_name=bucket_name)
        
        assert result.empty