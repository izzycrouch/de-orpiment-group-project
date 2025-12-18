import pandas as pd
import pytest
from moto import mock_aws
from io import BytesIO
import boto3
from datetime import datetime
from clean_layer.star_schema_tables.dim_currency import create_dim_currency

@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

@pytest.fixture()
def mock_s3_bucket():
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )   
    return s3

class TestDimCurrency:
    def _upload_parquet(self, mock_s3_bucket, df, key, bucket_name='test_bucket'):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        mock_s3_bucket.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue(),
        )

    def test_dim_currency_returns_isinstance_dataframe(self, mock_s3_bucket):
        key='currency/example.parquet'
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=df, key=key)
        
        result = create_dim_currency(file_path=key, bucket_name='test_bucket')
        
        assert isinstance(result, pd.DataFrame)

    
    def test_dim_currency_returns_correct_column_names(self, mock_s3_bucket):
        key='currency/example.parquet'
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=df, key=key)
        
        expected_columns = ['currency_id', 'currency_code', 'currency_name']
        result = create_dim_currency(file_path=key, bucket_name='test_bucket')
        output_columns = result.columns.values.tolist()

        assert expected_columns == output_columns


    def test_dim_currency_returns_correct_currency_name_for_code(self, mock_s3_bucket):
        key='currency/example.parquet'
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=df, key=key)
        
        result = create_dim_currency(file_path=key, bucket_name='test_bucket')

        assert result['currency_name'].iloc[0] == 'United States Dollar'


    def test_dim_currency_returns_correct_currency_name_for_code_2(self, mock_s3_bucket):
        key='currency/example.parquet'
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'GBP',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')},
                      {'currency_id' : 2,
                      'currency_code' : 'EUR',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=df, key=key)
        
        result = create_dim_currency(file_path=key, bucket_name='test_bucket')

        assert result['currency_name'].iloc[0] == 'British Pound Sterling'
        assert result['currency_name'].iloc[1] == 'Euro'
