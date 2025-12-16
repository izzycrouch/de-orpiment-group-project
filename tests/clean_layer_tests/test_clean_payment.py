from clean_layer.clean_payment import clean_payment_table
import pandas as pd
from datetime import datetime
import pytest
import boto3
from moto import mock_aws
from datetime import datetime
from io import BytesIO

@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

class TestCleanPayment:

    def test_correct_data_types(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "payment/example.parquet"

        test_data = [{
            'payment_id' : 1, 
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'transaction_id' : 2, 
            'counterparty_id' : 3,
            'payment_amount': 2.50,
            'currency_id': 4,
            'payment_type_id': 5,
            'paid': True,
            'payment_date': datetime.fromisoformat('2025-12-15'),
            'company_ac_number' : 12345678,
            'counterparty_ac_number' : 87654321
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_payment_table(file_path=file_name, bucket_name=bucket_name)

        assert isinstance(df, pd.DataFrame)
        assert df["payment_id"].dtypes == int
        assert df["created_at"].dtypes == 'datetime64[ns]'
        assert df["transaction_id"].dtypes == int
        assert df["payment_amount"].dtypes == float
        assert df["paid"].dtypes == bool
        assert df["payment_date"].dtypes == 'datetime64[ns]'
        assert df["company_ac_number"].dtypes == int

    def test_drop_null_values(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "payment/example.parquet"

        test_data = [{
            'payment_id' : 1, 
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'transaction_id' : 2, 
            'counterparty_id' : 3,
            'payment_amount': None,
            'currency_id': 4,
            'payment_type_id': 5,
            'paid': True,
            'payment_date': datetime.fromisoformat('2025-12-15'),
            'company_ac_number' : None,
            'counterparty_ac_number' : 87654321
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_payment_table(file_path=file_name, bucket_name=bucket_name)

        assert len(df['company_ac_number']) == 0
        assert len(df['payment_amount']) == 0

        null_mask = df.isnull().any(axis=1)
        assert null_mask.any() == False

    def test_valid_datetime(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "payment/example.parquet"

        test_data = [{
            'payment_id' : 1, 
            'created_at' : datetime.fromisoformat('2026-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'transaction_id' : 2, 
            'counterparty_id' : 3,
            'payment_amount': 2.50,
            'currency_id': 4,
            'payment_type_id': 5,
            'paid': True,
            'payment_date': datetime.fromisoformat('2025-12-15'),
            'company_ac_number' : 12345678,
            'counterparty_ac_number' : 87654321
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_payment_table(file_path=file_name, bucket_name=bucket_name)

        today = datetime.today()
        
        assert (df['payment_date'] <= today).all()
        assert (df['created_at'] <= today).all()
        assert len(df['created_at']) == 0
        assert (df['last_updated'] <= today).all()
        