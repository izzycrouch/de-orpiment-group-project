from clean_layer.clean_staff import clean_staff_table
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

class TestCleanStaff:

    def test_correct_data_types(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "staff/example.parquet"

        test_data = [{
            'staff_id' : 1, 
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'department_id' : 2, 
            'first_name' : 'Harry',
            'last_name': 'Potter',
            'email_address': 'harry.potter123@gmail.com'
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_staff_table(file_path=file_name, bucket_name=bucket_name)

        assert isinstance(df, pd.DataFrame)
        assert df["staff_id"].dtypes == int
        assert df["first_name"].dtypes == object
        assert df["last_name"].dtypes == object
        assert df["department_id"].dtypes == int
        assert df["email_address"].dtypes == object
        assert df["created_at"].dtypes == 'datetime64[ns]'
        assert df["last_updated"].dtypes == 'datetime64[ns]'

    def test_no_null_values(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "staff/example.parquet"

        test_data = [{
            'staff_id' : 1, 
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'department_id' : 2, 
            'first_name' : None,
            'last_name': 'Potter',
            'email_address': None
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_staff_table(file_path=file_name, bucket_name=bucket_name)
    
        assert len(df['first_name']) == 0
        assert len(df['email_address']) == 0

        null_mask = df.isnull().any(axis=1)
        assert null_mask.any() == False

    def test_valid_datetime(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "staff/example.parquet"

        test_data = [{
            'staff_id' : 1, 
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2026-12-16 15:51:20.825099'),
            'department_id' : 2, 
            'first_name' : 'Harry',
            'last_name': 'Potter',
            'email_address': 'harry.potter123@gmail.com'
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_staff_table(file_path=file_name, bucket_name=bucket_name)

        today = pd.Timestamp.now()
        
        assert (df['created_at'] <= today).all()
        assert (df['last_updated'] <= today).all()
        
    def test_email_validation(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')
        
        bucket_name = "test-bucket"
        file_name = "staff/example.parquet"

        test_data = [{
            'staff_id' : 1, 
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'department_id' : 2, 
            'first_name' : 'Harry',
            'last_name': 'Potter',
            'email_address': 'har@ry.potter123@gmail.com'
            }]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.read())
        df = clean_staff_table(file_path=file_name, bucket_name=bucket_name)

        pattern = r"^[a-zA-Z0-9._%+'-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        assert df["email_address"].str.match(pattern).all()
        assert len(df['email_address']) == 0