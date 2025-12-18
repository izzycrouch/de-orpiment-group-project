from clean_layer.clean_func.clean_design import clean_design
from clean_layer.star_schema_tables.dim_design import create_dim_design
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

class TestDimDesign:
    def test_dim_design_returns_dataframe(self):
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

        cleaned = clean_design(file_path=file_name, bucket_name=bucket_name)
        df = create_dim_design(cleaned)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):
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

        cleaned = clean_design(file_path=file_name, bucket_name=bucket_name)
        df = create_dim_design(cleaned)

        expected_cols = [
                'design_id',
                'design_name',
                'file_location',
                'file_name',
            ]
        assert (expected_cols == df.columns).all()

    def test_correct_data_types(self):
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

        cleaned = clean_design(file_path=file_name, bucket_name=bucket_name)
        df = create_dim_design(cleaned)

        assert df['design_id'].dtype == int
        assert df['design_name'].dtype == 'string'
        assert df['file_location'].dtype == 'string'
        assert df['file_name'].dtype == 'string'
