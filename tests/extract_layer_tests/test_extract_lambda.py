# import os
# import boto3
# import pandas as pd
# from io import BytesIO
# from moto import mock_aws
# from extract_layer.utils.extraction_info import convert_extraction_info_to_dict
# from extract_layer.extract_lambda import lambda_handler
# import pyarrow.parquet as pq
# import pytest
# from seed_test_database import seed_test_db

# @pytest.fixture(autouse=True)
# def seed_db():
#     seed_test_db()

# @pytest.fixture(autouse=True)
# def mock_s3_bucket():
#     with mock_aws():
#         mock_s3_bucket = boto3.client("s3", region_name="eu-west-2")
#         yield mock_s3_bucket

# def test_save_date_saves_latest_timestamp_into_s3_bucket(mock_s3_bucket):

#     bucket_name = "test-bucket"

#     mock_s3_bucket.create_bucket(
#         Bucket=bucket_name,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )
    
#     mock_s3_bucket.put_object(Bucket=bucket_name, Key='latest.json', Body=b'{}')

#     os.environ["S3_BUCKET_NAME"] = bucket_name

#     lambda_handler('a','b')

#     response = mock_s3_bucket.get_object(Bucket=bucket_name, Key='latest.json')

#     saved_data = response['Body'].read().decode('utf-8')

#     converted_data = convert_extraction_info_to_dict(saved_data)
#     assert isinstance(converted_data,dict)


# def test_save_date_saves_tables_into_s3_bucket(mock_s3_bucket):

#     bucket_name = "test-bucket"

#     mock_s3_bucket.create_bucket(
#         Bucket=bucket_name,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )
    
#     mock_s3_bucket.put_object(Bucket=bucket_name, Key='latest.json', Body=b'{}')

#     os.environ["S3_BUCKET_NAME"] = bucket_name

#     lambda_handler('a','b')

#     response = mock_s3_bucket.list_objects_v2(
#     Bucket=bucket_name)
#     key = response['Contents'][0]['Key']
#     print(key)
#     response = mock_s3_bucket.get_object(Bucket=bucket_name, Key=key)
#     #for csv:
#     # csv_string = response['Body'].read().decode('utf-8')
#     # df = pd.read_csv(StringIO(csv_string))

#     parquet_bytes = response['Body'].read()

#     df = pq.read_table(BytesIO(parquet_bytes)).to_pandas()
#     assert isinstance(df, pd.DataFrame)
