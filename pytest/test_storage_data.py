import boto3
import pytest
from moto import mock_aws
from botocore.exceptions import ClientError
from src.utils.storage_data import storage_data,read_data


@mock_aws
def test_storage_data_success():

    s3 = boto3.client("s3", region_name="eu-west-2")
    bucket_name = "test-bucket"
    file_name = "example.json"
    data = b"hello world"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    storage_data(data, bucket_name, file_name)
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    stored_data = response["Body"].read()

    assert stored_data == data


@mock_aws
def test_storage_data_bucket_not_found():
    bucket_name = "non-existent"
    file_name = "example.json"
    data = b"hello world"

    with pytest.raises(ClientError):
        storage_data(data=data, bucket=bucket_name, file_name=file_name)

@mock_aws
def test_read_data_success():
    s3 = boto3.client("s3", region_name="eu-west-2")
    bucket_name = "test-bucket"
    file_name = "example.json"
    data = b"hello world"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    storage_data(data, bucket_name, file_name)
    res = read_data(bucket_name,file_name)["Body"].read()
    assert res == data

