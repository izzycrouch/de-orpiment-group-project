from extract_layer.utils.zip_layer_files import create_zip_layer, upload_zip_to_s3
import zipfile
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError
import io
import pytest

def test_zip_files(fs):
    folder = "layers/layer_1"
    file = "layer_name.txt"
    path = f"{folder}/{file}"
    data = "hello world!"

    fs.create_dir(folder)
    fs.create_file(path, contents=data)

    result = create_zip_layer(folder)
    result.seek(0)
    assert zipfile.is_zipfile(result) == True

    with zipfile.ZipFile(result, mode='r') as zf:
        assert zf.read(file) == b"hello world!"
        info = zf.getinfo(file)
        assert info.file_size == len(data)

@mock_aws
def test_uploads_to_s3():

    s3 = boto3.client("s3", region_name="eu-west-2")
    bucket_name = "test-bucket"
    key = "layer_name.zip"
    data = b"hello world!"

    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    in_memory = io.BytesIO(data)

    upload_zip_to_s3(in_memory, bucket_name, key)
    object = s3.get_object(Bucket=bucket_name, Key=key)
    body = object["Body"].read()

    assert body == data

@mock_aws
def test_library_bucket_not_found():

    bucket_name = "non-existent"
    key = "layer_name.zip"
    data = b"hello world!"

    in_memory = io.BytesIO(data)

    with pytest.raises(ClientError):
        upload_zip_to_s3(in_memory, bucket_name, key)

    