from clean_layer.clean import lambda_func
from moto import mock_aws
import pytest
import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime


@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

@pytest.fixture()
def mock_s3_bucket():

    s3 = boto3.client("s3", region_name="eu-west-2")

    s3.create_bucket(
        Bucket='test_raw_bucket',
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3.create_bucket(
        Bucket='test_processed_bucket',
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return s3

class TestCleanBucket:
    def _upload_parquet(self, mock_s3_bucket, df, key, bucket_name):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        mock_s3_bucket.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue(),
        )


    def test_lambda_func(self, mock_s3_bucket):
        key='currency/example.parquet'
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=df, key=key,bucket_name='test_raw_bucket')


        key='currency/example2.parquet'
        df = pd.DataFrame([{'currency_id' : 2,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=df, key=key,bucket_name='test_raw_bucket')

        lambda_func({
                    "Records": [
                        {
                        "eventVersion": "2.1",
                        "eventSource": "aws:s3",
                        "awsRegion": "us-east-2",
                        "eventTime": "2019-09-03T19:37:27.192Z",
                        "eventName": "ObjectCreated:Put",
                        "userIdentity": {
                            "principalId": "AWS:AIDAINPONIXQXHT3IKHL2"
                        },
                        "requestParameters": {
                            "sourceIPAddress": "205.255.255.255"
                        },
                        "responseElements": {
                            "x-amz-request-id": "D82B88E5F771F645",
                            "x-amz-id-2": "vlR7PnpV2Ce81l0PRw6jlUpck7Jo5ZsQjryTjKlc5aLWGVHPZLj5NeC6qMa0emYBDXOo6QBU0Wo="
                        },
                        "s3": {
                            "s3SchemaVersion": "1.0",
                            "configurationId": "828aa6fc-f7b5-4305-8584-487c791949c1",
                            "bucket": {
                            "name": "my-bucket",
                            "ownerIdentity": {
                                "principalId": "A3I5XTEXAMAI3E"
                            },
                            "arn": "arn:aws:s3:::my-bucket"
                            },
                            "object": {
                            "key": "foo.jpg",
                            "size": 1305107,
                            "eTag": "b21b84d653bb07b05b1e6b33684dc11b",
                            "sequencer": "0C0F6F405D6ED209E1"
                            }
                        }
                        }
                    ]
                    },'b')
        upload_file = mock_s3_bucket.list_objects_v2(Bucket= 'test_processed_bucket')
        assert False




