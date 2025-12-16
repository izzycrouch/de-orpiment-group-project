from clean_layer.utils.save_df_into_parquet import save_data
from clean_layer.utils.get_raw_data import get_df
import pytest
from moto import mock_aws
import boto3
import pandas as pd
import io

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "label": [0, 1, 0],
        "value": [0.1, 0.2, 0.3],
    })

@mock_aws
def test_save_data(sample_df):
    bucket_name = "test-bucket"
    mock_s3 = boto3.client("s3", region_name="eu-west-2")

    mock_s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

    save_data(sample_df,bucket_name,file_name='test_parquet')


    resp = mock_s3.list_objects_v2(Bucket=bucket_name)
    assert resp["KeyCount"] > 0

    resp = mock_s3.get_object(
            Bucket=bucket_name,
            Key='test_parquet'
        )

    df = pd.read_parquet(io.BytesIO(resp["Body"].read()))

    assert isinstance(df, pd.DataFrame)
    assert df.equals(sample_df)





