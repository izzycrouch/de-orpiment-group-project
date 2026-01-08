from io import BytesIO
import boto3
import pandas as pd

def get_df(bucket_name: str, file_name: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="eu-west-2")

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    body = obj["Body"].read()

    return pd.read_parquet(BytesIO(body))