import io
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def save_data(df, bucket_name: str, file_name: str):
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c
