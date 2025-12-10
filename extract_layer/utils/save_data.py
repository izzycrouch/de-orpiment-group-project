import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Saves data to s3 bucket
def save_data(data: bytes, bucket_name: str, file_name: str):
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=data)
    
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c

# Reads data from s3 bucket
def read_data(bucket_name: str, file_name: str):
    try:
        s3_client = boto3.client("s3")
        data = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        return data
    
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c




