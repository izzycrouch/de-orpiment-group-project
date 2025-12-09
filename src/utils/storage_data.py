import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


#key
def storage_data(data, bucket, file_name):
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket, Key=file_name, Body=data)
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c


def read_data(bucket,file_name):
    try:
        s3_client = boto3.client("s3")
        data = s3_client.get_object(Bucket = bucket, Key = file_name)
        return data
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c




