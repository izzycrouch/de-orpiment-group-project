import logging
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime as dt
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(encoding='utf-8', level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

def lambda_func(event, context):
    s3_client = boto3.client("s3")
    timestamp = str(int(dt.timestamp(dt.now())))

    output_data = clean_data()

    key = f"clean_data_{timestamp}.json"
    write_result = write_to_s3(s3_client, output_data, BUCKET_NAME, key)

    if write_result:
        logger.info("New cleaned data was written to the s3 bucket.")
    else:
        logger.info("No new cleaned data was written to the s3 bucket.")

def clean_data():
    pass

def write_to_s3(client, data, bucket, key):
    body = json.dumps(data)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body)
        return True
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        return False

## connect to raw data bucket
## get the parquet before hte laster