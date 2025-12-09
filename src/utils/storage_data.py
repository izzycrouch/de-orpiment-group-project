"""Lambda handler to get quotes and write them to S3."""

import requests
import boto3
import os
import json
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime as dt
from random import random, randint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


#key
def storage_data(data, bucket, file_name):
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket, Key=file_name, Body=data)
        return True
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        return False


