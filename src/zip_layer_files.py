import logging
import boto3
import zipfile
import io
import os
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_zip_layer(folder_name: str):
    in_memory = io.BytesIO()

    with zipfile.ZipFile(in_memory, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(folder_name):
                for file in files:
                    file_path = os.path.join(root, file)
                    zip_file = os.path.relpath(file_path, folder_name)
                    zf.write(file_path, zip_file)

    in_memory.seek(0)
    logger.info(f"Zip layer files created for {folder_name}")

    return in_memory


def upload_zip_to_s3(in_memory: io.BytesIO, bucket: str, key: str):

    try:
        in_memory.seek(0)
        s3_client = boto3.client("s3")
        s3_client.upload_fileobj(in_memory, bucket, key)
        logger.info(f"Uploaded {key} to s3://{bucket}/")
    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c

