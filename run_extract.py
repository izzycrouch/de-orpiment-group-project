import os
os.environ["S3_RAW_BUCKET_NAME"] = "totesys-raw-data-aci"
os.environ["S3_PROCESSED_BUCKET_NAME"]= "totesys-transformed-data-aci-2"
from extract_layer.extract_lambda import lambda_handler
from clean_layer.clean import lambda_handler


lambda_handler(['payment/year=2026/month=1/day=7/batch_20260107T121905Z.parquet'],'b')