import os
os.environ["S3_RAW_BUCKET_NAME"] = "totesys-raw-data-aci"
os.environ["S3_PROCESSED_BUCKET_NAME"]= "totesys-transformed-data-aci-2"
from clean_layer.clean import lambda_handler


lambda_handler('a','b')