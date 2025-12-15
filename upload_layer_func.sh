#!/bin/bash

rm -f extract_lambda.zip
zip -r extract_lambda.zip extract_layer

aws s3 cp extract_lambda.zip s3://lambda-func-code-aci/extract_lambda.zip

rm -rf extract_lambda_layer
mkdir -p extract_lambda_layer/python
pip install pg8000 dotenv -t extract_lambda_layer/python

cd extract_lambda_layer
zip -r extract_lambda_layer.zip python
aws s3 cp extract_lambda_layer.zip s3://libraries-layer-aci/libraries.zip
cd ..


