#!/bin/bash

rm -f extract_lambda.zip
zip -r extract_lambda.zip extract_layer
aws s3 cp extract_lambda.zip s3://lambda-func-code-aci/extract_lambda.zip

rm -rf extract_lambda_layer
mkdir -p extract_lambda_layer/python
pip install pg8000 dotenv sqlalchemy -t extract_lambda_layer/python

cd extract_lambda_layer
zip -r extract_lambda_layer.zip python
aws s3 cp extract_lambda_layer.zip s3://libraries-layer-aci/libraries.zip
cd ..


rm -f transform_lambda.zip
zip -r transform_lambda.zip clean_layer
aws s3 cp transform_lambda.zip s3://lambda-func-code-aci/transform_lambda.zip

# rm -rf transform_lambda_layer
# mkdir -p transform_lambda_layer/python
# pip install pg8000 dotenv -t transform_lambda_layer/python

# cd transform_lambda_layer
# zip -r transform_lambda_layer.zip python
# aws s3 cp transform_lambda_layer.zip s3://libraries-layer-aci/libraries.zip
# cd ..



rm -f load_lambda.zip
zip -r load_lambda.zip load_layer
aws s3 cp load_lambda.zip s3://lambda-func-code-aci/load_lambda.zip

# rm -rf transform_lambda_layer
# mkdir -p transform_lambda_layer/python
# pip install pg8000 dotenv -t transform_lambda_layer/python

# cd transform_lambda_layer
# zip -r transform_lambda_layer.zip python
# aws s3 cp transform_lambda_layer.zip s3://libraries-layer-aci/libraries.zip
# cd ..
