#!/bin/bash

aws s3api create-bucket \
  --bucket remote-state-aci \
  --region eu-west-2 \
  --create-bucket-configuration LocationConstraint=eu-west-2

aws s3api create-bucket \
  --bucket totesys-raw-data-aci \
  --region eu-west-2 \
  --create-bucket-configuration LocationConstraint=eu-west-2

aws s3api create-bucket \
  --bucket lambda-func-code-aci \
  --region eu-west-2 \
  --create-bucket-configuration LocationConstraint=eu-west-2

aws s3api create-bucket \
  --bucket libraries-layer-aci \
  --region eu-west-2 \
  --create-bucket-configuration LocationConstraint=eu-west-2

aws s3api create-bucket \
  --bucket totesys-transformed-data-aci-1 \
  --region eu-west-2 \
  --create-bucket-configuration LocationConstraint=eu-west-2

