resource "aws_lambda_layer_version" "libraries_layer" {
  s3_bucket           = "libraries-layer-aci"
  s3_key              = "libraries.zip"
  layer_name          = "libraries_layer"
  compatible_runtimes = ["python3.12"]
}

resource "aws_lambda_function" "extract_raw_data_function" {
  function_name = "extract-func"
  role          = aws_iam_role.extract_role.arn

  s3_bucket = "lambda-func-code-aci"
  s3_key    = "extract_lambda.zip"

  handler = "extract_layer.extract_lambda.lambda_handler"
  runtime = var.python_runtime

  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:20",
  aws_lambda_layer_version.libraries_layer.arn]

  environment {
    variables = {
      S3_BUCKET_NAME = "totesys-raw-data-aci"
      ENV            = "prod"
    }
  }

  timeout = 900
}


resource "aws_lambda_function" "transform_data_function" {
  function_name = "transform-func"
  role          = aws_iam_role.transform_role.arn

  s3_bucket = "lambda-func-code-aci"
  s3_key    = "transform_lambda.zip"

  handler = "clean_layer.clean.lambda_handler"
  runtime = var.python_runtime

  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:20",
  aws_lambda_layer_version.libraries_layer.arn]

  environment {
    variables = {
      S3_RAW_BUCKET_NAME       = "totesys-raw-data-aci"
      S3_PROCESSED_BUCKET_NAME = "totesys-transformed-data-aci-2"
      ENV                      = "prod"
    }
  }

  timeout = 900
}



resource "aws_lambda_function" "load_function" {
  function_name = "load-func"
  role          = aws_iam_role.load_role.arn

  s3_bucket = "lambda-func-code-aci"
  s3_key    = "load_lambda.zip"

  handler = "load_layer.load.lambda_handler"
  runtime = var.python_runtime

  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:20",
  aws_lambda_layer_version.libraries_layer.arn]

  environment {
    variables = {
      S3_BUCKET_NAME = "totesys-transformed-data-aci-2"
      ENV            = "prod"
    }
  }

  timeout = 900
}
