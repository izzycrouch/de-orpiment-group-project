resource "aws_lambda_function" "extract_raw_data_function" {
  function_name = "extract-func"
  role          = aws_iam_role.s3_role.arn

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

  timeout = 200
}

resource "aws_lambda_layer_version" "libraries_layer" {
  s3_bucket           = "libraries-layer-aci"
  s3_key              = "libraries.zip"
  layer_name          = "libraries_layer"
  compatible_runtimes = ["python3.12"]
}