resource "aws_lambda_function" "extract_raw_data_function" {
  function_name = "extract-func"
  role          = aws_iam_role.s3_role.arn

  s3_bucket = var.lambda_code_bucket_name
  s3_key    = "extract.zip"

  handler = "extract.lambda_handler"
  runtime = var.python_runtime

  layers = [aws_lambda_layer_version.get_database_layer.arn,
    aws_lambda_layer_version.libraries_layer.arn,
  aws_lambda_layer_version.utils_layer.arn]

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.raw_data_bucket.bucket
    }
  }

  timeout = 200
}
  function_name   = "extract-func"
  role            = aws_iam_role.s3_role.arn
  
  s3_bucket   = "${var.lambda_code_bucket_name}"
  s3_key      = "extract.zip"

  handler     = "extract_lambda.lambda_handler"
  runtime     = var.python_runtime

  layers      = [
    aws_lambda_layer_version.get_database_layer.arn,
    aws_lambda_layer_version.libraries_layer.arn,
    aws_lambda_layer_version.utils_layer.arn
    ]
}

resource "aws_lambda_function" "zip_lambda_function" {
  function_name   = "zip-lambda-func"
  role            = aws_iam_role.s3_role.arn
  
  s3_bucket   = "${var.lambda_code_bucket_name}"
  s3_key      = "zip_lambda.zip"

  handler     = "zip_lambda_func.lambda_handler"
  runtime     = var.python_runtime

  layers      = [aws_lambda_layer_version.libraries_layer.arn]
}

resource "aws_lambda_layer_version" "libraries_layer" {
  s3_bucket = var.libraries_layer_bucket_name
  s3_key    = "libraries.zip"

  layer_name = "libraries_layer"

  compatible_runtimes = [var.python_runtime]
}

resource "aws_lambda_layer_version" "utils_layer" {
  s3_bucket = var.lambda_code_bucket_name
  s3_key    = "extract_layer/utils.zip"

  layer_name = "utils_layer"

  compatible_runtimes = [var.python_runtime]
}

resource "aws_lambda_layer_version" "get_database_layer" {
  s3_bucket = var.lambda_code_bucket_name
  s3_key    = "extract_layer/get_database.zip"

  layer_name = "get_database_layer"

  compatible_runtimes = [var.python_runtime]
}
