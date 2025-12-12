# data "archive_file" "lambda" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_file      = "${path.module}/../extract_layer/extract_lambda.py"
#   output_path      = "${path.module}/../extract_lambda.zip"
# }



resource "aws_lambda_function" "extract_raw_data_function" {
  function_name = "extract-func"
  role          = aws_iam_role.s3_role.arn

  s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key    = "extract.zip"

  # filename = data.archive_file.lambda.output_path

  handler = "extract_layer.extract_lambda.lambda_handler"
  runtime = var.python_runtime

  layers = [
    aws_lambda_layer_version.large_libraries_layer.arn,
    aws_lambda_layer_version.small_libraries_layer.arn,
  aws_lambda_layer_version.utils_layer.arn]

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.raw_data_bucket.bucket
    }
  }

  timeout = 200
}

resource "aws_lambda_function" "zip_lambda_function" {
  function_name = "zip-lambda-func"
  role          = aws_iam_role.s3_role.arn

  s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key    = "zip_lambda.zip"

  handler = "zip_lambda.zip_lambda.lambda_handler"
  runtime = var.python_runtime

  layers = [aws_lambda_layer_version.large_libraries_layer.arn, aws_lambda_layer_version.small_libraries_layer.arn]
}

resource "aws_lambda_layer_version" "large_libraries_layer" {
  s3_bucket = aws_s3_bucket.libraries_layer_bucket.bucket
  s3_key    = "large_libraries.zip"

  layer_name = "large_libraries_layer"

  compatible_runtimes = [var.python_runtime]
}


resource "aws_lambda_layer_version" "small_libraries_layer" {
  s3_bucket = aws_s3_bucket.libraries_layer_bucket.bucket
  s3_key    = "small_libraries.zip"

  layer_name = "small_libraries_layer"

  compatible_runtimes = [var.python_runtime]
}



resource "aws_lambda_layer_version" "utils_layer" {
  s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key    = "extract_layer/utils.zip"

  layer_name          = "utils_layer"
  compatible_runtimes = [var.python_runtime]
}
