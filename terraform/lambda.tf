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
  s3_key    = "extract_lambda.zip"

  # filename = data.archive_file.lambda.output_path

  handler = "extract_lambda.lambda_handler"
  runtime = var.python_runtime



  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:20",
    # aws_lambda_layer_version.libraries_layer.arn,
    # aws_lambda_layer_version.pyarrow_libraries_layer.arn,
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

  handler = "zip_lambda.lambda_handler"
  runtime = var.python_runtime

  # layers = [aws_lambda_layer_version.large_libraries_layer.arn, aws_lambda_layer_version.small_libraries_layer.arn]

  timeout = 60
}

resource "aws_lambda_layer_version" "libraries_layer" {
  s3_bucket = aws_s3_bucket.libraries_layer_bucket.bucket
  s3_key    = "libraries.zip"

  layer_name = "libraries_layer"

  compatible_runtimes = ["python3.13"]
}


resource "aws_lambda_layer_version" "pyarrow_libraries_layer" {
  s3_bucket = aws_s3_bucket.libraries_layer_bucket.bucket
  s3_key    = "pyarrow_libraries.zip"

  layer_name = "pyarrow_libraries_layer"

  compatible_runtimes = [var.python_runtime]
}



resource "aws_lambda_layer_version" "utils_layer" {
  s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key    = "utils.zip"

  layer_name          = "utils_layer"
  compatible_runtimes = [var.python_runtime]
}
