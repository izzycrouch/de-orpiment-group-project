# should switch to s3 bucket

data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/quotes.py"
  output_path      = "${path.module}/../function.zip"
}


data "archive_file" "layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer/"
  output_path      = "${path.module}/../layer.zip"
}


resource "aws_lambda_layer_version" "requests_layer" {
  layer_name          = "requests_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.layer.output_path
}

resource "aws_lambda_function" "quote_handler" {
  #TODO: Provision the lambda
  #TODO: Connect the layer which is outlined above
  filename      = data.archive_file.lambda.output_path
  function_name = "find_random_quotes"
  role          = aws_iam_role.lambda_role.arn
  handler       = "quotes.lambda_handler"
  runtime       = var.python_runtime
  layers        = [aws_lambda_layer_version.requests_layer.arn]
  environment {
    variables = {
      RAW_DATA_BUCKET_NAME = aws_s3_bucket.raw_data_bucket.bucket
    }
  }
  timeout = 30
}

# resource "aws_lambda_permission" "allow_events" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.quote_handler.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.scheduler.arn
# }

