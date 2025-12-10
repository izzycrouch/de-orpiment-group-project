resource "aws_lambda_function" "extract_raw_data_function" {
    function_name = "extract-func"
    role = aws_iam_role.s3_role.arn

    s3_bucket = "${var.lambda_code_bucket_name}"
    s3_key = "extract.zip"

    handler = "extract.lambda_handler"
    runtime = "python3.12"
}

data "archive_file" "small_layers" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer_small/"
  output_path      = "${path.module}/../layer_small.zip"
}

data "archive_file" "large_layers" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer_large/"
  output_path      = "${path.module}/../layer_large.zip"
}



