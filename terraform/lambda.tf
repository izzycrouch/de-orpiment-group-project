resource "aws_lambda_function" "extract_raw_data_function" {
    function_name = "extract-func"
    role = aws_iam_role.s3_role.arn
    
    s3_bucket = "${var.lambda_code_bucket_name}"
    s3_key = "extract.zip"

    handler = "extract.lambda_handler"
}