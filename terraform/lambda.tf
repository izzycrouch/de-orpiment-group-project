resource "aws_lambda_function" "extract_raw_data_function" {
    function_name = "extract-func"
    role = aws_iam_role.s3_role.arn
    
    s3_bucket = "${aws_s3_bucket.lambda_code_bucket}"
    s3_key = "extract.zip"

    handler = "extract.lambda_handler"
}