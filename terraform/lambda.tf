resource "aws_lambda_function" "extract_raw_data_function" {
    function_name = var.extract_lambda_func_name
    role = aws_iam_role.s3_role.arn
    
    s3_bucket = aws_s3_bucket.lambda_code_bucket
    s3_key = "${var.extract_lambda_func_name}-code.py"
    s3_object_version = "null"

}