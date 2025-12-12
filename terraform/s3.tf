resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.raw_data_bucket_name
  tags = {
    Name = "raw_bucket"
  }
}

resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket = var.lambda_code_bucket_name
  tags = {
    Name = "lambda_code_bucket_aci"
  }
}

resource "aws_s3_bucket" "clean_data_bucket" {
  bucket = var.clean_data_bucket_name
  tags = {
    Name = "clean_data_bucket"
  }
}

resource "aws_s3_bucket" "libraries_layer_bucket" {
  bucket = var.libraries_layer_bucket_name
}



resource "aws_s3_object" "connection_object" {
  bucket = var.lambda_code_bucket_name
  key    = "utils/connection.py"
  source = "../extract_layer/utils/connection.py"

  etag = filemd5("../extract_layer/utils/connection.py")
}

resource "aws_s3_object" "db_credentials_object" {
  bucket = var.lambda_code_bucket_name
  key    = "utils/db_credentals.py"
  source = "../extract_layer/utils/db_credentials.py"

  etag = filemd5("../extract_layer/utils/db_credentials.py")
}

resource "aws_s3_object" "extraction_info_object" {
  bucket = var.lambda_code_bucket_name
  key    = "utils/extraction_info.py"
  source = "../extract_layer/utils/extraction_info.py"

  etag = filemd5("../extract_layer/utils/extraction_info.py")
}

resource "aws_s3_object" "save_data_object" {
  bucket = var.lambda_code_bucket_name
  key    = "utils/save_data.py"
  source = "../extract_layer/utils/save_data.py"

  etag = filemd5("../extract_layer/utils/save_data.py")
}

resource "aws_s3_object" "extract_lambda_object" {
  bucket = var.lambda_code_bucket_name
  key    = "extract_lambda.py"
  source = "../extract_layer/extract_lambda.py"

  etag = filemd5("../extract_layer/extract_lambda.py")
}


