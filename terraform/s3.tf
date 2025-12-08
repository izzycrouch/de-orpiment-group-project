resource "aws_s3_bucket" "raw_data_bucket" {
  bucket_prefix = var.raw_data_bucket_name
  tags = {
    Name = "raw_bucket"
  }
}

resource "aws_s3_bucket" "raw_data_bucket" {
  bucket_prefix = var.dates_bucket_name
  tags = {
    Name = "dates_bucket"
  }
}
