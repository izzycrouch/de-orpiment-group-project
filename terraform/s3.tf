resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.raw_data_bucket_name
  tags = {
    Name = "raw_bucket"
  }
}

resource "aws_s3_bucket" "date_bucket" {
  bucket = var.dates_bucket_name
  tags = {
    Name = "dates_bucket"
  }
}
