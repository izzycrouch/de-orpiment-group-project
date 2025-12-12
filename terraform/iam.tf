# creat role
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "s3_role" {
  name               = "s3_role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags = {
    tag-key = "tag-value"
  }
}

# data_document
data "aws_iam_policy_document" "s3_write" {
  statement {
    actions = [
      "s3:*"
    ]
    resources = [
      "*"
    ]
  }
}


#create policy
resource "aws_iam_policy" "s3_write_policy" {
  name   = "write_s3_bucket"
  policy = data.aws_iam_policy_document.s3_write.json
}


#link policy with role
resource "aws_iam_role_policy_attachment" "attach_read" {
  role       = aws_iam_role.s3_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}



