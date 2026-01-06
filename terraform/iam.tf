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

data "aws_iam_policy_document" "cw_document" {
  statement {
    effect  = "Allow"
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    effect  = "Allow"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*:*"
    ]
  }
}

data "aws_iam_policy_document" "secrets_read" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

#create policy

resource "aws_iam_policy" "secrets_read_policy" {
  name   = "read_db_secret"
  policy = data.aws_iam_policy_document.secrets_read.json
}

resource "aws_iam_policy" "s3_write_policy" {
  name   = "write_s3_bucket"
  policy = data.aws_iam_policy_document.s3_write.json
}

resource "aws_iam_policy" "cloud_watch_policy" {
  name   = "log_cloud_watch"
  policy = data.aws_iam_policy_document.cw_document.json
}



# make  extract role
resource "aws_iam_role" "extract_role" {
  name               = "extract_role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags = {
    tag-key = "tag-value"
  }
}

#attatch extract role
resource "aws_iam_role_policy_attachment" "attach_secrets_to_extract" {
  role       = aws_iam_role.extract_role.name
  policy_arn = aws_iam_policy.secrets_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_read_to_extract" {
  role       = aws_iam_role.extract_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_cloud_watch_to_extract" {
  role       = aws_iam_role.extract_role.name
  policy_arn = aws_iam_policy.cloud_watch_policy.arn
}

# make transform role
resource "aws_iam_role" "transform_role" {
  name               = "transform_role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags = {
    tag-key = "tag-value"
  }
}


#attatch transform role

resource "aws_iam_role_policy_attachment" "attach_read_to_transform" {
  role       = aws_iam_role.transform_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_cloud_watch_to_transform" {
  role       = aws_iam_role.transform_role.name
  policy_arn = aws_iam_policy.cloud_watch_policy.arn
}


