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


data "aws_iam_policy_document" "secrets_read" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "secrets_read_policy" {
  name   = "read_db_secret"
  policy = data.aws_iam_policy_document.secrets_read.json
}

resource "aws_iam_role_policy_attachment" "attach_secrets" {
  role       = aws_iam_role.s3_role.name
  policy_arn = aws_iam_policy.secrets_read_policy.arn
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

# policy for logging to cloudwatch
data "aws_iam_policy_document" "cw_document" {
  statement {
    effect  = "Allow"
    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    effect  = "Allow"
    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*:*"
    ]
  }
}