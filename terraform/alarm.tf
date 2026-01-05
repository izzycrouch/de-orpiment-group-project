resource "aws_cloudwatch_log_group" "extract_lambda_log" {
  name = "/aws/lambda/${aws_lambda_function.extract_raw_data_function.function_name}"
}

resource "aws_cloudwatch_log_group" "transform_lambda_log" {
  name = "/aws/lambda/${aws_lambda_function.transform_data_function.function_name}"
}

resource "aws_cloudwatch_log_metric_filter" "log_extract_major_errors" {
  name           = "extract-major-errors"
  pattern        = "MAJOR_ERROR"
  log_group_name = aws_cloudwatch_log_group.extract_lambda_log.name

  metric_transformation {
    name      = "extract-major-errors"
    namespace = "lambda errors"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "log_transform_major_errors" {
  name           = "transform-major-errors"
  pattern        = "MAJOR_ERROR"
  log_group_name = aws_cloudwatch_log_group.transform_lambda_log.name

  metric_transformation {
    name      = "transform-major-errors"
    namespace = "lambda errors"
    value     = "1"
  }
}

resource "aws_sns_topic" "major_error" {
  name = "major-error"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.major_error.arn
  protocol  = "email"
  endpoint  = "dataengineering@northcoders.com"
}

resource "aws_cloudwatch_metric_alarm" "extract_errors_alarm" {
  alarm_name                = "major-error-in-extract-lambda"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = aws_cloudwatch_log_metric_filter.log_extract_major_errors.name
  namespace                 = aws_cloudwatch_log_metric_filter.log_extract_major_errors.metric_transformation[0].namespace
  period                    = 300
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "alert when major error occurs"
  alarm_actions = [aws_sns_topic.major_error.arn]
}

resource "aws_cloudwatch_metric_alarm" "transform_errors_alarm" {
  alarm_name                = "major-error-in-transform-lambda"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = aws_cloudwatch_log_metric_filter.log_transform_major_errors.name
  namespace                 = aws_cloudwatch_log_metric_filter.log_transform_major_errors.metric_transformation[0].namespace
  period                    = 300
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "alert when major error occurs"
  alarm_actions = [aws_sns_topic.major_error.arn]
}