resource "aws_cloudwatch_event_rule" "scheduler" {
    name        = "trigger-aws-lambda"
    description = "Trigger Lambda Function Every 5 Mins"
    schedule_expression = "rate(5 minutes)"
  }

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "trigger-extract-raw-data-function"
  arn       = aws_lambda_function.extract_raw_data_function.arn
}

resource "aws_lambda_permission" "allow_event_bridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_raw_data_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler.arn
}