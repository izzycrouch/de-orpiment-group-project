# # trigger for extract raw data lambda
# resource "aws_cloudwatch_event_rule" "scheduler" {
#   name                = "trigger-aws-lambda"
#   description         = "Trigger Lambda Function Every 5 Mins"
#   schedule_expression = "rate(5 minutes)"
# }

# resource "aws_cloudwatch_event_target" "lambda" {
#   rule      = aws_cloudwatch_event_rule.scheduler.name
#   target_id = "trigger-extract-raw-data-function"
#   arn       = aws_lambda_function.extract_raw_data_function.arn
# }

# resource "aws_lambda_permission" "allow_event_bridge_extract" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.extract_raw_data_function.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.scheduler.arn
# }

# resource "aws_s3_bucket_notification" "raw_data_bucket_notification" {
#   bucket      = "totesys-raw-data-aci"
#   eventbridge = true
# }


# # trigger for transform data lambda
# resource "aws_cloudwatch_event_rule" "raw_data_uploaded" {
#     name        = "trigger-transform-lambda"
#     description = "Trigger Lambda Function When Raw Data Ingested Into S3"

#     event_pattern = jsonencode({
#       source = ["aws.s3"],
#       detail-type = ["Object Created"],
#       detail= {
#         bucket = {
#           name = ["totesys-raw-data-aci"]
#         }
#       }
#     })
# }

# resource "aws_cloudwatch_event_target" "transform_lambda" {
#   rule      = aws_cloudwatch_event_rule.raw_data_uploaded.name
#   target_id = "trigger-transform-data-function"
#   arn       = aws_lambda_function.transform_data_function.arn
# }

# resource "aws_lambda_permission" "allow_event_bridge_transform" {
#   statement_id  = "AllowEventBridgeTransform"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.transform_data_function.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.raw_data_uploaded.arn
# }
