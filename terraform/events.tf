resource "aws_cloudwatch_event_rule" "scheduler" {
  name                = "trigger-state-machine"
  description         = "Trigger State Machine Every 15 Mins"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "state_machine" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "trigger-state-machine"
  arn       = aws_sfn_state_machine.sfn_state_machine.arn
  role_arn  = aws_iam_role.iam_for_eventbridge.arn
}
