resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "my-state-machine"
  role_arn = aws_iam_role.iam_for_sfn.arn

  definition = <<EOF
        {
          "Comment": "A description of my state machine",
          "StartAt": "invoke extract lambda",
          "States": {
            "invoke extract lambda": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Output": "{% $states.result.Payload %}",
              "Arguments": {
                "FunctionName": "arn:aws:lambda:eu-west-2:813185901868:function:extract-func:$LATEST",
                "Payload": "{% $states.input %}"
              },
              "Retry": [
                {
                  "ErrorEquals": [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException"
                  ],
                  "IntervalSeconds": 1,
                  "MaxAttempts": 3,
                  "BackoffRate": 2,
                  "JitterStrategy": "FULL"
                }
              ],
              "Next": "Invoke transform lambda"
            },
            "Invoke transform lambda": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Output": "{% $states.result.Payload %}",
              "Arguments": {
                "FunctionName": "arn:aws:lambda:eu-west-2:813185901868:function:transform-func:$LATEST",
                "Payload": "{% $states.input %}"
              },
              "Retry": [
                {
                  "ErrorEquals": [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException"
                  ],
                  "IntervalSeconds": 1,
                  "MaxAttempts": 3,
                  "BackoffRate": 2,
                  "JitterStrategy": "FULL"
                }
              ],
              "Next": "Lambda Invoke"
            },
            "Lambda Invoke": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Output": "{% $states.result.Payload %}",
              "Arguments": {
                "FunctionName": "arn:aws:lambda:eu-west-2:813185901868:function:load-func:$LATEST",
                "Payload": "{% $states.input %}"
              },
              "Retry": [
                {
                  "ErrorEquals": [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException"
                  ],
                  "IntervalSeconds": 1,
                  "MaxAttempts": 3,
                  "BackoffRate": 2,
                  "JitterStrategy": "FULL"
                }
              ],
              "End": true
            }
          },
          "QueryLanguage": "JSONata"
        }
EOF
}
