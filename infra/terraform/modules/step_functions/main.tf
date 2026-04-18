data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ------------------------------------------------------------------------------
# IAM Role for Step Functions
# ------------------------------------------------------------------------------
resource "aws_iam_role" "sfn" {
  name = "${var.state_machine_name}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "sfn_execution" {
  name = "${var.state_machine_name}-sfn-policy"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeLambda"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction",
        ]
        Resource = var.lambda_arns
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = "*"
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = [
          "sns:Publish",
        ]
        Resource = var.dlq_sns_topic_arn != "" ? [var.dlq_sns_topic_arn] : []
      },
      {
        Sid    = "XRayAccess"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets",
        ]
        Resource = "*"
      },
    ]
  })
}

# ------------------------------------------------------------------------------
# CloudWatch Log Group for Step Functions
# ------------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/stepfunctions/${var.state_machine_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# ------------------------------------------------------------------------------
# State Machine — Discover -> Translate -> Deploy -> Verify
# ------------------------------------------------------------------------------
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = var.state_machine_name
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "ETL Agent pipeline: Discover -> Translate -> Deploy -> Verify"
    StartAt = "Discover"
    States = {
      Discover = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.discover_lambda_arn
          "Payload.$"  = "$"
        }
        ResultPath = "$.discover_result"
        ResultSelector = {
          "payload.$" = "$.Payload"
        }
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "States.TaskFailed"]
            IntervalSeconds = 5
            MaxAttempts     = 3
            BackoffRate     = 2.0
          },
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
            ResultPath  = "$.error_info"
          },
        ]
        Next = "Translate"
      }
      Translate = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.translate_lambda_arn
          "Payload.$"  = "$"
        }
        ResultPath = "$.translate_result"
        ResultSelector = {
          "payload.$" = "$.Payload"
        }
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "States.TaskFailed"]
            IntervalSeconds = 5
            MaxAttempts     = 3
            BackoffRate     = 2.0
          },
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
            ResultPath  = "$.error_info"
          },
        ]
        Next = "Deploy"
      }
      Deploy = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.deploy_lambda_arn
          "Payload.$"  = "$"
        }
        ResultPath = "$.deploy_result"
        ResultSelector = {
          "payload.$" = "$.Payload"
        }
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "States.TaskFailed"]
            IntervalSeconds = 10
            MaxAttempts     = 2
            BackoffRate     = 2.0
          },
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
            ResultPath  = "$.error_info"
          },
        ]
        Next = "Verify"
      }
      Verify = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.verify_lambda_arn
          "Payload.$"  = "$"
        }
        ResultPath = "$.verify_result"
        ResultSelector = {
          "payload.$" = "$.Payload"
        }
        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "States.TaskFailed"]
            IntervalSeconds = 5
            MaxAttempts     = 3
            BackoffRate     = 2.0
          },
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
            ResultPath  = "$.error_info"
          },
        ]
        Next = "PipelineComplete"
      }
      HandleError = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = var.dlq_sns_topic_arn
          Subject     = "ETL Agent Pipeline Failure"
          "Message.$" = "States.Format('Pipeline failed. Error: {}', States.JsonToString($.error_info))"
        }
        Next = "PipelineFailed"
      }
      PipelineComplete = {
        Type = "Succeed"
      }
      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineError"
        Cause = "ETL pipeline encountered an unrecoverable error."
      }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = var.log_level
  }

  tracing_configuration {
    enabled = var.enable_xray_tracing
  }

  tags = var.tags
}
