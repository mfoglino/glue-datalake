locals {

}

# Create the IAM role for EventBridge Scheduler
resource "aws_iam_role" "scheduler_role" {
  name = "eventbridge-scheduler-sf-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      }
    ]
  })
}

# Create the IAM policy to allow EventBridge Scheduler to invoke Step Function
resource "aws_iam_role_policy" "scheduler_policy" {
  name = "eventbridge-scheduler-sf-policy"
  role = aws_iam_role.scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.sf_datalake_orchestration.arn
        ]
      }
    ]
  })
}

# Create the EventBridge Scheduler
resource "aws_scheduler_schedule" "hourly_sf_trigger" {
  name = "sf-dl-orchestrator-hourly"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(1 hour)"

  state = "DISABLED"

  target {
    arn      = aws_sfn_state_machine.sf_datalake_orchestration.arn
    role_arn = aws_iam_role.scheduler_role.arn

    input = jsonencode({
      "timestamp_bookmark_str": "None"
      }
    )
  }
}

