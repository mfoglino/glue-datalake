locals {

}

# Create Step Function state machine
resource "aws_sfn_state_machine" "sf_datalake_orchestration" {
  name     = "marcos-datalake-orchestration"
  role_arn = aws_iam_role.glue_etl_role.arn

  definition = jsonencode({
    "Comment" : "A description of my state machine",
    "StartAt" : "Glue Landing to Raw",
    "States" : {
      "Glue Landing to Raw" : {
        "Type" : "Task",
        "Resource" : "arn:aws:states:::glue:startJobRun.sync",
        "Parameters" : {
          "JobName" : "marcos-landing-to-raw",
          "Arguments" : {
            "--timestamp_bookmark_str.$" : "$.timestamp_bookmark_str"
          }
        },
        "Next" : "StartCrawler"
      },
      "StartCrawler" : {
        "Type" : "Task",
        "Parameters" : {
          "Name" : "marcos-raw-test-crawler"
        },
        "Resource" : "arn:aws:states:::aws-sdk:glue:startCrawler",
        "Next" : "Glue Raw to Stage"
      },
      "Glue Raw to Stage" : {
        "Type" : "Task",
        "Resource" : "arn:aws:states:::glue:startJobRun.sync",
        "Parameters" : {
          "JobName" : "marcos-raw-to-stage",
          "Arguments" : {
          }
        },
        "End" : true
      }
    }
  })
}


#### Example to call the step function:
# {
# "timestamp_bookmark_str": "INITIAL_LOAD"
# }

# or

# {
# "timestamp_bookmark_str": "2023-01-02 12:03:00.001"
# }
