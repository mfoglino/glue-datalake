




# give glue role permissions over marcos-test-datalake-glue-scripts
resource "aws_iam_policy" "glue_scripts_access" {
  name        = "GlueScriptsAccessPolicy"
  description = "Policy to allow Glue role access to marcos-test-datalake-glue-scripts bucket"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.glue_scripts_bucket.id}*",
          "arn:aws:s3:::${aws_s3_bucket.raw_bucket.id}*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_policy_attachment" {
  role       = local.glue_etl_role
  policy_arn = aws_iam_policy.glue_scripts_access.arn
}


resource "aws_iam_policy" "glue_etl_access" {
  name        = "GlueETLAccessPolicy"
  description = "Policy to allow Glue role access to Glue Data Catalog and S3 bucket"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "glue:GetTable",
          "glue:GetTables",
          "iam:PassRole"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_etl_role_policy_attachment" {
  role       = local.glue_etl_role
  policy_arn = aws_iam_policy.glue_etl_access.arn
}


