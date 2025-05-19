resource "aws_glue_crawler" "marcos_raw_test_crawler" {
  name         = "marcos-raw-test-crawler"
  role         = aws_iam_role.glue_etl_role.name
  database_name = "raw"

  s3_target {
    path = "s3://${aws_s3_bucket.raw_bucket.id}/tables"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  lineage_configuration {
    crawler_lineage_settings = "DISABLE"
  }

  configuration = jsonencode({
    Version                 = 1.0,
    Grouping                = {
      TableGroupingPolicy   = "CombineCompatibleSchemas",
      TableLevelConfiguration = 3
    },
    CreatePartitionIndex    = true
  })

  lake_formation_configuration {
    use_lake_formation_credentials = false
  }
}