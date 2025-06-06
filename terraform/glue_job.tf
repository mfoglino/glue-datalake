locals {
  enable_time_legacy_parser = false
  raw_job_name              = "job_landing_to_raw.py"
  stage_job_name            = "job_raw_to_stage.py"

  spark_conf = <<EOT
 conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
 --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
 --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.raw_bucket.id}/data-warehouse
 --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
 --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
 --conf spark.sql.defaultCatalog=glue_catalog
 --conf spark.sql.catalog.glue_catalog.default-namespace=stage
 --conf spark.sql.parquet.mergeSchema=true
${local.enable_time_legacy_parser ? " --conf spark.sql.legacy.timeParserPolicy=LEGACY" : ""}
EOT

}

resource "aws_s3_object" "raw_glue_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.raw_job_name}"
  source = "../glue_jobs/${local.raw_job_name}"
  etag   = filemd5("../glue_jobs/${local.raw_job_name}")
}


resource "aws_s3_object" "stage_glue_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.stage_job_name}"
  source = "../glue_jobs/${local.stage_job_name}"
  etag   = filemd5("../glue_jobs/${local.stage_job_name}")
}


resource "aws_glue_job" "raw_job" {
  name     = "marcos-landing-to-raw"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.raw_glue_job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--timestamp_bookmark_str"           = "-"
    "--landing_bucket_name"              = aws_s3_bucket.landing_bucket.id
    "--raw_bucket_name"                  = aws_s3_bucket.raw_bucket.id
    "--bucket_prefix"                    = "tables"
    "--bookmark_table"                   = aws_dynamodb_table.raw_to_stage_dynamodb_table.name
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 5
  }
}



resource "aws_glue_job" "stage_job" {
  name     = "marcos-raw-to-stage"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.stage_glue_job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--conf"                             = trim(local.spark_conf, "\n")
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 5
  }
}