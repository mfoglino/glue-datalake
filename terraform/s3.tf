

# create an s3 bucket
resource "aws_s3_bucket" "landing_bucket" {
  bucket = "marcos-test-datalake-landing"
}

resource "aws_s3_bucket" "raw_bucket" {
  bucket = "marcos-test-datalake-raw-unique"
}

# glue scripts bucket
resource "aws_s3_bucket" "glue_scripts_bucket" {
  bucket = "marcos-test-datalake-glue-scripts"
}