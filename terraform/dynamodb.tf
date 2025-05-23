

resource "aws_dynamodb_table" "raw_to_stage_dynamodb_table" {
  name           = "landing_to_raw_bookmark"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "table_name"

  attribute {
    name = "table_name"
    type = "S"
  }
}
