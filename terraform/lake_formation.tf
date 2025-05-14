locals {
  glue_etl_role = "arn:aws:iam::131578276461:role/service-role/AWSGlueServiceRole-MarcosTest"
  databases = ["raw"]
  roles = [local.glue_etl_role]
  databases_with_roles = flatten([
    for role in local.roles : [
      #for database in concat(var.databases_name, ["default"]) : {
      for database in local.databases : {
        role     = role
        database = database
      }
    ]
  ])
}

resource "aws_lakeformation_permissions" "full_access_to_db" {
  count       = length(local.databases_with_roles)
  permissions = ["ALL"]
  principal   = local.databases_with_roles[count.index].role

  database {
    name = local.databases_with_roles[count.index].database
  }
}

# resource "aws_lakeformation_permissions" "databrew_all_layers_table_access" {
#   count       = length(local.databases_with_roles)
#   permissions = ["ALL"]
#   principal   = local.databases_with_roles[count.index].role
#
#   table {
#     database_name = local.databases_with_roles[count.index].database
#     wildcard      = true
#   }
# }
