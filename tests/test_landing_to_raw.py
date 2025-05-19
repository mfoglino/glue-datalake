import boto3

from etl import process_landing_data, do_raw_to_stage, get_columns_metadata

lading_bucket_name = "marcos-test-datalake-landing"
raw_bucket_name = "marcos-test-datalake-raw-unique"
landing_bucket_prefix = "tables"
table = "people_table"
stage_db = "stage"
raw_db = "raw"

def test_landing_to_raw_initial_load(glue_context):

    spark = glue_context.spark_session
    timestamp_bookmark_str = "INITIAL_LOAD"
    logger = glue_context.get_logger()

    latest_data_df = process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark,
                                          table, timestamp_bookmark_str)

    latest_data_df.show()


def test_landing_to_raw_incremental_load(glue_context):

    spark = glue_context.spark_session
    timestamp_bookmark_str = "2023-01-02 12:03:00.001"
    logger = glue_context.get_logger()

    latest_data_df = process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark,
                                          table, timestamp_bookmark_str)

    latest_data_df.show()


def test_raw_to_stage(glue_context):
    spark = glue_context.spark_session
    do_raw_to_stage(glue_context, spark, table, glue_context.get_logger())


def test_describe(glue_context):
    spark = glue_context.spark_session

    spark.sql("SHOW TABLES IN raw").show()
    spark.sql("SELECT current_catalog()").show()
    spark.sql("SELECT current_schema()").show()
    spark.sql("USE SCHEMA raw")
    spark.sql(f"""DESCRIBE spark_catalog.{raw_db}.{table}""").show()

    print("List of available catalogs:", spark.catalog.listCatalogs())
    print("Check if 'unexisting_table' exists:", spark.catalog.tableExists("unexisting_table"))
    print("Check if 'raw.people_table' exists:", spark.catalog.tableExists("raw.people_table"))
    print("Current catalog in use:", spark.catalog.currentCatalog())

    #print("List of available databases:", spark.catalog.listDatabases())
    print("List of tables in the current catalog and schema:", spark.catalog.listTables())


    #columns = get_columns_metadata(database_name, table_name)

    #print(columns)

# def test_landing_to_stage_initial_load(glue_context):
#
#     spark = glue_context.spark_session
#     timestamp_bookmark_str = "INITIAL_LOAD"
#     lading_bucket_name = "marcos-test-datalake-landing"
#     landing_bucket_prefix = "tables"
#     raw_bucket_name = "marcos-test-datalake-raw-unique"
#     table = "people_table"
#     logger = glue_context.get_logger()
#
#     latest_data_df = process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark,
#                                           table, timestamp_bookmark_str)
#     # need to run the crawler first
#
#     do_raw_to_stage(glue_context, spark, table)
#
#     latest_data_df.show()

def test_clean_tables_and_data(glue_context):

    delete_tables_and_clean_data(glue_context, raw_bucket_name)



def delete_tables_and_clean_data(glue_context, raw_bucket_name):
    spark = glue_context.spark_session
    s3_client = boto3.client("s3")

    # Delete tables in raw and stage databases
    for database in ["raw", "stage"]:
        tables = spark.sql(f"SHOW TABLES IN {database}").collect()
        for table in tables:
            table_name = table["tableName"]
            print(f"Dropping table: {database}.{table_name}")
            spark.sql(f"DROP TABLE {database}.{table_name}")

    # Clean raw bucket
    print(f"Cleaning bucket: {raw_bucket_name}")
    clean_s3_bucket(s3_client, raw_bucket_name)



def clean_s3_bucket(s3_client, bucket_name):
    prefix = "tables/"  # Specify the folder prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})