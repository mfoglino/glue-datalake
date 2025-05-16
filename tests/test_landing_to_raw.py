from etl import process_landing_data, do_raw_to_stage, get_columns_metadata


def test_landing_to_raw_initial_load(glue_context):

    spark = glue_context.spark_session
    timestamp_bookmark_str = "INITIAL_LOAD"
    lading_bucket_name = "marcos-test-datalake-landing"
    landing_bucket_prefix = "tables"
    raw_bucket_name = "marcos-test-datalake-raw-unique"
    table = "people_table"
    logger = glue_context.get_logger()

    latest_data_df = process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark,
                                          table, timestamp_bookmark_str)

    latest_data_df.show()


def test_landing_to_raw_incremental_load(glue_context):

    spark = glue_context.spark_session
    timestamp_bookmark_str = "2023-01-02 12:03:00.001"
    lading_bucket_name = "marcos-test-datalake-landing"
    landing_bucket_prefix = "tables"
    raw_bucket_name = "marcos-test-datalake-raw-unique"
    table = "people_table"
    logger = glue_context.get_logger()

    latest_data_df = process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark,
                                          table, timestamp_bookmark_str)

    latest_data_df.show()


def test_raw_to_stage(glue_context):
    spark = glue_context.spark_session
    table = "people_table"

    do_raw_to_stage(glue_context, spark, table, glue_context.get_logger())


def test_describe(glue_context):
    spark = glue_context.spark_session
    database_name = "raw"
    table_name = "people_table"


    spark.sql("SHOW TABLES IN raw").show()
    spark.sql("SELECT current_catalog()").show()
    spark.sql("SELECT current_schema()").show()
    spark.sql("USE SCHEMA raw")
    table_schema = spark.sql(f"DESCRIBE raw.{table_name}").collect()
    #table_schema = spark.table(f"raw.{table}").schema
    print(table_schema)

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
