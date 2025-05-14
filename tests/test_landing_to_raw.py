from etl import process_landing_data


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


