from etl.etl import process_landing_data, do_raw_to_stage, run_crawler_sync
from etl.etl_manager import EtlManager

from tests.test_helper import delete_tables_and_clean_data

lading_bucket_name = "marcos-test-datalake-landing"
raw_bucket_name = "marcos-test-datalake-raw"
landing_bucket_prefix = "tables"
table = "people_table"
stage_db = "stage"
raw_db = "raw"


def test_step1_landing_to_raw_initial_load(glue_context):
    timestamp_bookmark_str = "INITIAL_LOAD"
    etl_manager = EtlManager(
        glue_context,
        landing_bucket_name=lading_bucket_name,
        bucket_prefix=landing_bucket_prefix,
        raw_bucket_name=raw_bucket_name,
    )
    latest_data_df = etl_manager.process_landing_data(table, timestamp_bookmark_str)
    latest_data_df.show()


def test_step2_run_crawler():
    # This test should be run manually to trigger the crawler
    # to create the tables in the raw database
    run_crawler_sync("marcos-raw-test-crawler")


def test_step3_raw_to_stage(glue_context):
    etl_manager = EtlManager(
        glue_context,
        landing_bucket_name=lading_bucket_name,
        bucket_prefix=landing_bucket_prefix,
        raw_bucket_name=raw_bucket_name,
    )
    etl_manager.do_raw_to_stage(table)


def test_step4_landing_to_raw_incremental_load(glue_context):
    etl_manager = EtlManager(
        glue_context,
        landing_bucket_name=lading_bucket_name,
        bucket_prefix=landing_bucket_prefix,
        raw_bucket_name=raw_bucket_name,
    )
    timestamp_bookmark_str = "2023-01-02 12:03:00.001"
    latest_data_df = etl_manager.process_landing_data(table, timestamp_bookmark_str)
    latest_data_df.show()


def test_step5_run_crawler(glue_context):
    run_crawler_sync("marcos-raw-test-crawler")


def test_step6_raw_to_stage_new_data(glue_context):
    etl_manager = EtlManager(
        glue_context,
        landing_bucket_name=lading_bucket_name,
        bucket_prefix=landing_bucket_prefix,
        raw_bucket_name=raw_bucket_name,
    )
    etl_manager.do_raw_to_stage(table)


def test_describe(glue_context):
    spark = glue_context.spark_session
    logger = glue_context.get_logger()
    logger.info("Testing describe function")

    spark.sql("SHOW TABLES IN raw").show()
    spark.sql("SELECT current_catalog()").show()
    spark.sql("SELECT current_schema()").show()
    spark.sql("USE SCHEMA raw")
    spark.sql(f"""DESCRIBE spark_catalog.{raw_db}.{table}""").show()

    print("List of available catalogs:", spark.catalog.listCatalogs())
    print("Check if 'unexisting_table' exists:", spark.catalog.tableExists("unexisting_table"))
    print("Check if 'raw.people_table' exists:", spark.catalog.tableExists("raw.people_table"))
    print("Current catalog in use:", spark.catalog.currentCatalog())

    # print("List of available databases:", spark.catalog.listDatabases())
    print("List of tables in the current catalog and schema:", spark.catalog.listTables())


def test_clean_tables_and_data(glue_context):
    delete_tables_and_clean_data(glue_context, raw_bucket_name)


def test_schema_evolution_end_to_end(glue_context):
    etl_manager = EtlManager(
        glue_context,
        landing_bucket_name=lading_bucket_name,
        bucket_prefix=landing_bucket_prefix,
        raw_bucket_name=raw_bucket_name,
    )
    spark = glue_context.spark_session

    ## STEP 1
    timestamp_bookmark_str = "INITIAL_LOAD"
    logger = glue_context.get_logger()
    latest_data_df = etl_manager.process_landing_data(table, timestamp_bookmark_str)
    latest_data_df.show()

    ### STEP 2
    run_crawler_sync("marcos-raw-test-crawler")

    ### STEP 3
    etl_manager.do_raw_to_stage(table)

    ### STEP 4
    timestamp_bookmark_str = "2023-01-02 12:03:00.001"
    latest_data_df = etl_manager.process_landing_data(table, timestamp_bookmark_str)
    latest_data_df.show()

    ### STEP 5
    run_crawler_sync("marcos-raw-test-crawler")

    ### STEP 6
    etl_manager.do_raw_to_stage(table)
