import time

import boto3

from data_helper import load_data_from_s3, add_date_information


def process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark, table, timestamp_bookmark_str):
    latest_data_df = load_data_from_s3(lading_bucket_name, landing_bucket_prefix, table, timestamp_bookmark_str, spark,
                                       logger)
    latest_data_df = add_date_information(latest_data_df)
    output_path = f"s3://{raw_bucket_name}/tables/{table}/"
    # Write the DataFrame to the raw bucket in append mode with partitions


    latest_data_df.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)
    return latest_data_df


def do_raw_to_stage(glue_context, spark, table, logger):
    spark.sql("SHOW TABLES IN raw").show()
    spark.sql("SELECT current_catalog()").show()
    spark.sql("SELECT current_schema()").show()

    target_database = "stage"

    df_raw = glue_context.create_dynamic_frame.from_catalog(
        database="raw",
        table_name="people_table",
        transformation_ctx="raw_people_ctx",
        additional_options={"useGlueDataCatalog": True, "mergeSchema": True}
    ).toDF()

    # Check if table exists
    if table_exists_in_glue_catalog(glue_context, target_database, table):

        # For existing tables, use ALTER TABLE to add missing columns first
        logger.info(f"Table {table} exists. Checking for schema differences.")

        # Get existing table schema
        # Retrieve table schema using get_columns_metadata
        stage_columns_metadata = get_columns_metadata(target_database, table)
        stage_table_columns = [column["Name"] for column in stage_columns_metadata]
        data_columns = df_raw.columns

        # Find missing columns
        missing_columns = set(data_columns) - set(stage_table_columns + ["year", "month", "day"])

        print(f"Missing columns: {missing_columns}")

        # Add missing columns to the table

        for col in missing_columns:
            col_type = df_raw.schema[col].dataType.simpleString()
            logger.info(f"Adding column {col} with type {col_type} to table {target_database}.{table}")
            spark.sql(f"ALTER TABLE {target_database}.{table} ADD COLUMN {col} {col_type}")

        # Now append data with schema evolution enabled
        logger.info(f"Appending data to table {table_exists_in_glue_catalog}.{table}")
        df_raw.writeTo(f"{target_database}.{table}") \
            .tableProperty("format-version", "2") \
            .append()
    else:
        # Create table if it doesn't exist
        logger.info(f"Table {table} doesn't exist. Creating new table.")
        df_raw.writeTo(f"{target_database}.{table}") \
            .tableProperty("format-version", "2") \
            .partitionedBy("year", "month", "day") \
            .create()


    df_raw.show(truncate=False)


def get_columns_metadata(database_name, table_name):
    glue_client = boto3.client("glue", region_name="us-east-1")
    # Get the table metadata
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    # Extract column metadata
    print("Response:", response)
    columns = response["Table"]["StorageDescriptor"]["Columns"]
    # Print column metadata
    for column in columns:
        print(f"Column Name: {column['Name']}, Type: {column['Type']}, Comment: {column.get('Comment', 'N/A')}")
    return columns

def table_exists_in_glue_catalog(spark, database_name, table_name):
    try:
        # Use spark.sql to check if the table exists in the database
        result = spark.sql(f"SHOW TABLES IN {database_name}").filter(f"tableName = '{table_name}'").count()
        return result > 0
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


def run_crawler_sync(crawler_name):
    glue_client = boto3.client("glue")

    # Start the crawler
    print(f"Starting crawler: {crawler_name}")
    glue_client.start_crawler(Name=crawler_name)

    # Poll the crawler status
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response["Crawler"]["State"]
        print(f"Crawler status: {status}")

        if status != "RUNNING":
            break

        time.sleep(5)  # Wait before checking again

    print(f"Crawler {crawler_name} has completed.")
