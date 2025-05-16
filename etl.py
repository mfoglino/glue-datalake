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

    #df_raw = spark.read.table(f"raw.{table}")
    #spark.sql(f"SELECT * FROM raw.{table} LIMIT 5").show(truncate=False)
    #df_raw.show()

    df_raw = glue_context.create_dynamic_frame.from_catalog(
        database="raw",
        table_name="people_table",
        transformation_ctx="raw_people_ctx",
        additional_options={"useGlueDataCatalog": True, "mergeSchema": True}
    ).toDF()

    # Check if table exists
    table_exists = table_exists_in_glue_context(glue_context, target_database, table)

    if table_exists:
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
        logger.info(f"Appending data to table {table_exists_in_glue_context}.{table}")
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

def table_exists_in_glue_context(glue_context, database_name, table_name):
    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="check_table_exists"
        )
        return dynamic_frame.count() > 0
    except Exception as e:
        if "EntityNotFoundException" in str(e):
            return False
        else:
            raise