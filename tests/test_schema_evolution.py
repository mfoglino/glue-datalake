from datetime import datetime
from pyspark.sql.functions import col


lading_bucket_name = "marcos-test-datalake-landing"
table = "people_table"


def setup_initial_table(spark):
    """Create and populate the initial table with a basic schema."""
    # Create synthetic data with the initial schema, including a cdc_timestamp
    initial_data = [
        (1, "Alice", 25, datetime(2023, 1, 1, 10, 0, 0)),
        (2, "Bob", 30, datetime(2023, 1, 1, 11, 0, 0)),
        (3, "Charlie", 35, datetime(2023, 1, 1, 12, 0, 0)),
    ]
    initial_schema = ["id", "name", "age", "cdc_timestamp"]

    # Create a DataFrame with the initial schema
    df_initial = spark.createDataFrame(initial_data, initial_schema)

    # Write the initial data to the Iceberg table
    # df_initial.writeTo(f"{database_name}.{table_name}") \
    #     .tableProperty("format-version", "2") \
    #     .create()
    return df_initial


def evolve_table_schema(spark):
    """Evolve the table schema by adding a new column."""
    # Create data with an evolved schema (added profession and cdc_timestamp)
    new_data = [
        (4, "David", 40, "Engineer", datetime(2023, 1, 2, 13, 0, 0)),
        (5, "Eve", 28, "Doctor", datetime(2023, 1, 2, 13, 0, 0)),
        (6, "Frank", 33, "Artist", datetime(2023, 1, 2, 13, 0, 0)),
    ]
    new_schema = ["id", "name", "age", "profession", "cdc_timestamp"]

    # Create a DataFrame with the new schema
    df_new = spark.createDataFrame(new_data, new_schema)

    # Return the updated table
    return df_new


def test_generate_initial_schema(glue_context):
    spark = glue_context.spark_session

    print(f"Spark version: {spark.version}")
    initial_df = setup_initial_table(spark)

    output_path = f"s3://{lading_bucket_name}/tables/{table}/LOAD00000001.parquet"
    initial_df.coalesce(1).write.mode("overwrite").parquet(output_path)

    # rename later manually to a single file named LOAD00000001.parquet


def test_evolve_schema(glue_context):
    spark = glue_context.spark_session

    # Step 2: Evolve schema and verify
    evolved_df = evolve_table_schema(spark)

    output_path = f"s3://{lading_bucket_name}/tables/{table}/2023/01/02/13"
    evolved_df.write.mode("append").parquet(output_path)
