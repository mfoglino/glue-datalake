import boto3


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
    #print(f"Cleaning bucket: {raw_bucket_name}")
    #clean_s3_bucket(s3_client, raw_bucket_name)


def clean_s3_bucket(s3_client, bucket_name):
    prefix = "tables/"  # Specify the folder prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})





def write_single_parquet_file(df, output_path, logger):
    # Write the DataFrame to a temporary folder
    temp_output_path = output_path + "_temp"
    df.coalesce(1).write.format("parquet").mode("overwrite").save(temp_output_path)

    # Parse S3 bucket and prefix
    s3 = boto3.client("s3")
    bucket_name = output_path.split("/")[2]
    prefix = "/".join(output_path.split("/")[3:-1])
    temp_prefix = "/".join(output_path.split("/")[3:]) + "_temp/"

    desired_file_name = output_path.split("/")[-1]

    # List the files in the temporary folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_prefix)
    parquet_file_key = None

    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            parquet_file_key = obj["Key"]
            break

    if parquet_file_key:
        # Copy the file to the desired location with the correct name
        copy_source = {"Bucket": bucket_name, "Key": parquet_file_key}
        logger.info(f"Copying {copy_source} to {prefix}/{desired_file_name}")
        s3.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=f"{prefix}/{desired_file_name}",
        )

        # Delete the temporary file
        s3.delete_object(Bucket=bucket_name, Key=parquet_file_key)

    # Delete the temporary folder
    if response.get("Contents"):
        for obj in response["Contents"]:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])