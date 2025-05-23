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
