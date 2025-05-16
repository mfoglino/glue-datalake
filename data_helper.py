from datetime import datetime

import boto3
from pyspark.sql.functions import year, month, dayofmonth

S3_SCHEME = "s3"  # "s3a" for local execution
s3_conn = boto3.session.Session().client("s3")
TIMESTAMP_COLUMN = "cdc_timestamp"  # Replace with your actual timestamp column name

def load_data_from_s3(bucket_name, prefix, table, timestamp_bookmark_str, spark, logger):
    logger.info(f"Loading data from {bucket_name}/{prefix}/{table}, since: {timestamp_bookmark_str}")

    is_initial_load = "INITIAL_LOAD" in timestamp_bookmark_str

    if is_initial_load:
        logger.info("Initial load: Reading all data in root folder of the table")
        #path_to_initial_data = f"{S3_SCHEME}://{bucket_name}/{prefix}/{table}"
        path_to_initial_data = f"{S3_SCHEME}://{bucket_name}/{prefix}/{table}/LOAD*.parquet"

        # initial_df = spark.read.option("recursiveFileLookup", "true").parquet(path_to_initial_data)
        initial_df = spark.read.parquet(path_to_initial_data)
        return initial_df
    else:
        timestamp_bookmark_date_hour = datetime.strptime(timestamp_bookmark_str, "%Y-%m-%d %H:%M:%S.%f").replace(
            minute=0, second=0, microsecond=0
        )

        # Read data from the correspondant partitions according to the timestamp bookmark
        files = list_keys_for_s3_bucket(s3_conn, bucket_name, f"{prefix}/{table}")
        filtered_files = filter_files_by_timestamp(f"{prefix}/{table}", files, timestamp_bookmark_date_hour)
        input_paths = [f"{S3_SCHEME}://{bucket_name}/{file_path}" for file_path in filtered_files]

        if input_paths:
            new_data_df = spark.read.parquet(*input_paths)
            # Doing Granular filtering by bookmark date
            new_data_df = new_data_df.filter(new_data_df[TIMESTAMP_COLUMN] > timestamp_bookmark_str)
            return new_data_df
        else:
            return None


def filter_files_by_timestamp(prefix, files, start_date):
    filtered_files = []
    for file in files:
        timestamp = extract_timestamp(file.replace(prefix, ""))
        if timestamp and timestamp >= start_date:
            filtered_files.append(file)
    return filtered_files

def list_keys_for_s3_bucket(s3_conn, bucket_name: str, prefix: str = None) -> list:
    """
    List all keys in an S3 bucket.
    """
    kwargs = {"Bucket": bucket_name}
    if prefix:
        kwargs["Prefix"] = prefix

    keys = []
    while True:
        resp = s3_conn.list_objects_v2(**kwargs)
        if "Contents" not in resp:
            return keys  # empty dir
        for obj in resp["Contents"]:
            keys.append(obj["Key"])

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

    return keys

def extract_timestamp(file_path):
    try:
        # Extract the date and hour part from the file path
        parts = file_path.split("/")[:-1]
        date_str = "".join(parts)  # Combine year, month, day, and hour
        return datetime.strptime(date_str, "%Y%m%d%H")
    except ValueError:
        return None

def add_date_information(input_data):
    input_data = input_data.withColumn("year", year(TIMESTAMP_COLUMN))
    input_data = input_data.withColumn("month", month(TIMESTAMP_COLUMN))
    input_data = input_data.withColumn("day", dayofmonth(TIMESTAMP_COLUMN))

    return input_data