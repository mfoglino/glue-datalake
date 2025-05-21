from datetime import datetime
import boto3
from pyspark.sql.functions import year, month, dayofmonth

class DataHelper:
    def __init__(self, glue_context):
        self.S3_SCHEME = "s3"  # "s3a" for local execution
        self.s3_conn = boto3.session.Session().client("s3")
        self.TIMESTAMP_COLUMN = "cdc_timestamp"  # Replace with your actual timestamp column name
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.logger = glue_context.get_logger()

    def load_data_from_s3(self, bucket_name, prefix, table, timestamp_bookmark_str):
        self.logger.info(f"Loading data from {bucket_name}/{prefix}/{table}, since: {timestamp_bookmark_str}")

        is_initial_load = "INITIAL_LOAD" in timestamp_bookmark_str

        if is_initial_load:
            self.logger.info("Initial load: Reading all data in root folder of the table")
            path_to_initial_data = f"{self.S3_SCHEME}://{bucket_name}/{prefix}/{table}/LOAD*.parquet"
            self.logger.info(f"Reading data from: {path_to_initial_data}")
            initial_df = self.spark.read.parquet(path_to_initial_data)
            return initial_df
        else:
            timestamp_bookmark_date_hour = datetime.strptime(timestamp_bookmark_str, "%Y-%m-%d %H:%M:%S.%f").replace(
                minute=0, second=0, microsecond=0
            )

            files = self.list_keys_for_s3_bucket(bucket_name, f"{prefix}/{table}")
            filtered_files = self.filter_files_by_timestamp(f"{prefix}/{table}", files, timestamp_bookmark_date_hour)
            input_paths = [f"{self.S3_SCHEME}://{bucket_name}/{file_path}" for file_path in filtered_files]

            if input_paths:
                new_data_df = self.spark.read.parquet(*input_paths)
                new_data_df = new_data_df.filter(new_data_df[self.TIMESTAMP_COLUMN] > timestamp_bookmark_str)
                return new_data_df
            else:
                return None

    def filter_files_by_timestamp(self, prefix, files, start_date):
        filtered_files = []
        for file in files:
            timestamp = self.extract_timestamp(file.replace(prefix, ""))
            if timestamp and timestamp >= start_date:
                filtered_files.append(file)
        return filtered_files

    def list_keys_for_s3_bucket(self, bucket_name: str, prefix: str = None) -> list:
        kwargs = {"Bucket": bucket_name}
        if prefix:
            kwargs["Prefix"] = prefix

        keys = []
        while True:
            resp = self.s3_conn.list_objects_v2(**kwargs)
            if "Contents" not in resp:
                return keys  # empty dir
            for obj in resp["Contents"]:
                keys.append(obj["Key"])

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

        return keys

    def extract_timestamp(self, file_path):
        try:
            parts = file_path.split("/")[:-1]
            date_str = "".join(parts)
            return datetime.strptime(date_str, "%Y%m%d%H")
        except ValueError:
            return None

    def add_date_information(self, input_data):
        input_data = input_data.withColumn("year", year(self.TIMESTAMP_COLUMN))
        input_data = input_data.withColumn("month", month(self.TIMESTAMP_COLUMN))
        input_data = input_data.withColumn("day", dayofmonth(self.TIMESTAMP_COLUMN))
        return input_data