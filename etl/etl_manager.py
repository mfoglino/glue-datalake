import time

import boto3

from etl.data_helper import DataHelper


class EtlManager:
    def __init__(self, glue_context, landing_bucket_name, bucket_prefix, raw_bucket_name):
        self.glue_client = boto3.client("glue", region_name="us-east-1")
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.logger = glue_context.get_logger()
        self.landing_bucket_name = landing_bucket_name
        self.bucket_prefix = bucket_prefix
        self.raw_bucket_name = raw_bucket_name
        self.data_helper = DataHelper(glue_context)

    def process_landing_data(self, table, timestamp_bookmark_str):
        latest_data_df = self.data_helper.load_data_from_s3(
            self.landing_bucket_name, self.bucket_prefix, table, timestamp_bookmark_str
        )
        latest_data_df = self.data_helper.add_date_information(latest_data_df)
        output_path = f"s3://{self.raw_bucket_name}/{self.bucket_prefix}/{table}/"
        self.logger.info(f"Writing data to {output_path}")
        latest_data_df.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)
        return latest_data_df

    def do_raw_to_stage(self, table):
        self.spark.sql("SHOW TABLES IN raw").show()
        self.spark.sql("SELECT current_catalog()").show()
        self.spark.sql("SELECT current_schema()").show()

        target_database = "stage"

        df_raw = self.glue_context.create_dynamic_frame.from_catalog(
            database="raw",
            table_name="people_table",
            transformation_ctx="raw_people_ctx",
            additional_options={"useGlueDataCatalog": True, "mergeSchema": True},
        ).toDF()

        if self.table_exists_in_glue_catalog(target_database, table):
            self.logger.info(f"Table {table} exists. Checking for schema differences.")
            stage_columns_metadata = self.get_columns_metadata(target_database, table)
            stage_table_columns = [column["Name"] for column in stage_columns_metadata]
            data_columns = df_raw.columns
            missing_columns = set(data_columns) - set(stage_table_columns + ["year", "month", "day"])
            self.logger.info(f"Missing columns: {missing_columns}")

            for col in missing_columns:
                col_type = df_raw.schema[col].dataType.simpleString()
                self.logger.info(f"Adding column {col} with type {col_type} to table {target_database}.{table}")
                self.spark.sql(f"ALTER TABLE {target_database}.{table} ADD COLUMN {col} {col_type}")

            self.logger.info(f"Appending data to table {target_database}.{table}")
            df_raw.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").append()
        else:
            self.logger.info(f"Table {table} doesn't exist. Creating new table.")
            df_raw.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").partitionedBy(
                "year", "month", "day"
            ).create()

        df_raw.show(truncate=False)

    def get_columns_metadata(self, database_name, table_name):
        response = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
        self.logger.info(f"Columns metadata for {database_name}.{table_name}:", response)
        columns = response["Table"]["StorageDescriptor"]["Columns"]
        return columns

    def table_exists_in_glue_catalog(self, database_name, table_name):
        try:
            result = self.spark.sql(f"SHOW TABLES IN {database_name}").filter(f"tableName = '{table_name}'").count()
            return result > 0
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False

    def run_crawler_sync(self, crawler_name):
        self.logger.info(f"Starting crawler: {crawler_name}")
        self.glue_client.start_crawler(Name=crawler_name)

        while True:
            response = self.glue_client.get_crawler(Name=crawler_name)
            status = response["Crawler"]["State"]
            print(f"Crawler status: {status}")

            if status != "RUNNING":
                break

            time.sleep(5)

        print(f"Crawler {crawler_name} has completed.")
