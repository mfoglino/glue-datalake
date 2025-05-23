import time
import boto3

from etl.bookmark_manager import BookmarkManager
from etl.data_helper import DataHelper

TIMESTAMP_COLUMN = "cdc_timestamp"  # Replace with your actual timestamp column name

class EtlManager:
    def __init__(self, bookmark_manager, glue_context, landing_bucket_name, bucket_prefix, raw_bucket_name):
        self.bookmark_manager: BookmarkManager = bookmark_manager
        self.glue_client = boto3.client("glue", region_name="us-east-1")
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.logger = glue_context.get_logger()
        self.landing_bucket_name = landing_bucket_name
        self.bucket_prefix = bucket_prefix
        self.raw_bucket_name = raw_bucket_name
        self.data_helper = DataHelper(glue_context)

    def process_landing_data(self, table, timestamp_bookmark_str):

        timestamp_bookmark_str = self.bookmark_manager.read_bookmark_with_fallback(
            timestamp_bookmark_str, table
        )

        latest_data_df = self.data_helper.load_data_from_s3(
            self.landing_bucket_name, self.bucket_prefix, table, timestamp_bookmark_str
        )
        latest_data_df = self.data_helper.add_date_information(latest_data_df)
        output_path = f"s3://{self.raw_bucket_name}/{self.bucket_prefix}/{table}/"
        self.logger.info(f"Writing data to {output_path}")
        latest_data_df.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)

        last_processed_timestamp = latest_data_df.selectExpr(f"max({TIMESTAMP_COLUMN})").collect()[0][0]
        if last_processed_timestamp is None:
            self.logger.warn(f"No new data to process (empty dataframe). Last processed timestamp remains as: {timestamp_bookmark_str}")
        else:
            self.logger.info(f"Last processed timestamp: {last_processed_timestamp}")
            self.bookmark_manager.write_bookmark(table, last_processed_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

        return latest_data_df

    def do_raw_to_stage(self, table):
        self.spark.sql("SHOW TABLES IN raw").show()
        self.spark.sql("SELECT current_catalog()").show()
        self.spark.sql("SELECT current_schema()").show()

        source_database = "raw"
        target_database = "stage"
        self.logger.info(f"Processing table {source_database}.{table}")
        df_raw = self.glue_context.create_dynamic_frame.from_catalog(
            database=source_database,
            table_name=table,
            transformation_ctx="raw_people_ctx",
            additional_options={"useGlueDataCatalog": True, "mergeSchema": True},
        ).toDF()

        if self.table_exists_in_glue_catalog(target_database, table):
            self.logger.info(f"Table {table} exists. Checking for schema differences.")
            self.evolve_table_schema_iceberg(df_raw, table, target_database)
            self.logger.info(f"Appending data to table {target_database}.{table}")
            df_raw.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").append()
        else:
            self.logger.info(f"Table {table} doesn't exist. Creating new table.")
            df_raw.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").partitionedBy(
                "year", "month", "day"
            ).create()

        df_raw.show(truncate=False)

    def evolve_table_schema_iceberg(self, df_raw, table, target_database):
        current_metadata = self.get_columns_metadata(target_database, table)
        current_columns = {column["Name"]: column["Type"] for column in current_metadata}  # Convert to a dictionary
        new_columns = {col: df_raw.schema[col].dataType.simpleString() for col in df_raw.columns}  # New columns with types

        to_add = set(new_columns.keys()) - set(current_columns.keys())
        to_drop = [col for col in current_columns.keys() if col not in new_columns.keys()]
        to_type_change = {
            col: (current_columns[col], new_columns[col])
            for col in new_columns.keys()
            if col in current_columns and current_columns[col] != new_columns[col]
        }

        if to_drop:
            raise Exception(f"Cannot drop columns automatically: {to_drop}")
        if to_type_change:
            raise Exception(f"Type changes detected (manual intervention needed): {to_type_change}")

        if to_add:
            self.logger.info(f"Adding new columns: {to_add}")
            for col in to_add:
                col_type = new_columns[col]
                self.logger.info(f"Adding column {col} with type {col_type} to table {target_database}.{table}")
                self.spark.sql(f"ALTER TABLE {target_database}.{table} ADD COLUMN {col} {col_type}")

    def get_columns_metadata(self, database_name, table_name):
        response = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
        #self.logger.info(f"Columns metadata for {database_name}.{table_name}: {response}")
        columns = response["Table"]["StorageDescriptor"]["Columns"]
        self.logger.info(f"Columns metadata for {database_name}.{table_name}: {columns}")
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
