from data_helper import load_data_from_s3, add_date_information


def process_landing_data(lading_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark, table, timestamp_bookmark_str):
    latest_data_df = load_data_from_s3(lading_bucket_name, landing_bucket_prefix, table, timestamp_bookmark_str, spark,
                                       logger)
    latest_data_df = add_date_information(latest_data_df)
    output_path = f"s3://{raw_bucket_name}/tables/{table}/"
    # Write the DataFrame to the raw bucket in append mode with partitions
    latest_data_df.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)
    return latest_data_df
