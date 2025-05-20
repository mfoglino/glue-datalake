import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.etl import process_landing_data

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "timestamp_bookmark_str",
    ],
)


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()


database_name = "raw"
table = "people_table"
timestamp_bookmark_str = args["timestamp_bookmark_str"]
landing_bucket_name = "marcos-test-datalake-landing"
landing_bucket_prefix = "tables"
raw_bucket_name = "marcos-test-datalake-raw"

# logger.info(f"Showing metadata...")
# spark.sql("SHOW TABLES IN raw").show()
# spark.sql("SELECT current_catalog()").show()
# spark.sql("SELECT current_schema()").show()
# spark.sql("USE SCHEMA raw")
# logger.info(f"Spark catalog imp {spark.conf.get('spark.sql.catalogImplementation')}")
#
# spark.sql(f"DESCRIBE stage.{table_name}").show()
#table_schema = spark.table(f"raw.{table}").schema
#print(table_schema)

#timestamp_bookmark_str = "INITIAL_LOAD"


latest_data_df = process_landing_data(landing_bucket_name, landing_bucket_prefix, logger, raw_bucket_name, spark,
                                      table, timestamp_bookmark_str)

latest_data_df.show()


job.commit()
