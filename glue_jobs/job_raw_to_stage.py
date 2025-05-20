import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.etl import process_landing_data, do_raw_to_stage

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

do_raw_to_stage(glueContext, spark, table, glueContext.get_logger())


job.commit()
