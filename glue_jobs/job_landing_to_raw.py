import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.bookmark_manager import BookmarkManager
from etl.etl_manager import EtlManager

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

bookmark_table = "landing_to_raw_bookmark"
database_name = "raw"
table = "people_table"
timestamp_bookmark_str = args["timestamp_bookmark_str"]
landing_bucket_name = "marcos-test-datalake-landing"
raw_bucket_name = "marcos-test-datalake-raw"
bucket_prefix = "tables"

bookmark_manager = BookmarkManager(bookmark_table)

etl_manager = EtlManager(
    bookmark_manager, glueContext, landing_bucket_name=landing_bucket_name, bucket_prefix=bucket_prefix, raw_bucket_name=raw_bucket_name
)
latest_data_df = etl_manager.process_landing_data(table, timestamp_bookmark_str)
latest_data_df.show()


job.commit()
