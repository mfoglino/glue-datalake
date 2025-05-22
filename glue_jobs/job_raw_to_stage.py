import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.etl_manager import EtlManager

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
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
landing_bucket_name = "marcos-test-datalake-landing"
landing_bucket_prefix = "tables"
raw_bucket_name = "marcos-test-datalake-raw"

etl_manager = EtlManager(
    glueContext,
    landing_bucket_name=landing_bucket_name,
    bucket_prefix=landing_bucket_prefix,
    raw_bucket_name=raw_bucket_name,
)
etl_manager.do_raw_to_stage(table)

job.commit()
