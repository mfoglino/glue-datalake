import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext, SparkConf

from utils.process_bookmark_helper import BookmarkManagerRaw
from utils.raw_to_stage_incremental_iceberg import (
    do_raw_to_stage_process_incremental_load_iceberg,
)


config = {
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.warehouse": "s3://marcos-test-datalake-raw/warehouse",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "glue_catalog",
    "spark.sql.catalog.glue_catalog.default-namespace": "stage",
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",  
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "spark.hadoop.fs.s3a.path.style.access":"true",
    "spark.hadoop.fs.s3a.region":"us-west-2"
}





# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_bucket_name",
        "raw_bucket_prefix",
        "table",
        "timestamp_bookmark_str",
        "bookmark_table",
    ],
)


# Create a SparkConf object using the config dictionary
spark_conf = SparkConf().setAll(config.items())

# Initialize the SparkContext with the SparkConf
sc = SparkContext(conf=spark_conf)

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()


# Get the input and output paths from the job arguments


job.commit()
