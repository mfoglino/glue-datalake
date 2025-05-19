import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext


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
table_name = "people_table"

logger.info(f"Showing metadata...")
spark.sql("SHOW TABLES IN raw").show()
spark.sql("SELECT current_catalog()").show()
spark.sql("SELECT current_schema()").show()
spark.sql("USE SCHEMA raw")
logger.info(f"Spark catalog imp {spark.conf.get('spark.sql.catalogImplementation')}")

spark.sql(f"DESCRIBE stage.{table_name}").show()
#table_schema = spark.table(f"raw.{table}").schema
#print(table_schema)


job.commit()
