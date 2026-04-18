"""
ETL Agent — Glue Transform Job
Reads from Glue Data Catalog (financial_hist_ext) and writes to S3 as Parquet.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "catalog_database", "data_bucket"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

catalog_db = args["catalog_database"]
data_bucket = args["data_bucket"]

# Read all tables from the Glue catalog database
tables = glue_context.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name="historic_all_financial_transactions",
)

print(f"Read {tables.count()} records from historic_all_financial_transactions")

# Write to S3 as Parquet
output_path = f"s3://{data_bucket}/output/historic_all_financial_transactions/"
glue_context.write_dynamic_frame.from_options(
    frame=tables,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
)

print(f"Wrote output to {output_path}")

job.commit()
