"""AWS Glue job: Sync SF Course Offering IDs → Moodle Course ID Numbers.

Glue job arguments:
    --sf_connection_name   Glue Salesforce connection name
    --moodle_url           Moodle site URL
    --moodle_secret_name   Secrets Manager secret with Moodle token
    --dry_run              "true" to match only (default: "false")
    --output_bucket        S3 bucket for sync results
"""

import json
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Resolve Glue job arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "sf_connection_name",
        "moodle_url",
        "moodle_secret_name",
        "dry_run",
        "output_bucket",
    ],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

dry_run = args.get("dry_run", "false").lower() == "true"

# ── Step 1: Read SF Course Offerings ─────────────────────────────────────
logger.info("Reading SF Course Offerings via Glue connector: %s", args["sf_connection_name"])
sf_dyf = glue_ctx.create_dynamic_frame.from_options(
    connection_type="salesforce",
    connection_options={
        "connectionName": args["sf_connection_name"],
        "ENTITY_NAME": "CourseOffering",
        "API_VERSION": "v60.0",
        "QUERY": "SELECT Id, Name FROM CourseOffering",
    },
)
sf_df = sf_dyf.toDF()
sf_offerings = [row.asDict() for row in sf_df.collect()]
logger.info("Read %d SF Course Offerings", len(sf_offerings))

# ── Step 2: Fetch Moodle courses and match ───────────────────────────────
# Import sync logic (bundled as --extra-py-files)
from pipelines.sf_moodle_sync.moodle_client import MoodleClient
from pipelines.sf_moodle_sync.course_sync import sync_courses

moodle = MoodleClient(
    base_url=args["moodle_url"],
    secret_name=args["moodle_secret_name"],
)

result = sync_courses(sf_offerings, moodle, dry_run=dry_run)

# ── Step 3: Write results to S3 ─────────────────────────────────────────
summary = {
    "matched": len(result.matched),
    "unmatched_sf": len(result.unmatched_sf),
    "already_set": len(result.already_set),
    "updated": len(result.updated),
    "errors": len(result.errors),
    "dry_run": dry_run,
    "matched_details": [m.__dict__ for m in result.matched],
    "unmatched_details": result.unmatched_sf,
    "error_details": result.errors,
}

output_path = f"s3://{args['output_bucket']}/sf_moodle_sync/course_sync_result.json"
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.createDataFrame([{"result": json.dumps(summary)}]).coalesce(1).write.mode("overwrite").text(
    f"s3://{args['output_bucket']}/sf_moodle_sync/course_sync/"
)

logger.info(
    "Course sync complete: %d matched, %d updated, %d unmatched, %d errors",
    len(result.matched), len(result.updated),
    len(result.unmatched_sf), len(result.errors),
)

job.commit()
