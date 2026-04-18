"""Glue job: Match gold_course_offering (course_id) to Moodle courses (Atlas ID).

Reads gold_course_offering from ies-prd-salesforce-migration-gold catalog,
reads moodle_courses from uat catalog, matches on course_id = Atlas ID.
Writes results to S3.
"""

import re
import sys
from difflib import SequenceMatcher

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "output_bucket"])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

OUTPUT_BUCKET = args["output_bucket"]


def section(title):
    print("\n" + "=" * 70)
    print("  " + title)
    print("=" * 70)


# ── 1. Load gold_course_offering ─────────────────────────────────────────

section("1. LOADING gold_course_offering")

gold_df = glue_ctx.create_dynamic_frame.from_catalog(
    database="ies-prd-salesforce-migration-gold",
    table_name="gold_course_offering",
).toDF()

print("  Records: %d" % gold_df.count())
print("  Columns: %s" % gold_df.columns)
gold_df.show(10, truncate=False)

# Check course_id
has_course_id = gold_df.filter(F.col("Course_Id").isNotNull()).count()
print("  With course_id: %d" % has_course_id)

# Sample course_id values
print("  Sample course_id values:")
gold_df.select("course_id", "name", "hcg_external_id__c").show(20, truncate=False)


# ── 2. Load Moodle courses ──────────────────────────────────────────────

section("2. LOADING moodle_courses from S3")

moodle_df = spark.read.option("multiLine", "true").json(
    "s3://%s/moodle_data/moodle_all_courses.json" % OUTPUT_BUCKET
)

print("  Records: %d" % moodle_df.count())
print("  Columns: %s" % moodle_df.columns)

# Extract Atlas ID from shortname (last number after '-')
extract_atlas = F.udf(
    lambda s: re.search(r'-(\d+)$', s).group(1) if s and re.search(r'-(\d+)$', s) else None,
    StringType(),
)
moodle_df = moodle_df.withColumn("atlas_id", extract_atlas(F.col("shortname")))

has_atlas = moodle_df.filter(F.col("atlas_id").isNotNull()).count()
print("  With Atlas ID: %d" % has_atlas)
moodle_df.show(10, truncate=False)


# ── 3. Match on course_id = atlas_id ────────────────────────────────────

section("3. MATCHING: gold.course_id = moodle.atlas_id")

# Cast course_id to string for join
gold_df = gold_df.withColumn("Course_Id_str", F.col("Course_Id").cast(StringType()))

matched_df = gold_df.join(
    moodle_df,
    gold_df["Course_Id_str"] == moodle_df["atlas_id"],
    "left",
)

total = matched_df.count()
matched_count = matched_df.filter(F.col("atlas_id").isNotNull()).count()
unmatched_count = total - matched_count

print("  Total gold records:    %d" % total)
print("  Matched to Moodle:     %d (%.1f%%)" % (matched_count, matched_count / total * 100 if total else 0))
print("  Unmatched:             %d" % unmatched_count)

# Show matched samples
print()
print("  MATCHED SAMPLES:")
matched_df.filter(F.col("atlas_id").isNotNull()) \
    .select("course_id", "name", "shortname", "fullname", "atlas_id") \
    .show(20, truncate=False)

# Show unmatched samples
print()
print("  UNMATCHED SAMPLES:")
matched_df.filter(F.col("atlas_id").isNull()) \
    .select("Course_Id", "Name", "HCG_External_ID__c") \
    .show(20, truncate=False)


# ── 4. Moodle not in gold ───────────────────────────────────────────────

section("4. MOODLE NOT IN GOLD")

moodle_atlas = moodle_df.filter(F.col("atlas_id").isNotNull())
moodle_not_in_gold = moodle_atlas.join(
    gold_df,
    moodle_atlas["atlas_id"] == gold_df["Course_Id_str"],
    "left_anti",
)
print("  Moodle with Atlas ID not in gold: %d" % moodle_not_in_gold.count())
moodle_not_in_gold.select("id", "shortname", "fullname", "atlas_id").show(20, truncate=False)


# ── 5. Save results ─────────────────────────────────────────────────────

section("5. SAVING RESULTS")

# Full match
out_match = "s3://%s/gold_moodle_match/matched/" % OUTPUT_BUCKET
matched_df.select(
    "Course_Id", "Name", "HCG_External_ID__c", "SectionNumber",
    "StartDate", "EndDate", "IsActive",
    F.col("id").alias("moodle_id"),
    F.col("shortname").alias("moodle_shortname"),
    F.col("fullname").alias("moodle_fullname"),
    "atlas_id",
).coalesce(1).write.mode("overwrite").option("header", "true").csv(out_match)
print("  Matched CSV: %s" % out_match)

# Moodle not in gold
out_moodle = "s3://%s/gold_moodle_match/moodle_not_in_gold/" % OUTPUT_BUCKET
moodle_not_in_gold.select("id", "shortname", "fullname", "idnumber", "atlas_id") \
    .coalesce(1).write.mode("overwrite").option("header", "true").csv(out_moodle)
print("  Moodle unmatched CSV: %s" % out_moodle)

# Summary
print()
print("  SUMMARY:")
print("  Gold course offerings:     %d" % total)
print("  Matched to Moodle:         %d (%.1f%%)" % (matched_count, matched_count / total * 100 if total else 0))
print("  Gold unmatched:            %d" % unmatched_count)
print("  Moodle not in gold:        %d" % moodle_not_in_gold.count())

section("DONE")
job.commit()
