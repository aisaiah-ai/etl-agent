"""AWS Glue job: Compare SF Course Offerings against Moodle Courses.

Reads cached JSON files from S3 (sf_all_course_offerings.json, moodle_all_courses_cache.json),
runs tiered fuzzy matching, and writes a comparison report to S3.

Glue job arguments:
    --data_bucket       S3 bucket containing the input JSON files
    --data_prefix       S3 prefix for input files (default: "course_comparison/")
    --output_prefix     S3 prefix for output (default: "course_comparison/results/")
"""

import json
import logging
import sys
from datetime import datetime
from difflib import SequenceMatcher

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("course_comparison")

# ── Glue init ────────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "data_bucket", "data_prefix", "output_prefix"],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

DATA_BUCKET = args["data_bucket"]
DATA_PREFIX = args.get("data_prefix", "course_comparison/")
OUTPUT_PREFIX = args.get("output_prefix", "course_comparison/results/")
RUN_TS = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")

FUZZY_AUTO_THRESHOLD = 0.90
FUZZY_CANDIDATE_THRESHOLD = 0.50


# ── Helpers ──────────────────────────────────────────────────────────────

def normalize(name):
    """Lowercase, collapse whitespace."""
    return " ".join(name.lower().split())


def best_match(sf_name, moodle_courses):
    """Find the best fuzzy match for an SF name among Moodle courses.

    Returns (best_course, similarity, matched_field).
    """
    best = None
    best_score = 0.0
    best_field = ""
    norm_sf = normalize(sf_name)

    for mc in moodle_courses:
        for field in ("shortname", "fullname"):
            mval = mc.get(field, "")
            if not mval:
                continue
            score = SequenceMatcher(None, norm_sf, normalize(mval)).ratio()
            if score > best_score:
                best_score = score
                best = mc
                best_field = field

    return best, best_score, best_field


# ── Load data from S3 ───────────────────────────────────────────────────

def section(title):
    logger.info("")
    logger.info("=" * 70)
    logger.info("  %s", title)
    logger.info("=" * 70)


section("LOADING DATA FROM S3")

sf_path = f"s3://{DATA_BUCKET}/{DATA_PREFIX}sf_all_course_offerings.json"
moodle_path = f"s3://{DATA_BUCKET}/{DATA_PREFIX}moodle_all_courses_cache.json"

logger.info("  SF offerings:   %s", sf_path)
logger.info("  Moodle courses: %s", moodle_path)

# Read JSON files via Spark (handles S3 natively)
sf_raw = spark.read.json(sf_path)
moodle_raw = spark.read.json(moodle_path)

sf_offerings = [row.asDict() for row in sf_raw.collect()]
moodle_courses = [row.asDict() for row in moodle_raw.collect()]

logger.info("  Loaded %d SF offerings, %d Moodle courses", len(sf_offerings), len(moodle_courses))


# ── Build exact-match lookups ────────────────────────────────────────────

section("MATCHING: %d SF offerings × %d Moodle courses" % (len(sf_offerings), len(moodle_courses)))

by_short = {}
by_full = {}
for mc in moodle_courses:
    short = normalize(mc.get("shortname", ""))
    full = normalize(mc.get("fullname", ""))
    if short:
        by_short[short] = mc
    if full:
        by_full[full] = mc

results = []
matched_moodle_ids = set()
stats = {"exact_short": 0, "exact_full": 0, "fuzzy_auto": 0, "needs_review": 0, "unmatched": 0}

for idx, sf in enumerate(sf_offerings):
    if idx % 100 == 0:
        logger.info("  Processing SF offering %d/%d ...", idx, len(sf_offerings))

    sf_id = sf.get("Id", "")
    sf_name = sf.get("Name", "")
    if not sf_name:
        results.append({
            "sf_id": sf_id, "sf_name": sf_name,
            "moodle_id": None, "moodle_shortname": "", "moodle_fullname": "",
            "match_type": "unmatched", "confidence": 0.0, "moodle_idnumber": "",
        })
        stats["unmatched"] += 1
        continue

    norm = normalize(sf_name)

    # ── Tier 1: Exact match ──
    mc = by_short.get(norm) or by_full.get(norm)
    if mc and mc["id"] not in matched_moodle_ids:
        match_type = "exact_short" if norm == normalize(mc.get("shortname", "")) else "exact_full"
        results.append({
            "sf_id": sf_id, "sf_name": sf_name,
            "moodle_id": mc["id"],
            "moodle_shortname": mc.get("shortname", ""),
            "moodle_fullname": mc.get("fullname", ""),
            "match_type": match_type, "confidence": 1.0,
            "moodle_idnumber": mc.get("idnumber", ""),
        })
        matched_moodle_ids.add(mc["id"])
        stats[match_type] += 1
        continue

    # ── Tier 2/3: Fuzzy match ──
    available = [c for c in moodle_courses if c["id"] not in matched_moodle_ids]
    best_mc, score, best_field = best_match(sf_name, available)

    if best_mc and score >= FUZZY_AUTO_THRESHOLD:
        results.append({
            "sf_id": sf_id, "sf_name": sf_name,
            "moodle_id": best_mc["id"],
            "moodle_shortname": best_mc.get("shortname", ""),
            "moodle_fullname": best_mc.get("fullname", ""),
            "match_type": "fuzzy_auto", "confidence": round(score, 3),
            "moodle_idnumber": best_mc.get("idnumber", ""),
        })
        matched_moodle_ids.add(best_mc["id"])
        stats["fuzzy_auto"] += 1
    elif best_mc and score >= FUZZY_CANDIDATE_THRESHOLD:
        results.append({
            "sf_id": sf_id, "sf_name": sf_name,
            "moodle_id": best_mc["id"],
            "moodle_shortname": best_mc.get("shortname", ""),
            "moodle_fullname": best_mc.get("fullname", ""),
            "match_type": "needs_review", "confidence": round(score, 3),
            "moodle_idnumber": best_mc.get("idnumber", ""),
        })
        stats["needs_review"] += 1
    else:
        results.append({
            "sf_id": sf_id, "sf_name": sf_name,
            "moodle_id": best_mc["id"] if best_mc else None,
            "moodle_shortname": best_mc.get("shortname", "") if best_mc else "",
            "moodle_fullname": best_mc.get("fullname", "") if best_mc else "",
            "match_type": "unmatched", "confidence": round(score, 3) if best_mc else 0.0,
            "moodle_idnumber": best_mc.get("idnumber", "") if best_mc else "",
        })
        stats["unmatched"] += 1


# ── Summary ──────────────────────────────────────────────────────────────

section("RESULTS SUMMARY")
logger.info("  Exact (shortname): %d", stats["exact_short"])
logger.info("  Exact (fullname):  %d", stats["exact_full"])
logger.info("  Fuzzy auto (≥90%%): %d", stats["fuzzy_auto"])
logger.info("  Needs review:      %d", stats["needs_review"])
logger.info("  Unmatched:         %d", stats["unmatched"])
logger.info("  Total:             %d", len(results))


# ── Write results to S3 ─────────────────────────────────────────────────

section("WRITING RESULTS TO S3")

schema = StructType([
    StructField("sf_id", StringType(), True),
    StructField("sf_name", StringType(), True),
    StructField("moodle_id", IntegerType(), True),
    StructField("moodle_shortname", StringType(), True),
    StructField("moodle_fullname", StringType(), True),
    StructField("match_type", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("moodle_idnumber", StringType(), True),
])

results_df = spark.createDataFrame(results, schema=schema)

# Write as CSV (single partition for easy download)
csv_path = f"s3://{DATA_BUCKET}/{OUTPUT_PREFIX}{RUN_TS}/course_match_report/"
results_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
logger.info("  CSV: %s", csv_path)

# Write as Parquet (efficient for Athena/Redshift Spectrum)
parquet_path = f"s3://{DATA_BUCKET}/{OUTPUT_PREFIX}{RUN_TS}/course_match_parquet/"
results_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
logger.info("  Parquet: %s", parquet_path)

# Write summary JSON
summary = {
    "run_timestamp": RUN_TS,
    "sf_offerings_count": len(sf_offerings),
    "moodle_courses_count": len(moodle_courses),
    "stats": stats,
}
summary_df = spark.createDataFrame([{"summary": json.dumps(summary)}])
summary_path = f"s3://{DATA_BUCKET}/{OUTPUT_PREFIX}{RUN_TS}/summary/"
summary_df.coalesce(1).write.mode("overwrite").text(summary_path)
logger.info("  Summary: %s", summary_path)

section("DONE")
logger.info("  Results at: s3://%s/%s%s/", DATA_BUCKET, OUTPUT_PREFIX, RUN_TS)

job.commit()
