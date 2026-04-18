"""AWS Glue job: Compare SF CourseOfferings to Moodle Courses using Atlas ID.

Reads sf_course_offerings_uat and moodle_courses from the Glue catalog (uat database),
matches on Atlas ID (exact, then nearby), verifies course names, and writes results.

Glue job arguments:
    --database        Glue catalog database name (default: uat)
    --output_bucket   S3 bucket for results
    --output_prefix   S3 prefix for results
"""

import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "database", "output_bucket", "output_prefix"]
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

DB = args.get("database", "uat")
OUTPUT_BUCKET = args["output_bucket"]
OUTPUT_PREFIX = args.get("output_prefix", "atlas_comparison/")
RUN_TS = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")


def section(title):
    print("\n" + "=" * 70)
    print("  " + title)
    print("=" * 70)


def normalize(s):
    return " ".join(s.lower().split())


def extract_sf_title(name):
    """Strip '250 - ' prefix from SF name."""
    m = re.match(r'^\d+[A-Za-z]?\s*[-\u2013]\s*(.+)$', name)
    return m.group(1).strip() if m else name


def extract_moodle_title(fullname):
    """Strip 'CITY-DEPT NNN-SEC (YEAR TERM) - ID-' prefix from Moodle fullname."""
    m = re.match(
        r'^.*?\(\d{4}\s+\w+(?:\s+\d)?\)\s*[-\u2013]\s*\d*[-\u2013\s]*(.+)$', fullname
    )
    if m:
        return m.group(1).strip()
    m = re.match(r'^[A-Z]{2,}\s+\d+[/A-Za-z]*\s*\d*\s+(.+?)(?:\s*\(\d{4}\s+\w+\))?$', fullname)
    if m:
        return m.group(1).strip()
    return fullname


def extract_atlas_id(shortname):
    """Extract Atlas ID from end of Moodle shortname."""
    m = re.search(r'-(\d{5,})$', shortname)
    return m.group(1) if m else None


# ── Load from Glue catalog ───────────────────────────────────────────────

section("LOADING DATA FROM GLUE CATALOG (%s)" % DB)

sf_df = glue_ctx.create_dynamic_frame.from_catalog(
    database=DB, table_name="sf_course_offerings_uat"
).toDF()
sf_rows = [row.asDict() for row in sf_df.collect()]
print("  SF offerings: %d" % len(sf_rows))

moodle_df = glue_ctx.create_dynamic_frame.from_catalog(
    database=DB, table_name="moodle_courses"
).toDF()
moodle_rows = [row.asDict() for row in moodle_df.collect()]
print("  Moodle courses: %d" % len(moodle_rows))


# ── Build Moodle Atlas ID index ──────────────────────────────────────────

section("BUILDING ATLAS ID INDEX")

moodle_by_atlas = {}
moodle_no_atlas = 0
for c in moodle_rows:
    atlas = extract_atlas_id(c.get("shortname", ""))
    if atlas:
        if atlas not in moodle_by_atlas:
            moodle_by_atlas[atlas] = []
        moodle_by_atlas[atlas].append(c)
    else:
        moodle_no_atlas += 1

print("  Moodle courses with Atlas ID: %d unique IDs" % len(moodle_by_atlas))
print("  Moodle courses without Atlas ID: %d" % moodle_no_atlas)


# ── Match ────────────────────────────────────────────────────────────────

section("MATCHING SF → MOODLE via Atlas ID")

results = []
stats = defaultdict(int)

for sf in sf_rows:
    sf_id = sf.get("sf_id", "")
    sf_name = sf.get("name", "")
    sf_title = extract_sf_title(sf_name)
    atlas_id = sf.get("atlas_id", "")
    learning_course = sf.get("learning_course_name", "")

    row = {
        "sf_id": sf_id,
        "sf_name": sf_name,
        "sf_title": sf_title,
        "learning_course": learning_course,
        "sf_atlas_id": atlas_id,
    }

    # Try exact match first
    if atlas_id and atlas_id in moodle_by_atlas:
        mc_list = moodle_by_atlas[atlas_id]
        mc = mc_list[0]  # take first match
        moodle_title = extract_moodle_title(mc.get("fullname", ""))
        sim = SequenceMatcher(None, normalize(sf_title), normalize(moodle_title)).ratio()

        row.update({
            "moodle_atlas_id": atlas_id,
            "offset": 0,
            "atlas_match": "EXACT",
            "moodle_id": mc.get("moodle_id", ""),
            "moodle_shortname": mc.get("shortname", ""),
            "moodle_fullname": mc.get("fullname", ""),
            "moodle_title": moodle_title,
            "moodle_idnumber": mc.get("idnumber", ""),
            "title_similarity": round(sim * 100, 1),
            "name_match": "YES" if sim >= 0.90 else "LIKELY" if sim >= 0.70 else "PARTIAL" if sim >= 0.50 else "NO",
            "moodle_sections": len(mc_list),
        })
        stats["exact"] += 1

    # Try nearby (±1 to ±5)
    elif atlas_id:
        found = False
        for offset in range(1, 6):
            for sign in [-1, 1]:
                candidate = str(int(atlas_id) + (sign * offset))
                if candidate in moodle_by_atlas:
                    mc_list = moodle_by_atlas[candidate]
                    mc = mc_list[0]
                    moodle_title = extract_moodle_title(mc.get("fullname", ""))
                    sim = SequenceMatcher(
                        None, normalize(sf_title), normalize(moodle_title)
                    ).ratio()

                    row.update({
                        "moodle_atlas_id": candidate,
                        "offset": sign * offset,
                        "atlas_match": "NEARBY_%+d" % (sign * offset),
                        "moodle_id": mc.get("moodle_id", ""),
                        "moodle_shortname": mc.get("shortname", ""),
                        "moodle_fullname": mc.get("fullname", ""),
                        "moodle_title": moodle_title,
                        "moodle_idnumber": mc.get("idnumber", ""),
                        "title_similarity": round(sim * 100, 1),
                        "name_match": "YES" if sim >= 0.90 else "LIKELY" if sim >= 0.70 else "PARTIAL" if sim >= 0.50 else "NO",
                        "moodle_sections": len(mc_list),
                    })
                    stats["nearby_%+d" % (sign * offset)] += 1
                    found = True
                    break
            if found:
                break

        if not found:
            row.update({
                "moodle_atlas_id": "",
                "offset": "",
                "atlas_match": "NONE",
                "moodle_id": "",
                "moodle_shortname": "",
                "moodle_fullname": "",
                "moodle_title": "",
                "moodle_idnumber": "",
                "title_similarity": 0.0,
                "name_match": "",
                "moodle_sections": 0,
            })
            stats["none"] += 1
    else:
        row.update({
            "moodle_atlas_id": "",
            "offset": "",
            "atlas_match": "NO_ATLAS_ID",
            "moodle_id": "",
            "moodle_shortname": "",
            "moodle_fullname": "",
            "moodle_title": "",
            "moodle_idnumber": "",
            "title_similarity": 0.0,
            "name_match": "",
            "moodle_sections": 0,
        })
        stats["no_atlas_id"] += 1

    results.append(row)


# ── Summary ──────────────────────────────────────────────────────────────

section("RESULTS SUMMARY")
print("  Total SF records: %d" % len(results))
for k in sorted(stats.keys()):
    print("  %-20s %d" % (k, stats[k]))

matched = [r for r in results if r["atlas_match"] not in ("NONE", "NO_ATLAS_ID")]
name_yes = sum(1 for r in matched if r["name_match"] == "YES")
name_likely = sum(1 for r in matched if r["name_match"] == "LIKELY")
name_partial = sum(1 for r in matched if r["name_match"] == "PARTIAL")
name_no = sum(1 for r in matched if r["name_match"] == "NO")

print()
print("  Name verification (%d Atlas-matched):" % len(matched))
print("    YES (>=90%%):      %d" % name_yes)
print("    LIKELY (70-89%%):  %d" % name_likely)
print("    PARTIAL (50-69%%): %d" % name_partial)
print("    NO (<50%%):        %d" % name_no)

# Log detailed mismatches
print()
print("  NAME MISMATCHES (Atlas matched but name != YES):")
for r in results:
    if r["atlas_match"] not in ("NONE", "NO_ATLAS_ID") and r["name_match"] != "YES":
        print('    SF Atlas %s → Moodle %s (offset %s) sim=%.1f%%' % (
            r["sf_atlas_id"], r["moodle_atlas_id"], r["offset"], r["title_similarity"]))
        print('      SF:     "%s"' % r["sf_title"])
        print('      Moodle: "%s"' % r["moodle_title"])

print()
print("  UNMATCHED:")
for r in results:
    if r["atlas_match"] in ("NONE", "NO_ATLAS_ID"):
        print('    SF Atlas %s  "%s"' % (r["sf_atlas_id"], r["sf_name"]))


# ── Write results ────────────────────────────────────────────────────────

section("WRITING RESULTS TO S3")

schema = StructType([
    StructField("sf_id", StringType(), True),
    StructField("sf_name", StringType(), True),
    StructField("sf_title", StringType(), True),
    StructField("learning_course", StringType(), True),
    StructField("sf_atlas_id", StringType(), True),
    StructField("moodle_atlas_id", StringType(), True),
    StructField("offset", StringType(), True),
    StructField("atlas_match", StringType(), True),
    StructField("moodle_id", StringType(), True),
    StructField("moodle_shortname", StringType(), True),
    StructField("moodle_fullname", StringType(), True),
    StructField("moodle_title", StringType(), True),
    StructField("moodle_idnumber", StringType(), True),
    StructField("title_similarity", DoubleType(), True),
    StructField("name_match", StringType(), True),
    StructField("moodle_sections", IntegerType(), True),
])

# Convert offset to string for schema
for r in results:
    r["offset"] = str(r["offset"]) if r["offset"] != "" else ""
    r["moodle_id"] = str(r["moodle_id"]) if r["moodle_id"] != "" else ""
    r["moodle_sections"] = int(r["moodle_sections"]) if r.get("moodle_sections") else 0

results_df = spark.createDataFrame(results, schema=schema)

# CSV
csv_path = "s3://%s/%s%s/atlas_comparison.csv/" % (OUTPUT_BUCKET, OUTPUT_PREFIX, RUN_TS)
results_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
print("  CSV:     %s" % csv_path)

# Parquet
parquet_path = "s3://%s/%s%s/atlas_comparison.parquet/" % (OUTPUT_BUCKET, OUTPUT_PREFIX, RUN_TS)
results_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
print("  Parquet: %s" % parquet_path)

# Summary JSON
summary = {
    "run_timestamp": RUN_TS,
    "sf_count": len(sf_rows),
    "moodle_count": len(moodle_rows),
    "moodle_atlas_ids": len(moodle_by_atlas),
    "stats": dict(stats),
    "name_verification": {
        "yes": name_yes, "likely": name_likely,
        "partial": name_partial, "no": name_no,
    },
}
summary_df = spark.createDataFrame([{"summary": json.dumps(summary)}])
summary_path = "s3://%s/%s%s/summary/" % (OUTPUT_BUCKET, OUTPUT_PREFIX, RUN_TS)
summary_df.coalesce(1).write.mode("overwrite").text(summary_path)
print("  Summary: %s" % summary_path)

section("DONE")
job.commit()
