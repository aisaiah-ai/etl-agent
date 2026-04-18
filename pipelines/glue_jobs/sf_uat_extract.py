"""AWS Glue job: Extract SF UAT CourseOfferings and sync to Glue catalog.

Reads from salesforce-uat-connection, saves to S3, registers in Glue catalog,
and matches HCG_Format__c to Moodle Atlas ID.
"""

import json
import re
import sys
import time
from difflib import SequenceMatcher

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "output_bucket", "output_prefix"])

# Optional: database name for catalog registration
try:
    extra = getResolvedOptions(sys.argv, ["database"])
    args.update(extra)
except Exception:
    pass

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

SF_CONN = "salesforce-uat-connection"
OUTPUT_BUCKET = args["output_bucket"]
OUTPUT_PREFIX = args.get("output_prefix", "uat/sf_uat_extract/")
GLUE_DB = args.get("database", "uat")


def section(title):
    print("\n" + "=" * 70)
    print("  " + title)
    print("=" * 70)


def read_sf(entity, query=None):
    opts = {
        "connectionName": SF_CONN,
        "ENTITY_NAME": entity,
        "API_VERSION": "v60.0",
    }
    if query:
        opts["QUERY"] = query
    try:
        df = glue_ctx.create_dynamic_frame.from_options(
            connection_type="salesforce",
            connection_options=opts,
        ).toDF()
        return df
    except Exception as e:
        print("  FAILED: %s" % e)
        return None


# ── 1. Discover CourseOffering schema ────────────────────────────────────

section("1. DISCOVER CourseOffering SCHEMA (SF UAT)")
start = time.time()

df_schema = read_sf("CourseOffering")
if df_schema is not None:
    print("  Columns (%d):" % len(df_schema.columns))
    for col in sorted(df_schema.columns):
        print("    - %s" % col)
    print("  Total rows: %d (%.1fs)" % (df_schema.count(), time.time() - start))
    df_schema.show(5, truncate=False)

    # Check specifically for HCG fields
    hcg_cols = [c for c in df_schema.columns if "HCG" in c.upper() or "Format" in c]
    print("  HCG/Format columns found: %s" % (hcg_cols if hcg_cols else "NONE"))
else:
    print("  FAILED to read CourseOffering schema")

# Also try explicit SOQL for HCG_Format__c
section("1b. PROBE HCG_Format__c on CourseOffering")
for field_name in ["HCG_Format__c", "Format__c", "HCG_Format__r", "HCG_Atlas_ID__c"]:
    probe = read_sf("CourseOffering", "SELECT Id, %s FROM CourseOffering LIMIT 1" % field_name)
    if probe is not None:
        print("  FOUND: %s — columns: %s" % (field_name, probe.columns))
        probe.show(5, truncate=False)
    else:
        print("  NOT FOUND: %s" % field_name)


# ── 2. Extract with HCG_Format__c (Atlas ID matching key) ────────────────

section("2. EXTRACT CourseOfferings with HCG_Format__c")
start = time.time()

# Primary query: includes HCG_Format__c for Atlas ID matching
df = read_sf(
    "CourseOffering",
    "SELECT Id, Name, HCG_Format__c, HCG_External_ID__c, LearningCourseId, "
    "StartDate, EndDate, SectionNumber, PrimaryFacultyId "
    "FROM CourseOffering",
)

if df is None:
    # Fallback: try without HCG fields
    print("  Retrying without HCG fields...")
    df = read_sf(
        "CourseOffering",
        "SELECT Id, Name, LearningCourseId, StartDate, EndDate, "
        "SectionNumber, PrimaryFacultyId FROM CourseOffering",
    )

if df is not None:
    total = df.count()
    print("  Records: %d (%.1fs)" % (total, time.time() - start))
    print("  Columns: %s" % df.columns)
    df.show(20, truncate=False)

    # Check HCG_Format__c population (primary matching key)
    if "HCG_Format__c" in df.columns:
        has_fmt = df.filter(df["HCG_Format__c"].isNotNull()).count()
        print("  HCG_Format__c populated: %d / %d" % (has_fmt, total))
        # Show sample values
        print("  Sample HCG_Format__c values:")
        df.select("Id", "Name", "HCG_Format__c").filter(
            df["HCG_Format__c"].isNotNull()
        ).show(10, truncate=False)
    else:
        print("  HCG_Format__c NOT available on this org")

    # Also check HCG_External_ID__c
    if "HCG_External_ID__c" in df.columns:
        has_ext = df.filter(df["HCG_External_ID__c"].isNotNull()).count()
        print("  HCG_External_ID__c populated: %d / %d" % (has_ext, total))
    else:
        print("  HCG_External_ID__c NOT available on this org")

    # ── 3. Save to S3 ───────────────────────────────────────────────────

    section("3. SAVING TO S3")

    # Parquet
    parquet_path = "s3://%s/%ssf_course_offerings_uat/" % (OUTPUT_BUCKET, OUTPUT_PREFIX)
    df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
    print("  Parquet: %s" % parquet_path)

    # CSV
    csv_path = "s3://%s/%ssf_course_offerings_uat_csv/" % (OUTPUT_BUCKET, OUTPUT_PREFIX)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print("  CSV: %s" % csv_path)

    # JSON (collect and write as single file)
    rows = [row.asDict() for row in df.collect()]
    json_str = json.dumps(rows, indent=2, default=str)
    json_df = spark.createDataFrame([{"data": json_str}])
    json_path = "s3://%s/%ssf_course_offerings_uat_json/" % (OUTPUT_BUCKET, OUTPUT_PREFIX)
    json_df.coalesce(1).write.mode("overwrite").text(json_path)
    print("  JSON: %s" % json_path)

    print()
    print("  TOTAL: %d CourseOfferings extracted" % total)

    # ── 4. Register in Glue Catalog ──────────────────────────────────────

    section("4. REGISTER IN GLUE CATALOG (%s)" % GLUE_DB)

    glue_client = boto3.client("glue")
    table_name = "sf_course_offerings_uat"

    # Build columns from DataFrame schema
    columns = []
    spark_to_glue = {
        "StringType": "string",
        "LongType": "bigint",
        "IntegerType": "int",
        "DoubleType": "double",
        "BooleanType": "boolean",
        "TimestampType": "timestamp",
        "DateType": "date",
    }
    for field in df.schema.fields:
        glue_type = spark_to_glue.get(type(field.dataType).__name__, "string")
        columns.append({"Name": field.name, "Type": glue_type})

    table_input = {
        "Name": table_name,
        "Description": "SF UAT CourseOfferings with HCG_Format__c (Atlas ID matching key)",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": parquet_path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet"},
    }

    try:
        glue_client.update_table(DatabaseName=GLUE_DB, TableInput=table_input)
        print("  Updated table %s.%s" % (GLUE_DB, table_name))
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=GLUE_DB, TableInput=table_input)
        print("  Created table %s.%s" % (GLUE_DB, table_name))
    except Exception as e:
        print("  WARNING: Could not register catalog table: %s" % e)

    print("  Location: %s" % parquet_path)
    print("  Columns: %s" % [c["Name"] for c in columns])

    # ── 5. Match HCG_Format__c → Moodle Atlas ID ────────────────────────

    section("5. MATCH HCG_Format__c → Moodle Atlas ID")

    # Try loading moodle_courses from Glue catalog
    moodle_df = None
    try:
        moodle_df = glue_ctx.create_dynamic_frame.from_catalog(
            database=GLUE_DB, table_name="moodle_courses"
        ).toDF()
        print("  Loaded moodle_courses from catalog: %d records" % moodle_df.count())
    except Exception as e1:
        # Fallback: try loading from S3 JSON
        try:
            moodle_df = spark.read.option("multiLine", "true").json(
                "s3://%s/moodle_data/moodle_all_courses.json" % OUTPUT_BUCKET
            )
            print("  Loaded moodle_courses from S3 JSON: %d records" % moodle_df.count())
        except Exception as e2:
            print("  WARNING: Could not load moodle_courses: catalog=%s, s3=%s" % (e1, e2))

    if moodle_df is not None and "HCG_Format__c" in df.columns:
        # Extract atlas_id from Moodle shortname (trailing digits after last '-')
        extract_atlas = F.udf(
            lambda s: re.search(r"-(\d{5,})$", s).group(1)
            if s and re.search(r"-(\d{5,})$", s)
            else None,
            StringType(),
        )
        # Rename Moodle columns to avoid ambiguity with SF columns
        moodle_df = (
            moodle_df
            .withColumnRenamed("id", "moodle_id")
            .withColumnRenamed("shortname", "moodle_shortname")
            .withColumnRenamed("fullname", "moodle_fullname")
            .withColumnRenamed("idnumber", "moodle_idnumber")
            .withColumn("atlas_id", extract_atlas(F.col("moodle_shortname")))
        )

        # UDF: compute name similarity as a percentage (0–100)
        def _name_similarity(a, b):
            if not a or not b:
                return None
            # Normalize: lowercase, collapse whitespace, strip punctuation noise
            norm = lambda s: re.sub(r"\s+", " ", re.sub(r"[/\-_]", " ", s)).strip().lower()
            return round(SequenceMatcher(None, norm(a), norm(b)).ratio() * 100, 1)

        name_sim_udf = F.udf(_name_similarity, StringType())

        has_atlas = moodle_df.filter(F.col("atlas_id").isNotNull()).count()
        print("  Moodle courses with Atlas ID: %d" % has_atlas)

        # Cast HCG_Format__c to string for join
        sf_with_fmt = (
            df.filter(F.col("HCG_Format__c").isNotNull())
            .withColumn("hcg_format_str", F.trim(F.col("HCG_Format__c").cast(StringType())))
            .withColumnRenamed("Id", "sf_id")
            .withColumnRenamed("Name", "sf_name")
        )
        print("  SF offerings with HCG_Format__c: %d" % sf_with_fmt.count())

        # Join on HCG_Format__c = atlas_id
        matched = sf_with_fmt.join(
            moodle_df,
            sf_with_fmt["hcg_format_str"] == moodle_df["atlas_id"],
            "left",
        ).withColumn(
            "shortname_confidence",
            name_sim_udf(F.col("sf_name"), F.col("moodle_shortname")),
        ).withColumn(
            "fullname_confidence",
            name_sim_udf(F.col("sf_name"), F.col("moodle_fullname")),
        )

        total_sf = matched.count()
        matched_count = matched.filter(F.col("atlas_id").isNotNull()).count()
        unmatched_count = total_sf - matched_count

        print()
        print("  MATCH RESULTS:")
        print("  Total SF with HCG_Format__c:  %d" % total_sf)
        print(
            "  Matched to Moodle:            %d (%.1f%%)"
            % (matched_count, matched_count / total_sf * 100 if total_sf else 0)
        )
        print("  Unmatched:                    %d" % unmatched_count)

        # Show matched samples
        print()
        print("  MATCHED SAMPLES (HCG_Format__c = Moodle atlas_id):")
        matched.filter(F.col("atlas_id").isNotNull()).select(
            "sf_id", "sf_name", "HCG_Format__c", "hcg_format_str",
            "moodle_id", "moodle_shortname", "moodle_fullname", "atlas_id",
            "shortname_confidence", "fullname_confidence",
        ).show(20, truncate=False)

        # Show unmatched
        print()
        print("  UNMATCHED (HCG_Format__c not in Moodle):")
        matched.filter(F.col("atlas_id").isNull()).select(
            "sf_id", "sf_name", "HCG_Format__c",
        ).show(20, truncate=False)

        # Save SF→Moodle match results
        match_path = "s3://%s/%shcg_format_atlas_match/" % (OUTPUT_BUCKET, OUTPUT_PREFIX)
        matched.select(
            "sf_id", "sf_name", "HCG_Format__c", "hcg_format_str",
            "moodle_id", "moodle_shortname", "moodle_fullname", "moodle_idnumber",
            "atlas_id", "shortname_confidence", "fullname_confidence",
        ).coalesce(1).write.mode("overwrite").option("header", "true").csv(match_path)
        print("  Match CSV: %s" % match_path)

        # ── 5b. MOODLE PERSPECTIVE SUMMARY ───────────────────────────────

        section("5b. MOODLE PERSPECTIVE — Which Moodle courses have an SF match?")

        total_moodle = moodle_df.count()
        moodle_with_atlas = moodle_df.filter(F.col("atlas_id").isNotNull())
        moodle_no_atlas = moodle_df.filter(F.col("atlas_id").isNull())

        # Left join: Moodle → SF (atlas_id = hcg_format_str)
        moodle_view = moodle_df.join(
            sf_with_fmt,
            moodle_df["atlas_id"] == sf_with_fmt["hcg_format_str"],
            "left",
        ).withColumn(
            "shortname_confidence",
            name_sim_udf(F.col("sf_name"), F.col("moodle_shortname")),
        ).withColumn(
            "fullname_confidence",
            name_sim_udf(F.col("sf_name"), F.col("moodle_fullname")),
        )

        moodle_matched = moodle_view.filter(F.col("hcg_format_str").isNotNull())
        moodle_unmatched_with_atlas = moodle_view.filter(
            F.col("atlas_id").isNotNull() & F.col("hcg_format_str").isNull()
        )
        moodle_unmatched_no_atlas = moodle_view.filter(F.col("atlas_id").isNull())

        matched_cnt = moodle_matched.count()
        unmatched_atlas_cnt = moodle_unmatched_with_atlas.count()
        no_atlas_cnt = moodle_unmatched_no_atlas.count()

        # Check idnumber status on matched Moodle courses
        already_set = moodle_matched.filter(
            (F.col("moodle_idnumber").isNotNull()) & (F.col("moodle_idnumber") != "")
        ).count()
        empty_idnumber = matched_cnt - already_set

        print()
        print("  MOODLE COURSE SUMMARY:")
        print("  %-40s %d" % ("Total Moodle courses", total_moodle))
        print("  %-40s %d" % ("  With Atlas ID (from shortname)", moodle_with_atlas.count()))
        print("  %-40s %d" % ("  Without Atlas ID", no_atlas_cnt))
        print()
        print("  %-40s %d (%.1f%%)" % (
            "Matched to SF (atlas_id = HCG_Format__c)",
            matched_cnt,
            matched_cnt / total_moodle * 100 if total_moodle else 0,
        ))
        print("  %-40s %d" % ("  -> idnumber already set", already_set))
        print("  %-40s %d" % ("  -> idnumber empty (ready to sync)", empty_idnumber))
        print("  %-40s %d" % (
            "Has Atlas ID but NO SF match",
            unmatched_atlas_cnt,
        ))
        print("  %-40s %d" % ("No Atlas ID (cannot match)", no_atlas_cnt))

        # Show matched Moodle courses
        print()
        print("  MOODLE MATCHED TO SF:")
        moodle_matched.select(
            "moodle_id", "moodle_shortname", "moodle_fullname",
            "atlas_id", "moodle_idnumber",
            "sf_id", "sf_name", "HCG_Format__c",
            "shortname_confidence", "fullname_confidence",
        ).orderBy("moodle_shortname").show(50, truncate=False)

        # Show Moodle with Atlas ID but no SF match
        if unmatched_atlas_cnt > 0:
            print()
            print("  MOODLE WITH ATLAS ID — NO SF MATCH (%d, showing first 50):" % unmatched_atlas_cnt)
            moodle_unmatched_with_atlas.select(
                "moodle_id", "moodle_shortname", "moodle_fullname",
                "atlas_id", "moodle_idnumber",
            ).orderBy("moodle_shortname").show(50, truncate=False)

        # Show Moodle without Atlas ID
        if no_atlas_cnt > 0 and no_atlas_cnt <= 50:
            print()
            print("  MOODLE WITHOUT ATLAS ID:")
            moodle_unmatched_no_atlas.select(
                "moodle_id", "moodle_shortname", "moodle_fullname", "moodle_idnumber",
            ).orderBy("moodle_shortname").show(50, truncate=False)
        elif no_atlas_cnt > 50:
            print()
            print("  MOODLE WITHOUT ATLAS ID (%d total, showing first 20):" % no_atlas_cnt)
            moodle_unmatched_no_atlas.select(
                "moodle_id", "moodle_shortname", "moodle_fullname", "moodle_idnumber",
            ).orderBy("moodle_shortname").show(20, truncate=False)

        # Save Moodle-perspective report
        moodle_report_path = "s3://%s/%smoodle_match_report/" % (OUTPUT_BUCKET, OUTPUT_PREFIX)
        moodle_view.select(
            "moodle_id", "moodle_shortname", "moodle_fullname",
            "atlas_id", "moodle_idnumber",
            "sf_id", "sf_name", "HCG_Format__c",
            F.when(F.col("hcg_format_str").isNotNull(), "MATCHED")
            .when(F.col("atlas_id").isNotNull(), "NO_SF_MATCH")
            .otherwise("NO_ATLAS_ID")
            .alias("match_status"),
            "shortname_confidence", "fullname_confidence",
        ).coalesce(1).write.mode("overwrite").option("header", "true").csv(moodle_report_path)
        print()
        print("  Moodle report CSV: %s" % moodle_report_path)

    elif "HCG_Format__c" not in df.columns:
        print("  SKIP: HCG_Format__c not available — cannot match to Moodle Atlas ID")
    else:
        print("  SKIP: Moodle courses not available for matching")

else:
    print("  FAILED to extract CourseOfferings")


# ── 6. Also grab LearningCourse for context ─────────────────────────────

section("6. EXTRACT LearningCourse (course catalog)")
start = time.time()

lc_df = read_sf("hed__Course__c")
if lc_df is None:
    lc_df = read_sf("LearningCourse")

if lc_df is not None:
    print("  Columns: %s" % sorted(lc_df.columns))
    print("  Records: %d (%.1fs)" % (lc_df.count(), time.time() - start))
    lc_df.show(10, truncate=False)

    lc_path = "s3://%s/%slearning_courses/" % (OUTPUT_BUCKET, OUTPUT_PREFIX)
    lc_df.coalesce(1).write.mode("overwrite").parquet(lc_path)
    print("  Saved: %s" % lc_path)
else:
    print("  LearningCourse not found")


section("DONE")
job.commit()
