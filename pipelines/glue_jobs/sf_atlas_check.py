"""Quick Glue job: Check if SF CourseOffering has Atlas/External ID populated."""

import sys
import json
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

SF_CONN = "Sandbox Salesforce Connection"


def read_sf(entity, query):
    try:
        df = glue_ctx.create_dynamic_frame.from_options(
            connection_type="salesforce",
            connection_options={
                "connectionName": SF_CONN,
                "ENTITY_NAME": entity,
                "API_VERSION": "v60.0",
                "QUERY": query,
            },
        ).toDF()
        return df
    except Exception as e:
        print(f"FAILED: {e}")
        return None


# ── 1. Get ALL columns on CourseOffering ─────────────────────────────────
print("=" * 70)
print("  1. ALL CourseOffering columns (schema discovery)")
print("=" * 70)

df_all = glue_ctx.create_dynamic_frame.from_options(
    connection_type="salesforce",
    connection_options={
        "connectionName": SF_CONN,
        "ENTITY_NAME": "CourseOffering",
        "API_VERSION": "v60.0",
    },
).toDF()

if df_all is not None:
    print(f"  Total columns: {len(df_all.columns)}")
    print(f"  Total rows: {df_all.count()}")
    print()
    for col in sorted(df_all.columns):
        print(f"    {col}")

    # Show fields with "external", "atlas", "hcg", "term", "session", "course" in name
    interesting = [c for c in df_all.columns
                   if any(kw in c.lower() for kw in
                          ["external", "atlas", "hcg", "term", "session",
                           "faculty", "location", "site", "city", "country"])]
    if interesting:
        print()
        print(f"  Interesting columns: {interesting}")
        df_all.select(["Id", "Name"] + interesting).show(30, truncate=False)


# ── 2. Check HCG_External_ID__c specifically ────────────────────────────
print()
print("=" * 70)
print("  2. CourseOffering with HCG_External_ID__c")
print("=" * 70)

df = read_sf(
    "CourseOffering",
    "SELECT Id, Name, HCG_External_ID__c FROM CourseOffering LIMIT 50",
)
if df is not None:
    print(f"  Records: {df.count()}")
    df.show(50, truncate=False)

    # Check how many have it populated
    populated = df.filter(df["HCG_External_ID__c"].isNotNull()).count()
    print(f"  HCG_External_ID__c populated: {populated}/{df.count()}")
else:
    print("  HCG_External_ID__c not available — trying all fields above")


# ── 3. Full extract with External ID ────────────────────────────────────
print()
print("=" * 70)
print("  3. ALL CourseOfferings with External ID (full extract)")
print("=" * 70)

df_full = read_sf(
    "CourseOffering",
    "SELECT Id, Name, HCG_External_ID__c FROM CourseOffering",
)
if df_full is not None:
    total = df_full.count()
    has_ext = df_full.filter(df_full["HCG_External_ID__c"].isNotNull()).count()
    empty_ext = total - has_ext
    print(f"  Total CourseOfferings: {total}")
    print(f"  With External ID:     {has_ext}")
    print(f"  Without External ID:  {empty_ext}")

    # Sample of populated ones
    print()
    print("  Samples WITH External ID:")
    df_full.filter(df_full["HCG_External_ID__c"].isNotNull()).show(20, truncate=False)

    # Save full extract as JSON for local analysis
    rows = [row.asDict() for row in df_full.collect()]
    print()
    print(json.dumps(rows[:5], indent=2, default=str))
else:
    print("  Query failed")

job.commit()
print("\nDone.")
