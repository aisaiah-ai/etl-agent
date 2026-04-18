"""Glue job: Run Atlas course query against RDS and save results to S3."""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "connection_name", "output_bucket"])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

CONN = args["connection_name"]
OUTPUT_BUCKET = args["output_bucket"]


def section(title):
    print("\n" + "=" * 70)
    print("  " + title)
    print("=" * 70)


# ── 1. Test connection — list atlas tables ───────────────────────────────

section("1. TESTING CONNECTION: %s" % CONN)

try:
    test_df = glue_ctx.create_dynamic_frame.from_options(
        connection_type="sqlserver",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONN,
            "dbtable": "INFORMATION_SCHEMA.TABLES",
            "sampleQuery": "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'atlas' ORDER BY TABLE_NAME",
        },
    ).toDF()
    print("  Atlas tables: %d" % test_df.count())
    test_df.show(50, truncate=False)
except Exception as e:
    print("  Method 1 failed: %s" % str(e)[:200])

    # Try method 2: use connection properties directly
    try:
        conn_props = glue_ctx.extract_jdbc_conf(CONN)
        print("  Connection props: %s" % {k: v[:30] + "..." if len(str(v)) > 30 else v for k, v in conn_props.items()})

        test_df = spark.read.format("jdbc") \
            .option("url", conn_props["fullUrl"]) \
            .option("user", conn_props["user"]) \
            .option("password", conn_props["password"]) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("query", "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'atlas' ORDER BY TABLE_NAME") \
            .load()
        print("  Atlas tables: %d" % test_df.count())
        test_df.show(50, truncate=False)
    except Exception as e2:
        print("  Method 2 failed: %s" % str(e2)[:200])


# ── 2. Run the Atlas query ───────────────────────────────────────────────

section("2. ATLAS COURSE QUERY")

QUERY_TABLES = "(SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'atlas' ORDER BY TABLE_NAME) AS t"

QUERY = """(
    SELECT
        acs.section_id,
        acs.course_id,
        acs.section_number,
        acs.start_date AS section_start_date,
        acs.end_date AS section_end_date,
        ac.course_name,
        ac.course_number,
        ac.course_description,
        aci.instructor_id,
        aci.instructor_name
    FROM atlas.atlas_course_section acs
    INNER JOIN atlas.atlas_course ac
        ON acs.course_id = ac.course_id
    LEFT OUTER JOIN atlas.atlas_course_instructor aci
        ON acs.section_id = aci.section_id
    WHERE EXISTS (
        SELECT 1
        FROM atlas.atlas_section_enrollment ase
        WHERE ase.section_id = acs.section_id
    )
) AS atlas_query"""

try:
    conn_props = glue_ctx.extract_jdbc_conf(CONN)
    url = conn_props.get("fullUrl", conn_props.get("url", ""))
    print("  Original URL: %s" % url[:80])

    import re as _re

    # Try multiple databases
    for db_name in [None, "Master", "IESApplicationDB"]:
        url_override = url
        if db_name:
            url_override = _re.sub(r'databaseName=\w+', 'databaseName=%s' % db_name, url)
        print("  Trying DB: %s" % (db_name or "(original)"))

        def run_query(query, label=""):
            return spark.read.format("jdbc") \
                .option("url", url_override) \
                .option("user", conn_props["user"]) \
                .option("password", conn_props["password"]) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .option("dbtable", query) \
                .load()

        # Step A: List all databases
        try:
            db_df = run_query("(SELECT name FROM sys.databases) AS dbs")
            print("  Databases:")
            db_df.show(20, truncate=False)
        except Exception as e:
            print("  sys.databases failed: %s" % str(e)[:100])

        # Step B: List schemas with 'atlas'
        try:
            schema_df = run_query(QUERY_TABLES)
            cnt = schema_df.count()
            print("  Atlas tables in this DB: %d" % cnt)
            if cnt > 0:
                schema_df.show(50, truncate=False)

                # Step C: Run the actual query
                try:
                    df = run_query(QUERY)
                    print("  Query succeeded! Rows: %d" % df.count())
                    break
                except Exception as qe:
                    print("  Atlas query failed: %s" % str(qe)[:200])
                    # Try simpler query
                    try:
                        simple = "(SELECT TOP 100 * FROM atlas.atlas_course_section) AS t"
                        df = run_query(simple)
                        print("  Simple query columns: %s" % df.columns)
                        df.show(5, truncate=False)
                    except Exception as se:
                        print("  Simple query also failed: %s" % str(se)[:200])
            else:
                print("  No atlas tables — trying next DB")
                continue
        except Exception as e:
            print("  INFORMATION_SCHEMA query failed: %s" % str(e)[:150])
            continue

        break

    total = df.count()
    print("  Records: %d" % total)
    print("  Columns (%d): %s" % (len(df.columns), df.columns))
    df.show(20, truncate=False)

    # Save to S3
    section("3. SAVING TO S3")

    csv_path = "s3://%s/atlas_query/atlas_course_sections_csv/" % OUTPUT_BUCKET
    df.coalesce(4).write.mode("overwrite").option("header", "true").csv(csv_path)
    print("  CSV: %s" % csv_path)

    parquet_path = "s3://%s/atlas_query/atlas_course_sections/" % OUTPUT_BUCKET
    df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
    print("  Parquet: %s" % parquet_path)

    print("  TOTAL: %d rows saved" % total)

except Exception as e:
    print("  QUERY FAILED: %s" % e)

section("DONE")
job.commit()
