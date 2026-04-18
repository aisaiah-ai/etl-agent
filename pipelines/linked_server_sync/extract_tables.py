"""AWS Glue job — Extract tables from SQL Server (tlMain287) via JDBC into Glue catalog.

Reads tblMessage, tblUser, tblCustomer, tblCase from the tlMain287 linked server
(via SQLCUST1) and writes each as a Delta table registered in the Glue Data Catalog.

Glue job parameters:
    --JOB_NAME       Glue job name
    --ENV            Environment: uat / prd / prod_replica
"""

import sys
import json
import logging
from datetime import date, timedelta

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logger = logging.getLogger(__name__)

REGION_NAME = "us-east-1"

ENV_CONFIG = {
    "uat": {
        "jdbc_url": "jdbc:sqlserver://aws-uat-db.iesabroad.org:1433;databaseName=IESApplicationDB",
        "secret_arn": "arn:aws:secretsmanager:us-east-1:442594162630:secret:glue.uat-hBSzBD",
        "secret_username_str": "chronus.db.username",
        "secret_password_str": "chronus.db.password",
    },
    "prd": {
        "jdbc_url": "jdbc:sqlserver://aws-prod-db.iesabroad.org:1433;databaseName=IESApplicationDB",
        "secret_arn": "arn:aws:secretsmanager:us-east-1:442594162630:secret:glue.prd-lUsr9F",
        "secret_username_str": "chronus_glue.db.username",
        "secret_password_str": "chronus_glue.db.password",
    },
    "prod": {
        "jdbc_url": "jdbc:sqlserver://aws-prod-db.iesabroad.org:1433;databaseName=IESApplicationDB",
        "secret_arn": "arn:aws:secretsmanager:us-east-1:442594162630:secret:glue.prd-lUsr9F",
        "secret_username_str": "chronus_glue.db.username",
        "secret_password_str": "chronus_glue.db.password",
    },
    "prod_replica": {
        "jdbc_url": "jdbc:sqlserver://aws-prod-db-replica.iesabroad.org:1433;databaseName=IESApplicationDB",
        "secret_arn": "arn:aws:secretsmanager:us-east-1:442594162630:secret:glue.prd-lUsr9F",
        "secret_username_str": "chronus_glue.db.username",
        "secret_password_str": "chronus_glue.db.password",
    },
}

# Linked server path: SQLCUST1 -> tlMain287 database -> dbo schema
LINKED_DB = "SQLCUST1.tlMain287.dbo"
MIN_YEAR = 2026

TABLE_PREFIX = "tlmain"
S3_BUCKET = "ies-devteam-prd"
S3_PREFIX = "gluecatalog"

# tblMessage extraction in 5-day chunks to avoid JDBC timeout
MSG_COLUMNS = """m.aEventID, m.nGlobalCaseID, m.tTo, m.tCc, m.tBcc, m.tSubject,
               m.mMsgContent, m.dCDServerTime, m.nOriginatorID, m.tMailHeader,
               m.bIncomingOrOutgoing, m.nStatus, m.nAliasID, m.nMsgType"""

CASE_COLUMNS = """c.aGlobalCaseID, c.nCaseState, c.nCreatedFromMedia,
               c.nQueueID, c.dCreatedAt, c.nOwnerID"""

CHUNK_DAYS = 5
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 3, 14)  # today + 1 to capture all of today


# ── Main ──────────────────────────────────────────────────────────────────


def main():
    params = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV"])
    try:
        extra = getResolvedOptions(sys.argv, ["TABLES"])
        params.update(extra)
    except Exception:
        pass
    env = params["ENV"]
    tables = [t.strip() for t in params.get("TABLES", "all").split(",")]

    if env not in ENV_CONFIG:
        raise ValueError(
            f"Invalid environment: {env}. Must be one of: {', '.join(ENV_CONFIG.keys())}"
        )

    config = ENV_CONFIG[env]

    sc = SparkContext()
    context = GlueContext(sc)
    spark = context.spark_session

    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    job = Job(context)
    job.init(params["JOB_NAME"], params)

    # Get credentials from Secrets Manager
    session = boto3.session.Session()
    sm_client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
    secret_resp = sm_client.get_secret_value(SecretId=config["secret_arn"])
    secret = json.loads(secret_resp["SecretString"])
    username = secret[config["secret_username_str"]]
    password = secret[config["secret_password_str"]]
    jdbc_url = config["jdbc_url"]
    logger.info(f"Connected to {jdbc_url} as {username}")

    jdbc_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "fetchsize": "5000",
    }

    # Extract tblMessage in 5-day chunks, append to same S3 path
    if "all" in tables or "tblmessage" in tables:
        s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/prd_glue_{TABLE_PREFIX}_tblmessage"
        total_rows = 0
        first_chunk = True
        chunk_start = START_DATE

        while chunk_start < END_DATE:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), END_DATE)
            db_table = f"""(
                SELECT {MSG_COLUMNS}
                FROM {LINKED_DB}.tblMessage m
                WHERE m.nGlobalCaseID != 0
                  AND m.dCDServerTime >= '{chunk_start}'
                  AND m.dCDServerTime < '{chunk_end}'
            ) AS t"""

            logger.info(f"Extracting tblMessage chunk: {chunk_start} to {chunk_end}...")

            df = spark.read.jdbc(
                url=jdbc_url,
                table=db_table,
                properties=jdbc_properties,
            )
            row_count = df.count()
            total_rows += row_count
            logger.info(f"  chunk {chunk_start}–{chunk_end}: {row_count} rows")

            mode = "overwrite" if first_chunk else "append"
            df.write.mode(mode).format("parquet").save(s3_path)
            logger.info(f"  -> wrote ({mode}) to {s3_path}")

            first_chunk = False
            chunk_start = chunk_end

        logger.info(f"tblMessage extraction complete: {total_rows} total rows")
    else:
        logger.info("Skipping tblMessage (not in --TABLES)")

    # Extract tblCase (small table, no chunking needed)
    if "all" in tables or "tblcase" in tables:
        case_s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/prd_glue_{TABLE_PREFIX}_tblcase"
        case_query = f"""(
            SELECT {CASE_COLUMNS}
            FROM {LINKED_DB}.tblCase c
            WHERE YEAR(c.dCreatedAt) >= {MIN_YEAR}
        ) AS t"""

        logger.info("Extracting tblCase...")
        case_df = spark.read.jdbc(url=jdbc_url, table=case_query, properties=jdbc_properties)
        case_count = case_df.count()
        logger.info(f"  tblCase: {case_count} rows")
        case_df.write.mode("overwrite").format("parquet").save(case_s3_path)
        logger.info(f"  -> wrote to {case_s3_path}")
    else:
        logger.info("Skipping tblCase (not in --TABLES)")

    job.commit()


if __name__ == "__main__":
    main()
