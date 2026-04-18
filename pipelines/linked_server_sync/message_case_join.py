"""AWS Glue job — Join tblMessage + tblUser + tblCustomer + tblCase into one table.

Reads the 4 extracted parquet tables from S3 and produces a single
denormalized table: tlmain_message_case_detail.

This replaces the original SQL Server linked-server query:

    SELECT m.aEventID, m.nGlobalCaseID,
           CASE WHEN bIncomingOrOutgoing=0 THEN u.tName ELSE cust.tName END ...
    FROM [tlMain287].[dbo].[tblMessage] m
    LEFT JOIN tblUser u ON u.aUserID = m.nOriginatorID
    LEFT JOIN tblCustomer cust ON cust.aCustID = m.nOriginatorID
    LEFT JOIN tblCase c ON m.nGlobalCaseID = c.aGlobalCaseID
    WHERE m.nGlobalCaseID != 0 AND year(c.dCreatedAt) >= 2026

Glue job parameters:
    --JOB_NAME       Glue job name
    --ENV            Environment: uat / prd / prod_replica
"""

import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, year

logger = logging.getLogger(__name__)

TABLE_PREFIX = "tlmain"
S3_BUCKET = "ies-devteam-prd"
S3_PREFIX = "gluecatalog"
MIN_YEAR = 2026


def main():
    params = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV"])

    sc = SparkContext()
    context = GlueContext(sc)
    spark = context.spark_session
    job = Job(context)
    job.init(params["JOB_NAME"], params)

    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    # ── Read source tables from S3 ───────────────────────────────────────
    prefix = f"prd_glue_{TABLE_PREFIX}"

    def read_table(name):
        path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{prefix}_{name}"
        logger.info(f"Reading {path}")
        return spark.read.parquet(path)

    msg = read_table("tblmessage")
    usr = read_table("tbluser")
    cust = read_table("tblcustomer")
    case = read_table("tblcase")

    logger.info(
        f"Source counts — messages: {msg.count()}, users: {usr.count()}, "
        f"customers: {cust.count()}, cases: {case.count()}"
    )

    # ── Join ──────────────────────────────────────────────────────────────
    joined = (
        msg.alias("m")
        .join(usr.alias("u"), col("m.nOriginatorID") == col("u.aUserID"), "left")
        .join(cust.alias("cust"), col("m.nOriginatorID") == col("cust.aCustID"), "left")
        .join(case.alias("c"), col("m.nGlobalCaseID") == col("c.aGlobalCaseID"), "left")
        .where(col("m.nGlobalCaseID") != 0)
        .where(year(col("c.dCreatedAt")) >= MIN_YEAR)
        .select(
            col("m.aEventID").alias("message_ext_id"),
            col("m.nGlobalCaseID"),
            when(col("m.bIncomingOrOutgoing") == 0, col("u.tName"))
            .when(col("m.bIncomingOrOutgoing") == 1, col("cust.tName"))
            .alias("from_name"),
            when(col("m.bIncomingOrOutgoing") == 0, col("u.tEmail"))
            .when(col("m.bIncomingOrOutgoing") == 1, col("cust.tEmail"))
            .alias("from_email"),
            col("m.tTo"),
            col("m.tCc"),
            col("m.tBcc"),
            col("m.tSubject"),
            col("m.mMsgContent"),
            col("m.dCDServerTime").alias("message_datetime"),
            col("m.nOriginatorID"),
            col("m.tMailHeader"),
            col("m.bIncomingOrOutgoing"),
            col("m.nStatus").alias("message_status"),
            col("c.nCaseState"),
            col("m.nAliasID"),
            col("m.nMsgType"),
            col("c.nCreatedFromMedia"),
            col("c.nQueueID"),
            col("c.nOwnerID"),
        )
    )

    row_count = joined.count()
    logger.info(f"Join produced {row_count} rows")

    # ── Write to S3 as parquet ────────────────────────────────────────────
    output_table = f"prd_glue_{TABLE_PREFIX}_message_case_detail"
    s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{output_table}"
    joined.write.mode("overwrite").format("parquet").save(s3_path)
    logger.info(f"Wrote {row_count} rows to {s3_path}")

    job.commit()


if __name__ == "__main__":
    main()
