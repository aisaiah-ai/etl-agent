"""AWS Glue job: Audit → Sync → Verify SF↔Moodle ID mappings.

Reads SF data via Glue Salesforce connector, syncs to Moodle, then
re-reads Moodle to verify the updates stuck.

Phases:
    1. READ  — Fetch SF CourseOffering + Account via Glue connector
    2. AUDIT — Check Moodle state for each SF record (empty/match/conflict/not_found)
    3. SYNC  — Update empty idnumbers (or force-overwrite with --force true)
    4. VERIFY — Re-read Moodle and confirm idnumbers match SF IDs

Glue job arguments:
    --sf_connection_name   Glue Salesforce connection name
    --moodle_url           Moodle site URL
    --moodle_token         Moodle web-service token (direct, for sandbox testing)
    --mode                 "audit" (read-only) | "sync" (update+verify) | "verify" (re-check only)
    --entity               "courses" | "users" | "both" (default: both)
    --force                "true" to overwrite existing idnumbers (default: "false")
    --dry_run              "true" to match only, no updates (default: "false")
    --output_bucket        S3 bucket for results (optional)
"""

import json
import logging
import sys
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync_test")


# ── Glue boilerplate ──────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "sf_connection_name",
        "moodle_url",
        "moodle_token",
        "mode",
        "entity",
        "force",
        "dry_run",
        "output_bucket",
    ],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

mode = args.get("mode", "audit")  # audit | sync | verify
entity = args.get("entity", "both")  # courses | users | both
force = args.get("force", "false").lower() == "true"
dry_run = args.get("dry_run", "false").lower() == "true"
output_bucket = args.get("output_bucket", "")
if output_bucket == "none":
    output_bucket = ""

SF_CONN = args["sf_connection_name"]
MOODLE_URL = args["moodle_url"]
MOODLE_TOKEN = args["moodle_token"]

if dry_run:
    logger.info("DRY RUN — no Moodle updates will be made")


# ── Helpers ───────────────────────────────────────────────────────────────

def read_sf(entity_name, query):
    """Read a Salesforce object via Glue connector."""
    logger.info("Reading SF %s ...", entity_name)
    try:
        dyf = glue_ctx.create_dynamic_frame.from_options(
            connection_type="salesforce",
            connection_options={
                "connectionName": SF_CONN,
                "ENTITY_NAME": entity_name,
                "API_VERSION": "v60.0",
                "QUERY": query,
            },
        )
        df = dyf.toDF()
        records = [row.asDict() for row in df.collect()]
        logger.info("  Read %d %s records", len(records), entity_name)
        return records
    except Exception as e:
        logger.error("  FAILED to read %s: %s", entity_name, e)
        return []


def print_section(title):
    logger.info("")
    logger.info("=" * 70)
    logger.info("  %s", title)
    logger.info("=" * 70)


def print_audit(report):
    """Log audit report summary + details."""
    logger.info("  Total SF records:        %d", report.total_sf)
    logger.info("  Matched in Moodle:       %d", report.matched)
    logger.info("  Not found in Moodle:     %d", report.not_found)
    logger.info("  Empty idnumber (to set): %d", report.empty_idnumber)
    logger.info("  Already correct:         %d", report.already_correct)
    logger.info("  Conflict (different ID): %d", report.conflict)

    for rec in report.records:
        moodle_label = f"Moodle#{rec.moodle_id}" if rec.moodle_id else "N/A"
        logger.info(
            "    [%s] SF %s  %-40s  %s  %s",
            rec.status.upper(), rec.sf_id, rec.sf_name[:40], moodle_label, rec.detail,
        )


def report_to_dict(report):
    return {
        "entity_type": report.entity_type,
        "total_sf": report.total_sf,
        "matched": report.matched,
        "not_found": report.not_found,
        "empty_idnumber": report.empty_idnumber,
        "already_correct": report.already_correct,
        "conflict": report.conflict,
        "records": [r.__dict__ for r in report.records],
    }


# ── Import sync modules (bundled via --extra-py-files) ────────────────────

from pipelines.sf_moodle_sync.moodle_client import MoodleClient
from pipelines.sf_moodle_sync.verify_sync import (
    audit_courses,
    audit_users,
    sync_from_audit,
    verify_sync,
)

moodle = MoodleClient(base_url=MOODLE_URL, token=MOODLE_TOKEN)

results = {}

# ── Course Sync ───────────────────────────────────────────────────────────

if entity in ("courses", "both"):
    print_section("COURSE SYNC: SF Course Offering ID → Moodle Course ID Number")

    sf_offerings = read_sf("CourseOffering", "SELECT Id, Name FROM CourseOffering")

    if sf_offerings:
        # Phase 1: AUDIT
        print_section("PHASE 1: AUDIT COURSES")
        course_report = audit_courses(sf_offerings, moodle)
        print_audit(course_report)
        results["course_audit"] = report_to_dict(course_report)

        # Phase 2: SYNC
        if mode == "sync" and not dry_run:
            print_section("PHASE 2: SYNC COURSES")
            if course_report.conflict > 0 and not force:
                logger.info(
                    "  WARNING: %d courses have conflicting idnumbers — SKIPPING (use --force true)",
                    course_report.conflict,
                )
            sync_result = sync_from_audit(course_report, moodle, force=force)
            logger.info(
                "  Sync result: %d updated, %d conflicts skipped",
                sync_result["updated"], sync_result["skipped_conflicts"],
            )
            if sync_result.get("errors"):
                logger.error("  Errors: %s", sync_result["errors"])
            results["course_sync"] = sync_result

            # Phase 3: VERIFY
            print_section("PHASE 3: VERIFY COURSES")
            time.sleep(2)  # Give Moodle a moment
            verify_result = verify_sync(course_report, moodle)
            logger.info(
                "  Verification: %d/%d correct",
                verify_result["verified"], verify_result["total_checked"],
            )
            if verify_result["mismatches"]:
                logger.error("  MISMATCHES:")
                for mm in verify_result["mismatches"]:
                    logger.error(
                        "    Moodle#%s: expected '%s', got '%s'",
                        mm["moodle_id"], mm["expected"], mm["actual"],
                    )
            results["course_verify"] = verify_result

        elif mode == "verify":
            print_section("VERIFY COURSES (re-read only)")
            verify_result = verify_sync(course_report, moodle)
            logger.info(
                "  Verification: %d/%d correct",
                verify_result["verified"], verify_result["total_checked"],
            )
            results["course_verify"] = verify_result
    else:
        logger.warning("No SF Course Offerings found — skipping course sync")

# ── User Sync ─────────────────────────────────────────────────────────────

if entity in ("users", "both"):
    print_section("USER SYNC: SF Account ID → Moodle User ID Number")

    sf_accounts = read_sf(
        "Account",
        "SELECT Id, Name, PersonEmail FROM Account WHERE IsPersonAccount = true",
    )

    if sf_accounts:
        # Phase 1: AUDIT
        print_section("PHASE 1: AUDIT USERS")
        user_report = audit_users(sf_accounts, moodle)
        print_audit(user_report)
        results["user_audit"] = report_to_dict(user_report)

        # Phase 2: SYNC
        if mode == "sync" and not dry_run:
            print_section("PHASE 2: SYNC USERS")
            if user_report.conflict > 0 and not force:
                logger.info(
                    "  WARNING: %d users have conflicting idnumbers — SKIPPING (use --force true)",
                    user_report.conflict,
                )
            sync_result = sync_from_audit(user_report, moodle, force=force)
            logger.info(
                "  Sync result: %d updated, %d conflicts skipped",
                sync_result["updated"], sync_result["skipped_conflicts"],
            )
            if sync_result.get("errors"):
                logger.error("  Errors: %s", sync_result["errors"])
            results["user_sync"] = sync_result

            # Phase 3: VERIFY
            print_section("PHASE 3: VERIFY USERS")
            time.sleep(2)
            verify_result = verify_sync(user_report, moodle)
            logger.info(
                "  Verification: %d/%d correct",
                verify_result["verified"], verify_result["total_checked"],
            )
            if verify_result["mismatches"]:
                logger.error("  MISMATCHES:")
                for mm in verify_result["mismatches"]:
                    logger.error(
                        "    Moodle#%s: expected '%s', got '%s'",
                        mm["moodle_id"], mm["expected"], mm["actual"],
                    )
            results["user_verify"] = verify_result

        elif mode == "verify":
            print_section("VERIFY USERS (re-read only)")
            verify_result = verify_sync(user_report, moodle)
            logger.info(
                "  Verification: %d/%d correct",
                verify_result["verified"], verify_result["total_checked"],
            )
            results["user_verify"] = verify_result
    else:
        logger.warning("No SF Person Accounts found — skipping user sync")

# ── Write results to S3 ──────────────────────────────────────────────────

if output_bucket:
    print_section("WRITING RESULTS TO S3")
    result_json = json.dumps(results, indent=2, default=str)
    spark.createDataFrame([{"result": result_json}]).coalesce(1).write.mode("overwrite").text(
        f"s3://{output_bucket}/sf_moodle_sync/test_results/"
    )
    logger.info("  Results written to s3://%s/sf_moodle_sync/test_results/", output_bucket)

# ── Final Summary ─────────────────────────────────────────────────────────

print_section("FINAL SUMMARY")
logger.info("  Mode: %s | Entity: %s | Force: %s | Dry run: %s", mode, entity, force, dry_run)
for key, val in results.items():
    if isinstance(val, dict):
        logger.info("  %s: %s", key, {k: v for k, v in val.items() if k != "records"})

# Check for failures
has_mismatches = False
for key in ("course_verify", "user_verify"):
    v = results.get(key, {})
    if v.get("mismatches"):
        has_mismatches = True
        logger.error("  FAIL: %s has %d mismatches", key, len(v["mismatches"]))

if has_mismatches:
    logger.error("  RESULT: FAILED — some idnumbers did not stick")
else:
    logger.info("  RESULT: PASSED")

job.commit()
