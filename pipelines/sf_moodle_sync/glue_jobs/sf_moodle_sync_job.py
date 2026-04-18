"""AWS Glue job: SF → Moodle ID Sync Pipeline.

Production pipeline that syncs Salesforce IDs to Moodle idnumber fields:
    1. COURSES  — SF CourseOffering.Id → Moodle course idnumber
    2. USERS    — SF Account.Id (Person Account) → Moodle user idnumber
                  (covers both students and faculty — all Person Accounts)

Each sync phase runs: READ (SF) → AUDIT (Moodle) → SYNC (update) → VERIFY (re-read).

Glue job arguments:
    --sf_connection_name   Glue Salesforce connection name
    --moodle_url           Moodle site URL
    --moodle_token         Moodle web-service token (for sandbox/dev)
    --moodle_secret_name   Secrets Manager secret name (for production)
    --mode                 "audit" (read-only) | "sync" (update+verify)
    --entity               "courses" | "users" | "all" (default: all)
    --force                "true" to overwrite existing idnumbers (default: "false")
    --dry_run              "true" to match only, no updates (default: "false")
    --output_bucket        S3 bucket for results (optional)
"""

import json
import logging
import sys
import time
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sf_moodle_sync")


# ── Glue boilerplate ──────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "sf_connection_name",
        "moodle_url",
        "mode",
        "entity",
        "force",
        "dry_run",
        "output_bucket",
    ],
)

# Optional args (token OR secret_name)
for opt in ("moodle_token", "moodle_secret_name"):
    try:
        extra = getResolvedOptions(sys.argv, [opt])
        args.update(extra)
    except Exception:
        pass

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

mode = args.get("mode", "audit")
entity = args.get("entity", "all")
force = args.get("force", "false").lower() == "true"
dry_run = args.get("dry_run", "false").lower() == "true"
output_bucket = args.get("output_bucket", "")
if output_bucket in ("none", ""):
    output_bucket = ""

SF_CONN = args["sf_connection_name"]
MOODLE_URL = args["moodle_url"]
MOODLE_TOKEN = args.get("moodle_token", "")
MOODLE_SECRET = args.get("moodle_secret_name", "")

RUN_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Helpers ───────────────────────────────────────────────────────────────

def section(title):
    logger.info("")
    logger.info("=" * 70)
    logger.info("  %s", title)
    logger.info("=" * 70)


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


def log_audit(report):
    """Log audit report summary."""
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


def run_sync_phase(report, moodle, phase_name):
    """Run sync + verify for an audit report. Returns dict with results."""
    phase_results = {}

    # Audit
    log_audit(report)
    phase_results["audit"] = report_to_dict(report)

    # Sync
    if mode == "sync" and not dry_run:
        section(f"SYNC {phase_name}")
        if report.conflict > 0 and not force:
            logger.info(
                "  WARNING: %d records have conflicting idnumbers — SKIPPING (use --force true)",
                report.conflict,
            )
        sync_result = sync_from_audit(report, moodle, force=force)
        logger.info(
            "  Sync: %d updated, %d conflicts skipped",
            sync_result["updated"], sync_result["skipped_conflicts"],
        )
        if sync_result.get("errors"):
            logger.error("  Errors: %d", len(sync_result["errors"]))
            for err in sync_result["errors"]:
                logger.error("    %s", err)
        phase_results["sync"] = sync_result

        # Verify
        section(f"VERIFY {phase_name}")
        time.sleep(2)
        verify_result = verify_sync(report, moodle)
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
        phase_results["verify"] = verify_result

    return phase_results


# ── Import sync modules (bundled via --extra-py-files) ────────────────────

from pipelines.sf_moodle_sync.moodle_client import MoodleClient
from pipelines.sf_moodle_sync.verify_sync import (
    audit_courses,
    audit_users,
    sync_from_audit,
    verify_sync,
)

# Initialize Moodle client
moodle_kwargs = {"base_url": MOODLE_URL}
if MOODLE_TOKEN:
    moodle_kwargs["token"] = MOODLE_TOKEN
elif MOODLE_SECRET:
    moodle_kwargs["secret_name"] = MOODLE_SECRET
moodle = MoodleClient(**moodle_kwargs)

results = {"run_timestamp": RUN_TIMESTAMP, "mode": mode, "entity": entity}
do_courses = entity in ("courses", "all")
do_users = entity in ("users", "all")


# ── 1. COURSE SYNC ───────────────────────────────────────────────────────

if do_courses:
    section("COURSE SYNC: SF CourseOffering.Id → Moodle course idnumber")
    sf_offerings = read_sf("CourseOffering", "SELECT Id, Name FROM CourseOffering")

    if sf_offerings:
        section("AUDIT COURSES (with fuzzy + AI matching)")
        course_report = audit_courses(sf_offerings, moodle, use_ai=True)
        results["courses"] = run_sync_phase(course_report, moodle, "COURSES")
    else:
        logger.warning("No SF CourseOfferings found — skipping")
        results["courses"] = {"skipped": True, "reason": "no_sf_data"}


# ── 2. USER SYNC (students + faculty — all Person Accounts) ─────────────

if do_users:
    section("USER SYNC: SF Account.Id → Moodle user idnumber (students + faculty)")
    sf_accounts = read_sf(
        "Account",
        "SELECT Id, Name, PersonEmail FROM Account "
        "WHERE IsPersonAccount = true AND PersonEmail != null",
    )

    if sf_accounts:
        section("AUDIT USERS")
        user_report = audit_users(sf_accounts, moodle)
        results["users"] = run_sync_phase(user_report, moodle, "USERS")
    else:
        logger.warning("No SF Person Accounts found — skipping")
        results["users"] = {"skipped": True, "reason": "no_sf_data"}


# ── Write results to S3 ──────────────────────────────────────────────────

if output_bucket:
    section("WRITING RESULTS TO S3")
    result_json = json.dumps(results, indent=2, default=str)
    output_path = f"s3://{output_bucket}/sf_moodle_sync/runs/{RUN_TIMESTAMP.replace(':', '-')}/"
    spark.createDataFrame([{"result": result_json}]).coalesce(1).write.mode("overwrite").text(
        output_path
    )
    logger.info("  Results written to %s", output_path)


# ── Final Summary ─────────────────────────────────────────────────────────

section("FINAL SUMMARY")
logger.info("  Run:    %s", RUN_TIMESTAMP)
logger.info("  Mode:   %s | Entity: %s | Force: %s | Dry run: %s", mode, entity, force, dry_run)

total_updated = 0
total_errors = 0
has_mismatches = False

for phase in ("courses", "users"):
    data = results.get(phase, {})
    if data.get("skipped"):
        logger.info("  %s: SKIPPED (%s)", phase.upper(), data.get("reason", ""))
        continue

    audit = data.get("audit", {})
    sync = data.get("sync", {})
    verify = data.get("verify", {})

    updated = sync.get("updated", 0)
    errors = sync.get("errors", [])
    verified = verify.get("verified", 0)
    total_checked = verify.get("total_checked", 0)
    mismatches = verify.get("mismatches", [])

    total_updated += updated
    total_errors += len(errors) if isinstance(errors, list) else 0

    status = "PASS" if not mismatches else "FAIL"
    if mode == "audit":
        status = "AUDIT ONLY"
    if mismatches:
        has_mismatches = True

    logger.info(
        "  %s: matched=%d, updated=%d, errors=%d, verified=%d/%d — %s",
        phase.upper(),
        audit.get("matched", 0),
        updated,
        len(errors) if isinstance(errors, list) else 0,
        verified,
        total_checked,
        status,
    )

logger.info("")
logger.info("  TOTAL: %d updated, %d errors", total_updated, total_errors)
if has_mismatches:
    logger.error("  RESULT: FAILED — some idnumbers did not sync correctly")
else:
    logger.info("  RESULT: %s", "AUDIT COMPLETE" if mode == "audit" else "PASSED")

job.commit()
