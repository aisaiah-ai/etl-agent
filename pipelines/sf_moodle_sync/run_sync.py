"""Standalone runner for SF→Moodle sync (no Glue dependency).

Reads SF data from JSON export files or directly via Salesforce REST API,
then syncs to Moodle via REST API.

Usage:
    # Dry run (match only, no updates)
    python3 -m pipelines.sf_moodle_sync.run_sync --dry-run

    # From JSON files (exported from SF)
    python3 -m pipelines.sf_moodle_sync.run_sync \
        --sf-courses output/sf_courses.json \
        --sf-accounts output/sf_accounts.json

    # Live sync
    python3 -m pipelines.sf_moodle_sync.run_sync

Environment variables:
    MOODLE_URL          Moodle site URL
    MOODLE_TOKEN        Moodle web-service token
    SF_INSTANCE_URL     Salesforce instance URL (for direct API access)
    SF_ACCESS_TOKEN     Salesforce OAuth access token (for direct API access)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import sys

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_sf_records(object_name: str, fields: list[str], where: str = "") -> list[dict]:
    """Fetch records from Salesforce REST API (SOQL query)."""
    instance_url = os.environ.get("SF_INSTANCE_URL", "").rstrip("/")
    access_token = os.environ.get("SF_ACCESS_TOKEN", "")
    if not instance_url or not access_token:
        raise ValueError(
            "Set SF_INSTANCE_URL and SF_ACCESS_TOKEN env vars for direct SF access"
        )

    soql = f"SELECT {','.join(fields)} FROM {object_name}"
    if where:
        soql += f" WHERE {where}"

    url = f"{instance_url}/services/data/v60.0/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    records: list[dict] = []

    resp = requests.get(url, headers=headers, params={"q": soql}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    records.extend(data.get("records", []))

    # Handle pagination
    while data.get("nextRecordsUrl"):
        resp = requests.get(
            f"{instance_url}{data['nextRecordsUrl']}", headers=headers, timeout=120
        )
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))

    logger.info("Fetched %d %s records from Salesforce", len(records), object_name)
    return records


def main():
    parser = argparse.ArgumentParser(description="SF → Moodle ID Sync")
    parser.add_argument("--dry-run", action="store_true", help="Match only, no updates")
    parser.add_argument("--courses-only", action="store_true", help="Only sync courses")
    parser.add_argument("--users-only", action="store_true", help="Only sync users")
    parser.add_argument("--sf-courses", help="Path to SF course offerings JSON file")
    parser.add_argument("--sf-accounts", help="Path to SF accounts JSON file")
    parser.add_argument("--output-dir", default="output/sf_moodle_sync", help="Output directory")
    args = parser.parse_args()

    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    from .moodle_client import MoodleClient

    moodle = MoodleClient()

    do_courses = not args.users_only
    do_users = not args.courses_only

    # ── Course Sync ──────────────────────────────────────────────────
    if do_courses:
        from .course_sync import load_sf_offerings_from_file, sync_courses

        logger.info("=" * 60)
        logger.info("COURSE SYNC: SF Course Offering ID → Moodle Course ID Number")
        logger.info("=" * 60)

        if args.sf_courses:
            sf_offerings = load_sf_offerings_from_file(args.sf_courses)
        else:
            sf_offerings = fetch_sf_records(
                "CourseOffering", ["Id", "Name"]
            )

        result = sync_courses(sf_offerings, moodle, dry_run=args.dry_run)

        summary = {
            "matched": len(result.matched),
            "unmatched_sf": len(result.unmatched_sf),
            "already_set": len(result.already_set),
            "updated": len(result.updated),
            "errors": len(result.errors),
            "matched_details": [m.__dict__ for m in result.matched],
            "unmatched_details": result.unmatched_sf,
            "error_details": result.errors,
        }
        (output_dir / "course_sync_result.json").write_text(
            json.dumps(summary, indent=2)
        )
        logger.info(
            "Course sync: %d matched, %d updated, %d unmatched, %d errors",
            summary["matched"], summary["updated"],
            summary["unmatched_sf"], summary["errors"],
        )

    # ── User Sync ────────────────────────────────────────────────────
    if do_users:
        from .user_sync import load_sf_accounts_from_file, sync_users

        logger.info("=" * 60)
        logger.info("USER SYNC: SF Account ID → Moodle User ID Number")
        logger.info("=" * 60)

        if args.sf_accounts:
            sf_accounts = load_sf_accounts_from_file(args.sf_accounts)
        else:
            sf_accounts = fetch_sf_records(
                "Account",
                ["Id", "Name", "PersonEmail"],
                where="IsPersonAccount = true AND PersonEmail != null",
            )

        result = sync_users(sf_accounts, moodle, dry_run=args.dry_run)

        summary = {
            "matched": len(result.matched),
            "unmatched_sf": len(result.unmatched_sf),
            "already_set": len(result.already_set),
            "updated": len(result.updated),
            "errors": len(result.errors),
            "matched_details": [m.__dict__ for m in result.matched],
            "unmatched_details": result.unmatched_sf,
            "error_details": result.errors,
        }
        (output_dir / "user_sync_result.json").write_text(
            json.dumps(summary, indent=2)
        )
        logger.info(
            "User sync: %d matched, %d updated, %d unmatched, %d errors",
            summary["matched"], summary["updated"],
            summary["unmatched_sf"], summary["errors"],
        )

    logger.info("Results written to %s/", output_dir)


if __name__ == "__main__":
    main()
