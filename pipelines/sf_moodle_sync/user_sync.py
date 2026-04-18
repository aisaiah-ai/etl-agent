"""Sync Salesforce Account IDs to Moodle User ID Numbers.

Match logic:
    SF Account (Person Account)  →  Moodle User
    - Match by SF PersonEmail ↔ Moodle email
    - Write SF Account ID → Moodle user ``idnumber`` field

Can run as:
    1. A standalone script (python3 -m pipelines.sf_moodle_sync.user_sync)
    2. An AWS Glue job (reads SF via Glue connector)
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
from dataclasses import dataclass

from .moodle_client import MoodleClient

logger = logging.getLogger(__name__)


@dataclass
class UserMatch:
    sf_account_id: str
    sf_email: str
    sf_name: str
    moodle_user_id: int
    moodle_email: str
    moodle_username: str


@dataclass
class UserSyncResult:
    matched: list[UserMatch]
    unmatched_sf: list[dict]  # SF accounts with no Moodle match
    already_set: list[UserMatch]  # Moodle idnumber already matches
    updated: list[UserMatch]
    errors: list[dict]


def match_users_by_email(
    sf_accounts: list[dict],
    moodle: MoodleClient,
    batch_size: int = 50,
) -> UserSyncResult:
    """Match SF accounts to Moodle users by email.

    Parameters
    ----------
    sf_accounts : list[dict]
        Each dict must have ``Id``, ``PersonEmail``, and optionally ``Name``.
    moodle : MoodleClient
        Authenticated Moodle client.
    batch_size : int
        Number of emails to look up per Moodle API call.
    """
    result = UserSyncResult(
        matched=[], unmatched_sf=[], already_set=[], updated=[], errors=[]
    )

    # Deduplicate by email
    by_email: dict[str, dict] = {}
    for sf in sf_accounts:
        email = (sf.get("PersonEmail") or sf.get("Email") or "").strip().lower()
        if email:
            by_email[email] = sf
        else:
            result.unmatched_sf.append({**sf, "_reason": "no_email"})

    emails = list(by_email.keys())
    logger.info("Looking up %d unique emails in Moodle...", len(emails))

    for i in range(0, len(emails), batch_size):
        batch_emails = emails[i : i + batch_size]

        for email in batch_emails:
            sf = by_email[email]
            try:
                moodle_users = moodle.get_users_by_field(field="email", values=[email])
            except Exception as e:
                logger.error("Moodle lookup failed for %s: %s", email, e)
                result.unmatched_sf.append({**sf, "_reason": f"api_error: {e}"})
                continue

            if not moodle_users:
                result.unmatched_sf.append({**sf, "_reason": "not_found"})
                continue

            mu = moodle_users[0]  # Take first match
            match = UserMatch(
                sf_account_id=sf.get("Id", ""),
                sf_email=email,
                sf_name=sf.get("Name", ""),
                moodle_user_id=mu["id"],
                moodle_email=mu.get("email", ""),
                moodle_username=mu.get("username", ""),
            )
            result.matched.append(match)

            if mu.get("idnumber", "") == sf.get("Id", ""):
                result.already_set.append(match)

    return result


def sync_users(
    sf_accounts: list[dict],
    moodle: MoodleClient,
    dry_run: bool = False,
    batch_size: int = 50,
) -> UserSyncResult:
    """Full sync: look up Moodle users by email, update idnumber with SF Account ID.

    Parameters
    ----------
    sf_accounts : list[dict]
        SF Account records with ``Id``, ``PersonEmail``, and optionally ``Name``.
    moodle : MoodleClient
        Authenticated Moodle client.
    dry_run : bool
        If True, match only — do not update Moodle.
    batch_size : int
        Batch size for Moodle API calls.
    """
    result = match_users_by_email(sf_accounts, moodle, batch_size=batch_size)
    logger.info(
        "Matched %d, unmatched %d, already set %d",
        len(result.matched),
        len(result.unmatched_sf),
        len(result.already_set),
    )

    if dry_run:
        logger.info("Dry run — skipping updates")
        return result

    to_update = [m for m in result.matched if m not in result.already_set]
    if not to_update:
        logger.info("No users need updating")
        return result

    update_batch_size = 50
    for i in range(0, len(to_update), update_batch_size):
        batch = to_update[i : i + update_batch_size]
        updates = [
            {"id": m.moodle_user_id, "idnumber": m.sf_account_id} for m in batch
        ]
        try:
            moodle.update_users(updates)
            result.updated.extend(batch)
            logger.info("Updated batch %d–%d", i + 1, i + len(batch))
        except Exception as e:
            logger.error("Failed to update batch %d–%d: %s", i + 1, i + len(batch), e)
            for m in batch:
                result.errors.append({"match": m.__dict__, "error": str(e)})

    return result


def load_sf_accounts_from_file(path: str) -> list[dict]:
    """Load SF accounts from a JSON file (for testing or one-time runs)."""
    data = json.loads(pathlib.Path(path).read_text())
    if isinstance(data, list):
        return data
    return data.get("records", data.get("accounts", []))


# ---------------------------------------------------------------------------
# Glue job entry point
# ---------------------------------------------------------------------------

def run_glue_user_sync():
    """Entry point when running as an AWS Glue job."""
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "sf_connection_name",
            "moodle_url",
            "moodle_secret_name",
            "dry_run",
        ],
    )

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Read SF Accounts (Person Accounts) via Glue connector
    sf_dyf = glue_ctx.create_dynamic_frame.from_options(
        connection_type="salesforce",
        connection_options={
            "connectionName": args["sf_connection_name"],
            "ENTITY_NAME": "Account",
            "API_VERSION": "v60.0",
            "QUERY": "SELECT Id, Name, PersonEmail FROM Account WHERE IsPersonAccount = true",
        },
    )
    sf_df = sf_dyf.toDF()
    sf_accounts = [row.asDict() for row in sf_df.collect()]
    logger.info("Read %d SF Person Accounts", len(sf_accounts))

    moodle = MoodleClient(
        base_url=args["moodle_url"],
        secret_name=args["moodle_secret_name"],
    )

    result = sync_users(
        sf_accounts,
        moodle,
        dry_run=args.get("dry_run", "false").lower() == "true",
    )

    print(f"User sync complete: {len(result.updated)} updated, "
          f"{len(result.unmatched_sf)} unmatched, {len(result.errors)} errors")

    job.commit()
