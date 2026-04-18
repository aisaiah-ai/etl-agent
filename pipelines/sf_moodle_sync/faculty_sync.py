"""Sync Faculty SF Account IDs to Moodle User ID Numbers.

Resolves faculty from SF CourseOffering.PrimaryFacultyId (Contact ID)
through Contact → AccountId, then syncs to Moodle via email matching.

Flow:
    CourseOffering.PrimaryFacultyId (Contact 003...)
        → Contact.AccountId (Account 001...)
        → Contact.Email
        → Moodle user (match by email)
        → Write AccountId to Moodle idnumber

Usage:
    # From JSON files (exported from SF)
    python3 -m pipelines.sf_moodle_sync.faculty_sync \
        --offerings output/sf_offerings.json \
        --contacts output/sf_contacts.json

    # Dry run (no Moodle updates)
    python3 -m pipelines.sf_moodle_sync.faculty_sync \
        --offerings output/sf_offerings.json \
        --contacts output/sf_contacts.json --dry-run

Environment variables:
    MOODLE_URL      Moodle site URL
    MOODLE_TOKEN    Moodle web-service token
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
import sys
from dataclasses import dataclass, field

from .moodle_client import MoodleClient
from .user_sync import UserMatch, UserSyncResult, match_users_by_email

logger = logging.getLogger(__name__)


@dataclass
class FacultyRecord:
    """A faculty member resolved from CourseOffering → Contact → Account."""

    contact_id: str
    account_id: str
    name: str
    email: str
    offering_ids: list[str] = field(default_factory=list)


@dataclass
class FacultyResolution:
    """Result of resolving CourseOffering faculty to syncable accounts."""

    resolved: list[FacultyRecord]
    no_faculty: list[dict]  # Offerings with null PrimaryFacultyId
    no_contact: list[str]  # Contact IDs not found in contacts data
    no_email: list[FacultyRecord]  # Resolved but Contact has no email


def resolve_faculty(
    offerings: list[dict],
    contacts: list[dict],
) -> FacultyResolution:
    """Resolve CourseOffering faculty to SF Account records for syncing.

    Parameters
    ----------
    offerings : list[dict]
        SF CourseOffering records with ``Id``, ``Name``, ``PrimaryFacultyId``.
    contacts : list[dict]
        SF Contact records with ``Id``, ``AccountId``, ``Email``, ``Name``.
    """
    result = FacultyResolution(
        resolved=[], no_faculty=[], no_contact=[], no_email=[]
    )

    # Index contacts by Id
    contact_by_id: dict[str, dict] = {}
    for c in contacts:
        cid = c.get("Id", "")
        if cid:
            contact_by_id[cid] = c

    # Deduplicate faculty by Contact ID (a faculty can teach multiple offerings)
    faculty_by_contact: dict[str, FacultyRecord] = {}

    for off in offerings:
        faculty_id = off.get("PrimaryFacultyId") or ""
        if not faculty_id:
            result.no_faculty.append(off)
            continue

        if faculty_id in faculty_by_contact:
            faculty_by_contact[faculty_id].offering_ids.append(off.get("Id", ""))
            continue

        contact = contact_by_id.get(faculty_id)
        if not contact:
            result.no_contact.append(faculty_id)
            continue

        email = (contact.get("Email") or "").strip().lower()
        account_id = contact.get("AccountId", "")
        name = contact.get("Name", "")

        rec = FacultyRecord(
            contact_id=faculty_id,
            account_id=account_id,
            name=name,
            email=email,
            offering_ids=[off.get("Id", "")],
        )
        faculty_by_contact[faculty_id] = rec

        if not email:
            result.no_email.append(rec)
        else:
            result.resolved.append(rec)

    logger.info(
        "Faculty resolution: %d resolved, %d no PrimaryFacultyId, "
        "%d contact not found, %d no email",
        len(result.resolved),
        len(result.no_faculty),
        len(result.no_contact),
        len(result.no_email),
    )
    return result


def faculty_to_sf_accounts(resolution: FacultyResolution) -> list[dict]:
    """Convert resolved faculty to SF Account format for user_sync.

    Returns records compatible with ``user_sync.sync_users()`` — each has
    ``Id`` (AccountId), ``PersonEmail``, and ``Name``.
    """
    return [
        {
            "Id": rec.account_id,
            "Name": rec.name,
            "PersonEmail": rec.email,
            "Email": rec.email,
            "_contact_id": rec.contact_id,
            "_role": "faculty",
            "_offering_ids": rec.offering_ids,
        }
        for rec in resolution.resolved
    ]


def sync_faculty(
    offerings: list[dict],
    contacts: list[dict],
    moodle: MoodleClient,
    dry_run: bool = False,
) -> tuple[FacultyResolution, UserSyncResult]:
    """Full faculty sync: resolve from CourseOfferings, then sync to Moodle.

    Parameters
    ----------
    offerings : list[dict]
        SF CourseOffering records with ``PrimaryFacultyId``.
    contacts : list[dict]
        SF Contact records with ``Id``, ``AccountId``, ``Email``.
    moodle : MoodleClient
        Authenticated Moodle client.
    dry_run : bool
        If True, resolve and match only — do not update Moodle.

    Returns
    -------
    tuple[FacultyResolution, UserSyncResult]
        Resolution details and sync results.
    """
    resolution = resolve_faculty(offerings, contacts)

    if not resolution.resolved:
        logger.info("No faculty resolved — nothing to sync")
        return resolution, UserSyncResult(
            matched=[], unmatched_sf=[], already_set=[], updated=[], errors=[]
        )

    sf_accounts = faculty_to_sf_accounts(resolution)
    logger.info("Syncing %d faculty as SF accounts...", len(sf_accounts))

    from .user_sync import sync_users

    sync_result = sync_users(sf_accounts, moodle, dry_run=dry_run)
    return resolution, sync_result


def print_faculty_report(
    resolution: FacultyResolution, sync_result: UserSyncResult | None = None
) -> None:
    """Print a human-readable faculty resolution + sync report."""
    print(f"\n{'=' * 70}")
    print("  FACULTY SYNC REPORT")
    print(f"{'=' * 70}")
    print(f"  Resolved (with email):     {len(resolution.resolved)}")
    print(f"  No PrimaryFacultyId:       {len(resolution.no_faculty)}")
    print(f"  Contact not found in SF:   {len(resolution.no_contact)}")
    print(f"  No email on Contact:       {len(resolution.no_email)}")
    print(f"{'=' * 70}\n")

    if resolution.resolved:
        print("── Resolved Faculty ──")
        for r in resolution.resolved:
            offerings = ", ".join(r.offering_ids[:3])
            if len(r.offering_ids) > 3:
                offerings += f" (+{len(r.offering_ids) - 3} more)"
            print(
                f"  {r.name:<30s}  Contact {r.contact_id}  "
                f"Account {r.account_id}  {r.email}"
            )
        print()

    if resolution.no_email:
        print("── Faculty with no email (cannot sync) ──")
        for r in resolution.no_email:
            print(f"  {r.name:<30s}  Contact {r.contact_id}  Account {r.account_id}")
        print()

    if resolution.no_contact:
        print("── Contact IDs not found in SF ──")
        for cid in resolution.no_contact:
            print(f"  {cid}")
        print()

    if sync_result:
        print(f"── Moodle Sync ──")
        print(f"  Matched:      {len(sync_result.matched)}")
        print(f"  Updated:      {len(sync_result.updated)}")
        print(f"  Already set:  {len(sync_result.already_set)}")
        print(f"  Unmatched:    {len(sync_result.unmatched_sf)}")
        print(f"  Errors:       {len(sync_result.errors)}")
        print()


def load_json(path: str) -> list[dict]:
    """Load records from a JSON file."""
    data = json.loads(pathlib.Path(path).read_text())
    if isinstance(data, list):
        return data
    for key in ("records", "offerings", "contacts"):
        if key in data:
            return data[key]
    return data


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Sync faculty SF Account IDs → Moodle User ID Numbers"
    )
    parser.add_argument(
        "--offerings", required=True,
        help="SF CourseOffering JSON file (with PrimaryFacultyId)",
    )
    parser.add_argument(
        "--contacts", required=True,
        help="SF Contact JSON file (with Id, AccountId, Email)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Resolve and match only — do not update Moodle",
    )
    parser.add_argument(
        "--output-dir", default="output/faculty_sync",
        help="Output directory for reports",
    )
    args = parser.parse_args()

    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    offerings = load_json(args.offerings)
    contacts = load_json(args.contacts)

    moodle = MoodleClient()
    resolution, sync_result = sync_faculty(
        offerings, contacts, moodle, dry_run=args.dry_run
    )

    print_faculty_report(resolution, sync_result)

    # Save results
    resolution_out = {
        "resolved": [
            {
                "contact_id": r.contact_id,
                "account_id": r.account_id,
                "name": r.name,
                "email": r.email,
                "offering_ids": r.offering_ids,
            }
            for r in resolution.resolved
        ],
        "no_faculty": resolution.no_faculty,
        "no_contact": resolution.no_contact,
        "no_email": [
            {
                "contact_id": r.contact_id,
                "account_id": r.account_id,
                "name": r.name,
                "offering_ids": r.offering_ids,
            }
            for r in resolution.no_email
        ],
    }
    (output_dir / "faculty_resolution.json").write_text(
        json.dumps(resolution_out, indent=2)
    )

    if sync_result:
        sync_out = {
            "matched": len(sync_result.matched),
            "updated": len(sync_result.updated),
            "already_set": len(sync_result.already_set),
            "unmatched": len(sync_result.unmatched_sf),
            "errors": len(sync_result.errors),
            "matched_details": [m.__dict__ for m in sync_result.matched],
            "unmatched_details": sync_result.unmatched_sf,
            "error_details": sync_result.errors,
        }
        (output_dir / "faculty_sync_result.json").write_text(
            json.dumps(sync_out, indent=2)
        )

    logger.info("Results written to %s/", output_dir)


if __name__ == "__main__":
    main()
