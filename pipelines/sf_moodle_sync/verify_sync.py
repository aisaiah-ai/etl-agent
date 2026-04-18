"""Audit, sync, and verify SF→Moodle ID mappings.

Three modes:
    audit   — Read-only. Show what's empty, set, or conflicting in Moodle.
    sync    — Update empty idnumbers only (use --force to overwrite existing).
    verify  — Re-read Moodle after sync to confirm values stuck.

Usage:
    # Audit courses (read-only)
    python3 -m pipelines.sf_moodle_sync.verify_sync audit courses \
        --sf-courses output/sf_courses.json

    # Audit users (read-only)
    python3 -m pipelines.sf_moodle_sync.verify_sync audit users \
        --sf-accounts output/sf_accounts.json

    # Sync only empty idnumbers
    python3 -m pipelines.sf_moodle_sync.verify_sync sync courses \
        --sf-courses output/sf_courses.json

    # Force-overwrite existing idnumbers
    python3 -m pipelines.sf_moodle_sync.verify_sync sync courses \
        --sf-courses output/sf_courses.json --force

    # Verify after sync
    python3 -m pipelines.sf_moodle_sync.verify_sync verify courses \
        --sf-courses output/sf_courses.json

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────

@dataclass
class AuditRecord:
    sf_id: str
    sf_name: str
    moodle_id: int | None  # None = no match found in Moodle
    moodle_name: str
    current_idnumber: str  # what Moodle has now
    status: str  # "empty" | "match" | "conflict" | "not_found" | "needs_review"
    detail: str = ""
    match_type: str = ""  # "exact_shortname" | "exact_fullname" | "fuzzy" | "ai" | ""
    confidence: float = 0.0


@dataclass
class AuditReport:
    entity_type: str  # "course" | "user"
    total_sf: int = 0
    matched: int = 0
    not_found: int = 0
    empty_idnumber: int = 0
    already_correct: int = 0
    conflict: int = 0
    records: list[AuditRecord] = field(default_factory=list)


# ── Helpers ───────────────────────────────────────────────────────────────

def _normalize(name: str) -> str:
    return " ".join(name.lower().split())


def _load_json(path: str) -> list[dict]:
    data = json.loads(pathlib.Path(path).read_text())
    if isinstance(data, list):
        return data
    # Support common wrapper keys
    for key in ("records", "offerings", "accounts"):
        if key in data:
            return data[key]
    return data


# ── Course audit ──────────────────────────────────────────────────────────

def audit_courses(
    sf_offerings: list[dict],
    moodle: MoodleClient,
    use_ai: bool = True,
    bedrock_region: str = "us-east-1",
) -> AuditReport:
    """Check each SF offering against Moodle using tiered fuzzy + AI matching.

    Hybrid approach:
    1. Try exact shortname match per offering (fast API calls)
    2. Only fetch all courses for fuzzy matching if there are unmatched offerings
    """
    from .course_sync import match_courses, print_review_report, CourseMatch, CourseSyncResult

    report = AuditReport(entity_type="course", total_sf=len(sf_offerings))

    # ── Phase 1: Exact match per offering (fast) ──
    exact_matched: list[dict] = []  # (sf, mc) pairs
    unmatched_sf: list[dict] = []

    for sf in sf_offerings:
        sf_name = sf.get("Name", "")
        if not sf_name:
            unmatched_sf.append(sf)
            continue
        matches = moodle.search_courses_by_field("shortname", sf_name)
        if matches:
            exact_matched.append((sf, matches[0]))
        else:
            unmatched_sf.append(sf)

    logger.info(
        "Phase 1 (exact): %d matched, %d unmatched", len(exact_matched), len(unmatched_sf)
    )

    # ── Phase 2: Fuzzy match unmatched offerings (slow — fetches all courses) ──
    if unmatched_sf:
        logger.info(
            "Phase 2: Fetching all Moodle courses for fuzzy matching %d offerings...",
            len(unmatched_sf),
        )
        try:
            all_courses = moodle.get_courses(timeout=600)
            logger.info("Fetched %d Moodle courses", len(all_courses))
        except Exception as e:
            logger.error("Failed to fetch all courses: %s — skipping fuzzy matching", e)
            all_courses = None

    # Build sync_result from exact matches + fuzzy matches
    if unmatched_sf and all_courses:
        # Exclude already-matched Moodle course IDs from fuzzy pool
        matched_ids = {mc["id"] for _, mc in exact_matched}
        fuzzy_pool = [c for c in all_courses if c["id"] not in matched_ids]
        sync_result = match_courses(
            unmatched_sf, fuzzy_pool, use_ai=use_ai, bedrock_region=bedrock_region
        )
    else:
        sync_result = CourseSyncResult()
        # Any still-unmatched go to unmatched
        if unmatched_sf and not all_courses:
            sync_result.unmatched_sf = unmatched_sf

    # Prepend exact matches into sync_result
    for sf, mc in exact_matched:
        sync_result.matched.insert(0, CourseMatch(
            sf_offering_id=sf.get("Id", ""),
            sf_course_name=sf.get("Name", ""),
            moodle_course_id=mc["id"],
            moodle_shortname=mc.get("shortname", ""),
            moodle_fullname=mc.get("fullname", ""),
            matched_on="exact_shortname",
            confidence=1.0,
        ))

    # Build a lookup for Moodle course data (from exact matches + bulk fetch)
    moodle_by_id: dict[int, dict] = {mc["id"]: mc for _, mc in exact_matched}
    if unmatched_sf and all_courses:
        for c in all_courses:
            moodle_by_id.setdefault(c["id"], c)

    # Convert matched courses to audit records
    for m in sync_result.matched:
        report.matched += 1
        mc = moodle_by_id.get(m.moodle_course_id, {})
        current = mc.get("idnumber", "")

        if not current:
            status = "empty"
            report.empty_idnumber += 1
            detail = f"Moodle idnumber is empty — safe to set ({m.matched_on} {m.confidence:.0%})"
        elif current == m.sf_offering_id:
            status = "match"
            report.already_correct += 1
            detail = f"Already correct ({m.matched_on})"
        else:
            status = "conflict"
            report.conflict += 1
            detail = (
                f"Moodle has '{current}', SF wants '{m.sf_offering_id}' "
                f"({m.matched_on} {m.confidence:.0%})"
            )

        report.records.append(AuditRecord(
            sf_id=m.sf_offering_id, sf_name=m.sf_course_name,
            moodle_id=m.moodle_course_id, moodle_name=m.moodle_shortname,
            current_idnumber=current, status=status, detail=detail,
            match_type=m.matched_on, confidence=m.confidence,
        ))

    # Convert needs_review to audit records
    for r in sync_result.needs_review:
        report.not_found += 1
        ai_note = ""
        if r.ai_confidence is not None:
            ai_note = f" AI: {r.ai_confidence:.0%} — {r.ai_reasoning}"
        report.records.append(AuditRecord(
            sf_id=r.sf_offering_id, sf_name=r.sf_course_name,
            moodle_id=r.moodle_course_id, moodle_name=r.moodle_shortname,
            current_idnumber="", status="needs_review",
            detail=f"Fuzzy {r.similarity:.0%} — needs manual review.{ai_note}",
            match_type="review", confidence=r.similarity,
        ))

    # Convert unmatched to audit records
    for sf in sync_result.unmatched_sf:
        report.not_found += 1
        report.records.append(AuditRecord(
            sf_id=sf.get("Id", ""), sf_name=sf.get("Name", "(no name)"),
            moodle_id=None, moodle_name="", current_idnumber="",
            status="not_found", detail="No Moodle match found (exact or fuzzy)",
        ))

    # Print review report if there are ambiguous/unmatched
    if sync_result.needs_review or sync_result.unmatched_sf:
        print_review_report(sync_result)

    return report

    return report


# ── User audit ────────────────────────────────────────────────────────────

def audit_users(sf_accounts: list[dict], moodle: MoodleClient) -> AuditReport:
    """Check each SF account against Moodle: is idnumber empty, correct, or conflicting?"""
    report = AuditReport(entity_type="user", total_sf=len(sf_accounts))

    for sf in sf_accounts:
        sf_id = sf.get("Id", "")
        email = (sf.get("PersonEmail") or sf.get("Email") or "").strip().lower()
        sf_name = sf.get("Name", "")

        if not email:
            report.not_found += 1
            report.records.append(AuditRecord(
                sf_id=sf_id, sf_name=sf_name, moodle_id=None,
                moodle_name="", current_idnumber="", status="not_found",
                detail="SF record has no email",
            ))
            continue

        try:
            moodle_users = moodle.get_users_by_field(field="email", values=[email])
        except Exception as e:
            report.not_found += 1
            report.records.append(AuditRecord(
                sf_id=sf_id, sf_name=sf_name, moodle_id=None,
                moodle_name="", current_idnumber="", status="not_found",
                detail=f"Moodle API error: {e}",
            ))
            continue

        if not moodle_users:
            report.not_found += 1
            report.records.append(AuditRecord(
                sf_id=sf_id, sf_name=sf_name, moodle_id=None,
                moodle_name="", current_idnumber="", status="not_found",
                detail=f"No Moodle user with email '{email}'",
            ))
            continue

        mu = moodle_users[0]
        report.matched += 1
        current = mu.get("idnumber", "")

        if not current:
            status = "empty"
            report.empty_idnumber += 1
            detail = "Moodle idnumber is empty — safe to set"
        elif current == sf_id:
            status = "match"
            report.already_correct += 1
            detail = "Already correct"
        else:
            status = "conflict"
            report.conflict += 1
            detail = f"Moodle has '{current}', SF wants '{sf_id}'"

        report.records.append(AuditRecord(
            sf_id=sf_id, sf_name=sf_name,
            moodle_id=mu["id"], moodle_name=mu.get("username", ""),
            current_idnumber=current, status=status, detail=detail,
        ))

    return report


# ── Sync (write) ──────────────────────────────────────────────────────────

def sync_from_audit(
    report: AuditReport, moodle: MoodleClient, force: bool = False
) -> dict:
    """Update Moodle idnumbers based on audit results.

    By default only updates 'empty' records. With force=True, also overwrites 'conflict'.
    Never touches 'match' (already correct) or 'not_found'.
    """
    to_update = []
    skipped_conflicts = []

    for rec in report.records:
        if rec.status == "empty":
            to_update.append(rec)
        elif rec.status == "conflict":
            if force:
                to_update.append(rec)
            else:
                skipped_conflicts.append(rec)

    if not to_update:
        logger.info("Nothing to update")
        return {"updated": 0, "skipped_conflicts": len(skipped_conflicts)}

    updated = 0
    errors = []
    is_course = report.entity_type == "course"

    for rec in to_update:
        if rec.moodle_id is None:
            continue
        try:
            if is_course:
                moodle.update_courses([{"id": rec.moodle_id, "idnumber": rec.sf_id}])
            else:
                moodle.update_users([{"id": rec.moodle_id, "idnumber": rec.sf_id}])
            updated += 1
            logger.info(
                "Updated %s %d: idnumber '' → '%s'",
                report.entity_type, rec.moodle_id, rec.sf_id,
            )
        except Exception as e:
            errors.append({"moodle_id": rec.moodle_id, "sf_id": rec.sf_id, "error": str(e)})
            logger.error("Failed to update %s %d: %s", report.entity_type, rec.moodle_id, e)

    return {
        "updated": updated,
        "skipped_conflicts": len(skipped_conflicts),
        "errors": errors,
    }


# ── Verify (re-read after sync) ──────────────────────────────────────────

def verify_sync(report: AuditReport, moodle: MoodleClient) -> dict:
    """Re-read Moodle for all matched records and check idnumber is correct."""
    verified = 0
    mismatches = []
    is_course = report.entity_type == "course"

    for rec in report.records:
        if rec.moodle_id is None:
            continue

        try:
            if is_course:
                results = moodle.search_courses_by_field("id", str(rec.moodle_id))
                current = results[0].get("idnumber", "") if results else ""
            else:
                results = moodle.get_users_by_field(field="id", values=[str(rec.moodle_id)])
                current = results[0].get("idnumber", "") if results else ""
        except Exception as e:
            mismatches.append({
                "moodle_id": rec.moodle_id,
                "expected": rec.sf_id,
                "actual": f"ERROR: {e}",
            })
            continue

        if current == rec.sf_id:
            verified += 1
        else:
            mismatches.append({
                "moodle_id": rec.moodle_id,
                "sf_id": rec.sf_id,
                "expected": rec.sf_id,
                "actual": current,
            })

    return {
        "total_checked": sum(1 for r in report.records if r.moodle_id is not None),
        "verified": verified,
        "mismatches": mismatches,
    }


# ── Display ───────────────────────────────────────────────────────────────

def print_report(report: AuditReport) -> None:
    """Print a human-readable audit report."""
    print(f"\n{'=' * 70}")
    print(f"  AUDIT REPORT: {report.entity_type.upper()}S")
    print(f"{'=' * 70}")
    print(f"  Total SF records:        {report.total_sf}")
    print(f"  Matched in Moodle:       {report.matched}")
    print(f"  Not found in Moodle:     {report.not_found}")
    print(f"  Empty idnumber (to set): {report.empty_idnumber}")
    print(f"  Already correct:         {report.already_correct}")
    print(f"  Conflict (different ID): {report.conflict}")
    print(f"{'=' * 70}\n")

    # Group by status
    for status in ("empty", "conflict", "match", "needs_review", "not_found"):
        group = [r for r in report.records if r.status == status]
        if not group:
            continue
        label = {
            "empty": "EMPTY — Ready to set",
            "conflict": "CONFLICT — Moodle has different ID",
            "match": "OK — Already correct",
            "needs_review": "NEEDS REVIEW — Ambiguous match",
            "not_found": "NOT FOUND — No Moodle match",
        }[status]
        print(f"── {label} ({len(group)}) ──")
        for r in group:
            moodle_label = f"Moodle #{r.moodle_id}" if r.moodle_id else "N/A"
            print(f"  SF {r.sf_id}  {r.sf_name:<40s}  {moodle_label:<12s}  {r.detail}")
        print()


# ── CLI ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Audit, sync, and verify SF→Moodle ID mappings"
    )
    parser.add_argument("mode", choices=["audit", "sync", "verify"])
    parser.add_argument("entity", choices=["courses", "users"])
    parser.add_argument("--sf-courses", help="SF course offerings JSON file")
    parser.add_argument("--sf-accounts", help="SF accounts JSON file")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing Moodle idnumbers that differ from SF")
    parser.add_argument("--no-ai", action="store_true",
                        help="Disable Bedrock AI for course fuzzy matching")
    parser.add_argument("--output-dir", default="output/sf_moodle_verify",
                        help="Output directory for reports")
    args = parser.parse_args()

    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    moodle = MoodleClient()

    # ── Load SF data ──────────────────────────────────────────────────
    if args.entity == "courses":
        if not args.sf_courses:
            print("ERROR: --sf-courses is required for course operations", file=sys.stderr)
            sys.exit(1)
        sf_data = _load_json(args.sf_courses)
        report = audit_courses(sf_data, moodle, use_ai=not args.no_ai)
    else:
        if not args.sf_accounts:
            print("ERROR: --sf-accounts is required for user operations", file=sys.stderr)
            sys.exit(1)
        sf_data = _load_json(args.sf_accounts)
        report = audit_users(sf_data, moodle)

    # ── Always print audit ────────────────────────────────────────────
    print_report(report)

    # Save audit JSON
    audit_file = output_dir / f"{args.entity}_audit.json"
    audit_file.write_text(json.dumps(
        {
            "entity_type": report.entity_type,
            "total_sf": report.total_sf,
            "matched": report.matched,
            "not_found": report.not_found,
            "empty_idnumber": report.empty_idnumber,
            "already_correct": report.already_correct,
            "conflict": report.conflict,
            "records": [r.__dict__ for r in report.records],
        },
        indent=2,
    ))
    logger.info("Audit saved to %s", audit_file)

    # ── Sync ──────────────────────────────────────────────────────────
    if args.mode == "sync":
        if report.conflict > 0 and not args.force:
            print(f"\nWARNING: {report.conflict} records have conflicting idnumbers.")
            print("These will be SKIPPED. Use --force to overwrite them.\n")

        sync_result = sync_from_audit(report, moodle, force=args.force)
        print(f"\nSync result: {sync_result['updated']} updated, "
              f"{sync_result['skipped_conflicts']} conflicts skipped")
        if sync_result.get("errors"):
            print(f"  Errors: {len(sync_result['errors'])}")

        (output_dir / f"{args.entity}_sync_result.json").write_text(
            json.dumps(sync_result, indent=2)
        )

    # ── Verify ────────────────────────────────────────────────────────
    if args.mode in ("sync", "verify"):
        print("\nVerifying — re-reading Moodle to confirm idnumbers...")
        verify_result = verify_sync(report, moodle)
        print(f"\nVerification: {verify_result['verified']}/{verify_result['total_checked']} correct")
        if verify_result["mismatches"]:
            print(f"  MISMATCHES: {len(verify_result['mismatches'])}")
            for mm in verify_result["mismatches"]:
                print(f"    Moodle #{mm['moodle_id']}: expected '{mm['expected']}', got '{mm['actual']}'")

        (output_dir / f"{args.entity}_verify_result.json").write_text(
            json.dumps(verify_result, indent=2)
        )

    print(f"\nAll reports saved to {output_dir}/")


if __name__ == "__main__":
    main()
