"""Integration tests for SF→Moodle sync pipeline.

Workflow:
    1. SEED — Create known test courses & users in Moodle sandbox
    2. RUN  — Execute sync logic with SF fixture data (JSON)
    3. VERIFY — Re-read Moodle to confirm idnumbers are set correctly
    4. CLEANUP — Delete test data from Moodle

Edge cases covered:
    - Course: empty idnumber → gets set
    - Course: already correct idnumber → left alone
    - Course: conflicting idnumber → skipped (or overwritten with --force)
    - Course: no match in Moodle → reported as unmatched
    - User: empty idnumber → gets set
    - User: already correct → left alone
    - User: no email → reported as unmatched
    - User: no Moodle account → reported as unmatched

Requires:
    MOODLE_URL and MOODLE_TOKEN env vars pointing at a sandbox.
    The Moodle service token must have create/delete capabilities.

Run:
    MOODLE_URL=https://ies-sbox.unhosting.site MOODLE_TOKEN=... \
        python3 -m pytest tests/integration/test_sf_moodle_sync.py -v -s
"""

from __future__ import annotations

import logging
import os
import time
import uuid

import pytest

logger = logging.getLogger(__name__)

# Skip entire module if Moodle env vars aren't set
pytestmark = pytest.mark.skipif(
    not os.environ.get("MOODLE_URL") or not os.environ.get("MOODLE_TOKEN"),
    reason="Requires MOODLE_URL and MOODLE_TOKEN env vars (sandbox only)",
)

# Unique prefix to avoid collisions with real data
TEST_PREFIX = f"SYNCTEST_{uuid.uuid4().hex[:6].upper()}"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def moodle():
    from pipelines.sf_moodle_sync.moodle_client import MoodleClient
    return MoodleClient()


@pytest.fixture(scope="module")
def test_courses(moodle):
    """Create 3 test courses in Moodle with known states."""
    courses_to_create = [
        {
            "shortname": f"{TEST_PREFIX}_EMPTY",
            "fullname": f"{TEST_PREFIX} Course Empty IDNum",
            "categoryid": 1,
            "idnumber": "",  # Edge case: empty
        },
        {
            "shortname": f"{TEST_PREFIX}_CORRECT",
            "fullname": f"{TEST_PREFIX} Course Already Correct",
            "categoryid": 1,
            "idnumber": "SF_COURSE_CORRECT",  # Will match SF fixture
        },
        {
            "shortname": f"{TEST_PREFIX}_CONFLICT",
            "fullname": f"{TEST_PREFIX} Course With Conflict",
            "categoryid": 1,
            "idnumber": "OLD_WRONG_ID",  # Differs from SF fixture
        },
    ]

    created = moodle.create_courses(courses_to_create)
    logger.info("Created %d test courses: %s", len(created), created)

    # Map shortname → moodle id
    course_map = {c["shortname"]: c["id"] for c in created}

    yield {
        "created": created,
        "map": course_map,
        "sf_fixtures": [
            {"Id": "SF_COURSE_EMPTY", "Name": f"{TEST_PREFIX}_EMPTY"},
            {"Id": "SF_COURSE_CORRECT", "Name": f"{TEST_PREFIX}_CORRECT"},
            {"Id": "SF_COURSE_CONFLICT", "Name": f"{TEST_PREFIX}_CONFLICT"},
            {"Id": "SF_COURSE_NOMATCH", "Name": f"{TEST_PREFIX}_DOESNOTEXIST"},
        ],
    }

    # Manual cleanup needed — log IDs for reference
    logger.info(
        "TEST CLEANUP NEEDED — delete these courses manually: %s (prefix: %s)",
        list(course_map.values()), TEST_PREFIX,
    )


@pytest.fixture(scope="module")
def test_users(moodle):
    """Create 2 test users in Moodle with known states."""
    users_to_create = [
        {
            "username": f"{TEST_PREFIX.lower()}_empty",
            "password": f"Test1234!{uuid.uuid4().hex[:4]}",
            "firstname": "Test",
            "lastname": "EmptyID",
            "email": f"{TEST_PREFIX.lower()}_empty@test-synctest.example.com",
            "idnumber": "",  # Edge case: empty
        },
        {
            "username": f"{TEST_PREFIX.lower()}_correct",
            "password": f"Test1234!{uuid.uuid4().hex[:4]}",
            "firstname": "Test",
            "lastname": "CorrectID",
            "email": f"{TEST_PREFIX.lower()}_correct@test-synctest.example.com",
            "idnumber": "SF_USER_CORRECT",  # Will match SF fixture
        },
    ]

    created = moodle.create_users(users_to_create)
    logger.info("Created %d test users: %s", len(created), created)

    user_map = {u["username"]: u["id"] for u in created}

    yield {
        "created": created,
        "map": user_map,
        "sf_fixtures": [
            {
                "Id": "SF_USER_EMPTY",
                "Name": "Test EmptyID",
                "PersonEmail": f"{TEST_PREFIX.lower()}_empty@test-synctest.example.com",
            },
            {
                "Id": "SF_USER_CORRECT",
                "Name": "Test CorrectID",
                "PersonEmail": f"{TEST_PREFIX.lower()}_correct@test-synctest.example.com",
            },
            {
                "Id": "SF_USER_NOEMAIL",
                "Name": "No Email Person",
                "PersonEmail": "",  # Edge case: no email
            },
            {
                "Id": "SF_USER_NOMATCH",
                "Name": "Ghost User",
                "PersonEmail": f"{TEST_PREFIX.lower()}_ghost@test-synctest.example.com",
            },
        ],
    }

    # Manual cleanup needed — log IDs for reference
    logger.info(
        "TEST CLEANUP NEEDED — delete these users manually: %s (prefix: %s)",
        list(user_map.values()), TEST_PREFIX,
    )


# ── Course Sync Tests ─────────────────────────────────────────────────────

class TestCourseSyncIntegration:
    """Full round-trip: seed → audit → sync → verify → cleanup."""

    def test_audit_detects_all_states(self, moodle, test_courses):
        """Audit should categorize each course correctly before any sync."""
        from pipelines.sf_moodle_sync.verify_sync import audit_courses

        report = audit_courses(test_courses["sf_fixtures"], moodle)

        statuses = {r.sf_id: r.status for r in report.records}
        assert statuses["SF_COURSE_EMPTY"] == "empty", "Empty idnumber should be 'empty'"
        assert statuses["SF_COURSE_CORRECT"] == "match", "Correct idnumber should be 'match'"
        assert statuses["SF_COURSE_CONFLICT"] == "conflict", "Wrong idnumber should be 'conflict'"
        assert statuses["SF_COURSE_NOMATCH"] == "not_found", "Missing course should be 'not_found'"

        assert report.empty_idnumber == 1
        assert report.already_correct == 1
        assert report.conflict == 1
        assert report.not_found == 1

    def test_sync_updates_empty_only(self, moodle, test_courses):
        """Sync without --force should only update empty idnumbers."""
        from pipelines.sf_moodle_sync.verify_sync import audit_courses, sync_from_audit

        report = audit_courses(test_courses["sf_fixtures"], moodle)
        result = sync_from_audit(report, moodle, force=False)

        assert result["updated"] == 1, "Should update only the empty one"
        assert result["skipped_conflicts"] == 1, "Should skip the conflict"

    def test_verify_after_sync(self, moodle, test_courses):
        """Re-read Moodle and confirm the empty course now has the SF ID."""
        from pipelines.sf_moodle_sync.verify_sync import audit_courses

        # Small delay to let Moodle propagate
        time.sleep(1)

        report = audit_courses(test_courses["sf_fixtures"], moodle)
        statuses = {r.sf_id: r.status for r in report.records}

        # The previously empty one should now be correct
        assert statuses["SF_COURSE_EMPTY"] == "match", \
            "After sync, previously empty course should now show 'match'"
        # The already-correct one should still be correct
        assert statuses["SF_COURSE_CORRECT"] == "match"
        # The conflict should still be a conflict (we didn't force)
        assert statuses["SF_COURSE_CONFLICT"] == "conflict"

    def test_sync_force_overwrites_conflict(self, moodle, test_courses):
        """Sync with force=True should overwrite the conflicting idnumber."""
        from pipelines.sf_moodle_sync.verify_sync import audit_courses, sync_from_audit

        report = audit_courses(test_courses["sf_fixtures"], moodle)
        result = sync_from_audit(report, moodle, force=True)

        # Conflict should now be updated (empty was already set in previous test)
        assert result["updated"] >= 1

        time.sleep(1)

        # Verify all matched courses are now correct
        report2 = audit_courses(test_courses["sf_fixtures"], moodle)
        matched_statuses = {r.sf_id: r.status for r in report2.records if r.status != "not_found"}
        for sf_id, status in matched_statuses.items():
            assert status == "match", f"{sf_id} should be 'match' after force sync, got '{status}'"

    def test_original_sync_function_works(self, moodle, test_courses):
        """The original sync_courses function also works end-to-end."""
        from pipelines.sf_moodle_sync.course_sync import sync_courses

        # Reset one course's idnumber to empty for this test
        empty_course_id = test_courses["map"].get(f"{TEST_PREFIX}_EMPTY")
        if empty_course_id:
            moodle.update_courses([{"id": empty_course_id, "idnumber": ""}])
            time.sleep(1)

        result = sync_courses(test_courses["sf_fixtures"], moodle, dry_run=False)

        assert result.matched  # At least some matched
        assert any(m.sf_offering_id == "SF_COURSE_EMPTY" for m in result.matched)


# ── User Sync Tests ───────────────────────────────────────────────────────

class TestUserSyncIntegration:
    """Full round-trip: seed → audit → sync → verify → cleanup."""

    def test_audit_detects_all_states(self, moodle, test_users):
        """Audit should categorize each user correctly before any sync."""
        from pipelines.sf_moodle_sync.verify_sync import audit_users

        report = audit_users(test_users["sf_fixtures"], moodle)

        statuses = {r.sf_id: r.status for r in report.records}
        assert statuses["SF_USER_EMPTY"] == "empty"
        assert statuses["SF_USER_CORRECT"] == "match"
        assert statuses["SF_USER_NOEMAIL"] == "not_found"
        assert statuses["SF_USER_NOMATCH"] == "not_found"

    def test_sync_updates_empty_only(self, moodle, test_users):
        """Sync should only update the user with empty idnumber."""
        from pipelines.sf_moodle_sync.verify_sync import audit_users, sync_from_audit

        report = audit_users(test_users["sf_fixtures"], moodle)
        result = sync_from_audit(report, moodle, force=False)

        assert result["updated"] == 1

    def test_verify_after_sync(self, moodle, test_users):
        """Re-read Moodle and confirm the empty user now has the SF ID."""
        from pipelines.sf_moodle_sync.verify_sync import audit_users

        time.sleep(1)
        report = audit_users(test_users["sf_fixtures"], moodle)
        statuses = {r.sf_id: r.status for r in report.records}

        assert statuses["SF_USER_EMPTY"] == "match", \
            "After sync, previously empty user should now show 'match'"
        assert statuses["SF_USER_CORRECT"] == "match"

    def test_original_sync_function_works(self, moodle, test_users):
        """The original sync_users function also works end-to-end."""
        from pipelines.sf_moodle_sync.user_sync import sync_users

        # Reset user idnumber to empty
        empty_user_id = test_users["map"].get(f"{TEST_PREFIX.lower()}_empty")
        if empty_user_id:
            moodle.update_users([{"id": empty_user_id, "idnumber": ""}])
            time.sleep(1)

        result = sync_users(test_users["sf_fixtures"], moodle, dry_run=False)

        assert result.matched
        assert any(m.sf_account_id == "SF_USER_EMPTY" for m in result.matched)


# ── Idempotency Test ──────────────────────────────────────────────────────

class TestIdempotency:
    """Running sync twice should be safe — no errors, no changes on second run."""

    def test_course_sync_idempotent(self, moodle, test_courses):
        from pipelines.sf_moodle_sync.verify_sync import audit_courses, sync_from_audit

        # First sync (force to set everything)
        r1 = audit_courses(test_courses["sf_fixtures"], moodle)
        sync_from_audit(r1, moodle, force=True)
        time.sleep(1)

        # Second sync — should find nothing to do
        r2 = audit_courses(test_courses["sf_fixtures"], moodle)
        result = sync_from_audit(r2, moodle, force=True)

        assert result["updated"] == 0, "Second sync should have nothing to update"

    def test_user_sync_idempotent(self, moodle, test_users):
        from pipelines.sf_moodle_sync.verify_sync import audit_users, sync_from_audit

        r1 = audit_users(test_users["sf_fixtures"], moodle)
        sync_from_audit(r1, moodle, force=True)
        time.sleep(1)

        r2 = audit_users(test_users["sf_fixtures"], moodle)
        result = sync_from_audit(r2, moodle, force=True)

        assert result["updated"] == 0
