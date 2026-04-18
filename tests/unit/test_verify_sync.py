"""Tests for pipelines.sf_moodle_sync.verify_sync."""

from unittest.mock import MagicMock

import pytest

from pipelines.sf_moodle_sync.verify_sync import (
    AuditRecord,
    AuditReport,
    audit_courses,
    audit_users,
    sync_from_audit,
    verify_sync,
)


@pytest.fixture
def mock_moodle():
    return MagicMock()


# ── Course audit tests ────────────────────────────────────────────────────

class TestAuditCourses:
    def test_empty_idnumber(self, mock_moodle):
        """Course found in Moodle with empty idnumber → status 'empty'."""
        mock_moodle.search_courses_by_field.return_value = [
            {"id": 10, "shortname": "PSYCH101", "fullname": "Psychology", "idnumber": ""}
        ]
        sf = [{"Id": "a0xABC", "Name": "PSYCH101"}]
        report = audit_courses(sf, mock_moodle)

        assert report.matched == 1
        assert report.empty_idnumber == 1
        assert report.records[0].status == "empty"

    def test_already_correct(self, mock_moodle):
        """Course idnumber already matches SF ID → status 'match'."""
        mock_moodle.search_courses_by_field.return_value = [
            {"id": 10, "shortname": "PSYCH101", "idnumber": "a0xABC"}
        ]
        sf = [{"Id": "a0xABC", "Name": "PSYCH101"}]
        report = audit_courses(sf, mock_moodle)

        assert report.already_correct == 1
        assert report.records[0].status == "match"

    def test_conflict(self, mock_moodle):
        """Course has different idnumber → status 'conflict'."""
        mock_moodle.search_courses_by_field.return_value = [
            {"id": 10, "shortname": "PSYCH101", "idnumber": "OLD-ID-999"}
        ]
        sf = [{"Id": "a0xABC", "Name": "PSYCH101"}]
        report = audit_courses(sf, mock_moodle)

        assert report.conflict == 1
        assert report.records[0].status == "conflict"
        assert "OLD-ID-999" in report.records[0].detail

    def test_not_found(self, mock_moodle):
        """No Moodle course matches → status 'not_found'."""
        mock_moodle.search_courses_by_field.return_value = []
        sf = [{"Id": "a0xABC", "Name": "NONEXISTENT"}]
        report = audit_courses(sf, mock_moodle)

        assert report.not_found == 1
        assert report.records[0].status == "not_found"

    def test_mixed_statuses(self, mock_moodle):
        """Multiple SF records with different statuses."""
        def search_side_effect(field, value):
            lookup = {
                "COURSE_A": [{"id": 1, "shortname": "COURSE_A", "idnumber": ""}],
                "COURSE_B": [{"id": 2, "shortname": "COURSE_B", "idnumber": "SF-B"}],
                "COURSE_C": [{"id": 3, "shortname": "COURSE_C", "idnumber": "WRONG"}],
            }
            return lookup.get(value, [])

        mock_moodle.search_courses_by_field.side_effect = search_side_effect
        sf = [
            {"Id": "SF-A", "Name": "COURSE_A"},  # empty
            {"Id": "SF-B", "Name": "COURSE_B"},  # match
            {"Id": "SF-C", "Name": "COURSE_C"},  # conflict
            {"Id": "SF-D", "Name": "COURSE_D"},  # not found
        ]
        report = audit_courses(sf, mock_moodle)

        assert report.total_sf == 4
        assert report.empty_idnumber == 1
        assert report.already_correct == 1
        assert report.conflict == 1
        assert report.not_found == 1


# ── User audit tests ──────────────────────────────────────────────────────

class TestAuditUsers:
    def test_empty_idnumber(self, mock_moodle):
        mock_moodle.get_users_by_field.return_value = [
            {"id": 50, "username": "jdoe", "email": "j@example.com", "idnumber": ""}
        ]
        sf = [{"Id": "001ABC", "Name": "John Doe", "PersonEmail": "j@example.com"}]
        report = audit_users(sf, mock_moodle)

        assert report.empty_idnumber == 1
        assert report.records[0].status == "empty"

    def test_already_correct(self, mock_moodle):
        mock_moodle.get_users_by_field.return_value = [
            {"id": 50, "username": "jdoe", "email": "j@example.com", "idnumber": "001ABC"}
        ]
        sf = [{"Id": "001ABC", "Name": "John Doe", "PersonEmail": "j@example.com"}]
        report = audit_users(sf, mock_moodle)

        assert report.already_correct == 1
        assert report.records[0].status == "match"

    def test_no_email(self, mock_moodle):
        sf = [{"Id": "001ABC", "Name": "No Email Person"}]
        report = audit_users(sf, mock_moodle)

        assert report.not_found == 1
        assert "no email" in report.records[0].detail


# ── Sync tests ────────────────────────────────────────────────────────────

class TestSyncFromAudit:
    def test_sync_empty_only(self, mock_moodle):
        """Default: only updates empty idnumbers, skips conflicts."""
        report = AuditReport(entity_type="course", records=[
            AuditRecord(sf_id="SF-A", sf_name="A", moodle_id=1,
                        moodle_name="A", current_idnumber="", status="empty"),
            AuditRecord(sf_id="SF-B", sf_name="B", moodle_id=2,
                        moodle_name="B", current_idnumber="OLD", status="conflict"),
            AuditRecord(sf_id="SF-C", sf_name="C", moodle_id=3,
                        moodle_name="C", current_idnumber="SF-C", status="match"),
        ])

        result = sync_from_audit(report, mock_moodle, force=False)

        assert result["updated"] == 1
        assert result["skipped_conflicts"] == 1
        mock_moodle.update_courses.assert_called_once_with(
            [{"id": 1, "idnumber": "SF-A"}]
        )

    def test_sync_force_overwrites_conflicts(self, mock_moodle):
        """With force=True, also overwrites conflicting idnumbers."""
        report = AuditReport(entity_type="course", records=[
            AuditRecord(sf_id="SF-A", sf_name="A", moodle_id=1,
                        moodle_name="A", current_idnumber="", status="empty"),
            AuditRecord(sf_id="SF-B", sf_name="B", moodle_id=2,
                        moodle_name="B", current_idnumber="OLD", status="conflict"),
        ])

        result = sync_from_audit(report, mock_moodle, force=True)

        assert result["updated"] == 2
        assert result["skipped_conflicts"] == 0

    def test_sync_users(self, mock_moodle):
        """User sync uses update_users, not update_courses."""
        report = AuditReport(entity_type="user", records=[
            AuditRecord(sf_id="001X", sf_name="Jane", moodle_id=99,
                        moodle_name="jane", current_idnumber="", status="empty"),
        ])

        sync_from_audit(report, mock_moodle, force=False)
        mock_moodle.update_users.assert_called_once_with(
            [{"id": 99, "idnumber": "001X"}]
        )

    def test_nothing_to_update(self, mock_moodle):
        """All records already correct → no API calls."""
        report = AuditReport(entity_type="course", records=[
            AuditRecord(sf_id="SF-A", sf_name="A", moodle_id=1,
                        moodle_name="A", current_idnumber="SF-A", status="match"),
        ])

        result = sync_from_audit(report, mock_moodle, force=False)
        assert result["updated"] == 0
        mock_moodle.update_courses.assert_not_called()


# ── Verify tests ──────────────────────────────────────────────────────────

class TestVerifySync:
    def test_all_verified(self, mock_moodle):
        """All idnumbers match after sync."""
        mock_moodle.search_courses_by_field.return_value = [
            {"id": 1, "idnumber": "SF-A"}
        ]
        report = AuditReport(entity_type="course", records=[
            AuditRecord(sf_id="SF-A", sf_name="A", moodle_id=1,
                        moodle_name="A", current_idnumber="", status="empty"),
        ])

        result = verify_sync(report, mock_moodle)
        assert result["verified"] == 1
        assert result["mismatches"] == []

    def test_mismatch_detected(self, mock_moodle):
        """Moodle idnumber doesn't match expected → mismatch."""
        mock_moodle.search_courses_by_field.return_value = [
            {"id": 1, "idnumber": "WRONG"}
        ]
        report = AuditReport(entity_type="course", records=[
            AuditRecord(sf_id="SF-A", sf_name="A", moodle_id=1,
                        moodle_name="A", current_idnumber="", status="empty"),
        ])

        result = verify_sync(report, mock_moodle)
        assert result["verified"] == 0
        assert len(result["mismatches"]) == 1
        assert result["mismatches"][0]["actual"] == "WRONG"

    def test_verify_users(self, mock_moodle):
        """User verification uses get_users_by_field."""
        mock_moodle.get_users_by_field.return_value = [
            {"id": 99, "idnumber": "001X"}
        ]
        report = AuditReport(entity_type="user", records=[
            AuditRecord(sf_id="001X", sf_name="Jane", moodle_id=99,
                        moodle_name="jane", current_idnumber="", status="empty"),
        ])

        result = verify_sync(report, mock_moodle)
        assert result["verified"] == 1

    def test_skips_not_found(self, mock_moodle):
        """Records with no moodle_id are skipped in verification."""
        report = AuditReport(entity_type="course", records=[
            AuditRecord(sf_id="SF-X", sf_name="X", moodle_id=None,
                        moodle_name="", current_idnumber="", status="not_found"),
        ])

        result = verify_sync(report, mock_moodle)
        assert result["total_checked"] == 0
