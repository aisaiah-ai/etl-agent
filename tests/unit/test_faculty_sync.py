"""Unit tests for faculty sync (CourseOffering → Contact → Moodle)."""

from unittest.mock import MagicMock

import pytest

from pipelines.sf_moodle_sync.faculty_sync import (
    FacultyRecord,
    resolve_faculty,
    faculty_to_sf_accounts,
    sync_faculty,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def offerings():
    return [
        {"Id": "OFF001", "Name": "AH 221", "PrimaryFacultyId": "003AAA"},
        {"Id": "OFF002", "Name": "ECON 301", "PrimaryFacultyId": "003BBB"},
        {"Id": "OFF003", "Name": "HIST 210", "PrimaryFacultyId": "003AAA"},  # same faculty
        {"Id": "OFF004", "Name": "PHIL 101", "PrimaryFacultyId": None},  # no faculty
        {"Id": "OFF005", "Name": "BIO 100", "PrimaryFacultyId": "003CCC"},  # no email
        {"Id": "OFF006", "Name": "CHEM 200", "PrimaryFacultyId": "003ZZZ"},  # missing contact
    ]


@pytest.fixture
def contacts():
    return [
        {"Id": "003AAA", "AccountId": "001XXX", "Name": "Michael Faraday", "Email": "mfaraday@bustaff.edu"},
        {"Id": "003BBB", "AccountId": "001YYY", "Name": "Marie Curie", "Email": "mcurie@bustaff.edu"},
        {"Id": "003CCC", "AccountId": "001ZZZ", "Name": "Katherine Johnson", "Email": ""},
    ]


@pytest.fixture
def mock_moodle():
    client = MagicMock()

    def lookup(field="email", values=None):
        users_db = {
            "mfaraday@bustaff.edu": [
                {"id": 1001, "email": "mfaraday@bustaff.edu", "username": "mfaraday", "idnumber": ""}
            ],
            "mcurie@bustaff.edu": [
                {"id": 1002, "email": "mcurie@bustaff.edu", "username": "mcurie", "idnumber": "001YYY"}
            ],
        }
        if values and values[0] in users_db:
            return users_db[values[0]]
        return []

    client.get_users_by_field.side_effect = lookup
    return client


# ── Resolution tests ─────────────────────────────────────────────────────

def test_resolve_faculty_basic(offerings, contacts):
    result = resolve_faculty(offerings, contacts)
    assert len(result.resolved) == 2  # Faraday and Curie (with email)
    assert len(result.no_faculty) == 1  # OFF004 has no PrimaryFacultyId
    assert len(result.no_contact) == 1  # 003ZZZ not in contacts
    assert len(result.no_email) == 1  # Katherine Johnson has no email


def test_resolve_deduplicates_by_contact(offerings, contacts):
    result = resolve_faculty(offerings, contacts)
    faraday = [r for r in result.resolved if r.contact_id == "003AAA"]
    assert len(faraday) == 1
    assert "OFF001" in faraday[0].offering_ids
    assert "OFF003" in faraday[0].offering_ids


def test_resolve_no_faculty_id(offerings, contacts):
    result = resolve_faculty(offerings, contacts)
    no_fac_ids = [o["Id"] for o in result.no_faculty]
    assert "OFF004" in no_fac_ids


def test_resolve_missing_contact(offerings, contacts):
    result = resolve_faculty(offerings, contacts)
    assert "003ZZZ" in result.no_contact


def test_resolve_no_email(offerings, contacts):
    result = resolve_faculty(offerings, contacts)
    no_email_names = [r.name for r in result.no_email]
    assert "Katherine Johnson" in no_email_names


# ── Conversion tests ─────────────────────────────────────────────────────

def test_faculty_to_sf_accounts(offerings, contacts):
    resolution = resolve_faculty(offerings, contacts)
    accounts = faculty_to_sf_accounts(resolution)
    assert len(accounts) == 2
    for acct in accounts:
        assert "Id" in acct
        assert "PersonEmail" in acct
        assert acct["_role"] == "faculty"


def test_faculty_to_sf_accounts_has_correct_ids(offerings, contacts):
    resolution = resolve_faculty(offerings, contacts)
    accounts = faculty_to_sf_accounts(resolution)
    ids = {a["Id"] for a in accounts}
    assert "001XXX" in ids  # Faraday's AccountId
    assert "001YYY" in ids  # Curie's AccountId


# ── Sync tests ──────────────────────────────────────────────────────────

def test_sync_faculty_dry_run(offerings, contacts, mock_moodle):
    resolution, sync_result = sync_faculty(
        offerings, contacts, mock_moodle, dry_run=True
    )
    assert len(resolution.resolved) == 2
    assert len(sync_result.updated) == 0
    mock_moodle.update_users.assert_not_called()


def test_sync_faculty_updates(offerings, contacts, mock_moodle):
    resolution, sync_result = sync_faculty(
        offerings, contacts, mock_moodle, dry_run=False
    )
    assert len(sync_result.matched) == 2
    assert len(sync_result.updated) == 1  # Only Faraday (Curie already set)
    assert len(sync_result.already_set) == 1
    mock_moodle.update_users.assert_called_once()
    call_args = mock_moodle.update_users.call_args[0][0]
    assert call_args[0]["id"] == 1001
    assert call_args[0]["idnumber"] == "001XXX"


def test_sync_empty_offerings(contacts, mock_moodle):
    resolution, sync_result = sync_faculty([], contacts, mock_moodle)
    assert len(resolution.resolved) == 0
    assert len(sync_result.matched) == 0


def test_sync_all_null_faculty(contacts, mock_moodle):
    offerings = [
        {"Id": "OFF1", "Name": "Course A", "PrimaryFacultyId": None},
        {"Id": "OFF2", "Name": "Course B", "PrimaryFacultyId": ""},
    ]
    resolution, sync_result = sync_faculty(offerings, contacts, mock_moodle)
    assert len(resolution.no_faculty) == 2
    assert len(resolution.resolved) == 0
