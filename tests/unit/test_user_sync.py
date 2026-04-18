"""Unit tests for SF → Moodle user sync."""

from unittest.mock import MagicMock

import pytest

from pipelines.sf_moodle_sync.user_sync import match_users_by_email, sync_users


@pytest.fixture
def mock_moodle():
    client = MagicMock()

    def lookup(field="email", values=None):
        users_db = {
            "alice@iesabroad.org": [
                {"id": 101, "email": "alice@iesabroad.org", "username": "alice", "idnumber": ""}
            ],
            "bob@iesabroad.org": [
                {"id": 102, "email": "bob@iesabroad.org", "username": "bob", "idnumber": "SF-BOB"}
            ],
        }
        if values and values[0] in users_db:
            return users_db[values[0]]
        return []

    client.get_users_by_field.side_effect = lookup
    return client


@pytest.fixture
def sf_accounts():
    return [
        {"Id": "0015f00000AAA001", "Name": "Alice Smith", "PersonEmail": "alice@iesabroad.org"},
        {"Id": "SF-BOB", "Name": "Bob Jones", "PersonEmail": "bob@iesabroad.org"},
        {"Id": "0015f00000AAA003", "Name": "Charlie Brown", "PersonEmail": "charlie@iesabroad.org"},
        {"Id": "0015f00000AAA004", "Name": "No Email", "PersonEmail": ""},
    ]


def test_match_by_email(sf_accounts, mock_moodle):
    result = match_users_by_email(sf_accounts, mock_moodle)
    assert len(result.matched) == 2
    emails = [m.sf_email for m in result.matched]
    assert "alice@iesabroad.org" in emails
    assert "bob@iesabroad.org" in emails


def test_unmatched_no_moodle_user(sf_accounts, mock_moodle):
    result = match_users_by_email(sf_accounts, mock_moodle)
    unmatched_emails = [u.get("PersonEmail", u.get("Email", "")).strip().lower()
                        for u in result.unmatched_sf if u.get("_reason") == "not_found"]
    assert "charlie@iesabroad.org" in unmatched_emails


def test_unmatched_no_email(sf_accounts, mock_moodle):
    result = match_users_by_email(sf_accounts, mock_moodle)
    no_email = [u for u in result.unmatched_sf if u.get("_reason") == "no_email"]
    assert len(no_email) == 1


def test_already_set(sf_accounts, mock_moodle):
    result = match_users_by_email(sf_accounts, mock_moodle)
    assert len(result.already_set) == 1
    assert result.already_set[0].sf_account_id == "SF-BOB"


def test_dry_run_no_updates(sf_accounts, mock_moodle):
    result = sync_users(sf_accounts, mock_moodle, dry_run=True)
    assert len(result.updated) == 0
    mock_moodle.update_users.assert_not_called()


def test_sync_updates(sf_accounts, mock_moodle):
    result = sync_users(sf_accounts, mock_moodle, dry_run=False)
    assert len(result.updated) == 1  # Only Alice (Bob already set)
    mock_moodle.update_users.assert_called_once()
    call_args = mock_moodle.update_users.call_args[0][0]
    assert call_args[0]["id"] == 101
    assert call_args[0]["idnumber"] == "0015f00000AAA001"
