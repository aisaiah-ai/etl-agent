"""Unit tests for SF → Moodle course sync."""

import pytest

from pipelines.sf_moodle_sync.course_sync import match_courses


@pytest.fixture
def moodle_courses():
    return [
        {"id": 1, "shortname": "HIST 101", "fullname": "History 101 - Fall 2025", "idnumber": ""},
        {"id": 2, "shortname": "ENG 200", "fullname": "English Literature", "idnumber": ""},
        {"id": 3, "shortname": "ART 300", "fullname": "Art History", "idnumber": "SF-EXIST"},
        {"id": 4, "shortname": "MATH 101", "fullname": "Intro to Mathematics", "idnumber": ""},
    ]


@pytest.fixture
def sf_offerings():
    return [
        {"Id": "a0B5f00000ABC001", "Name": "HIST 101"},
        {"Id": "a0B5f00000ABC002", "Name": "English Literature"},
        {"Id": "a0B5f00000ABC003", "Name": "ART 300"},
        {"Id": "a0B5f00000ABC004", "Name": "Biology 101"},  # No match
    ]


def test_match_by_shortname(sf_offerings, moodle_courses):
    result = match_courses(sf_offerings, moodle_courses)
    shortname_matches = [m for m in result.matched if m.matched_on == "shortname"]
    assert any(m.sf_offering_id == "a0B5f00000ABC001" for m in shortname_matches)
    assert any(m.moodle_shortname == "HIST 101" for m in shortname_matches)


def test_match_by_fullname(sf_offerings, moodle_courses):
    result = match_courses(sf_offerings, moodle_courses)
    fullname_matches = [m for m in result.matched if m.matched_on == "fullname"]
    assert any(m.sf_offering_id == "a0B5f00000ABC002" for m in fullname_matches)


def test_unmatched(sf_offerings, moodle_courses):
    result = match_courses(sf_offerings, moodle_courses)
    unmatched_ids = [u["Id"] for u in result.unmatched_sf]
    assert "a0B5f00000ABC004" in unmatched_ids


def test_already_set(moodle_courses):
    sf = [{"Id": "SF-EXIST", "Name": "ART 300"}]
    result = match_courses(sf, moodle_courses)
    assert len(result.already_set) == 1
    assert result.already_set[0].sf_offering_id == "SF-EXIST"


def test_case_insensitive_match(moodle_courses):
    sf = [{"Id": "a0B5f00000ABC005", "Name": "hist 101"}]
    result = match_courses(sf, moodle_courses)
    assert len(result.matched) == 1
    assert result.matched[0].moodle_course_id == 1


def test_empty_inputs():
    result = match_courses([], [])
    assert len(result.matched) == 0
    assert len(result.unmatched_sf) == 0
