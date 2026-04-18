"""Moodle REST API client for course and user lookups/updates."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any
from urllib.parse import urlencode

import boto3
import requests

logger = logging.getLogger(__name__)


class MoodleClient:
    """Thin wrapper around Moodle's REST web-service API.

    Parameters
    ----------
    base_url : str
        Moodle site URL, e.g. ``https://moodle.iesabroad.org``.
    token : str | None
        Web-service token.  If *None*, the client reads from the
        ``MOODLE_TOKEN`` env var or from AWS Secrets Manager
        (secret name given by ``secret_name``).
    secret_name : str | None
        AWS Secrets Manager secret name that holds the Moodle token
        under a ``token`` key.
    region : str
        AWS region for Secrets Manager.
    """

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        secret_name: str | None = None,
        region: str = "us-east-1",
    ):
        self.base_url = (base_url or os.environ.get("MOODLE_URL", "")).rstrip("/")
        self.token = token or os.environ.get("MOODLE_TOKEN")
        if not self.token and secret_name:
            self.token = self._get_token_from_secrets(secret_name, region)
        if not self.base_url:
            raise ValueError("Moodle base_url is required (or set MOODLE_URL env var)")
        if not self.token:
            raise ValueError(
                "Moodle token is required (pass token=, set MOODLE_TOKEN, "
                "or provide secret_name for Secrets Manager)"
            )
        self._endpoint = f"{self.base_url}/webservice/rest/server.php"

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_token_from_secrets(secret_name: str, region: str) -> str:
        client = boto3.client("secretsmanager", region_name=region)
        resp = client.get_secret_value(SecretId=secret_name)
        data = json.loads(resp["SecretString"])
        return data["token"]

    def _call(
        self,
        wsfunction: str,
        *,
        _check_warnings: bool = False,
        _timeout: int | None = None,
        **params: Any,
    ) -> Any:
        """Execute a Moodle web-service function and return the JSON result.

        Parameters
        ----------
        _check_warnings : bool
            If *True*, raise on any ``warnings`` in the response.  Used by
            write operations (update/create) so that permission errors and
            partial failures are not silently swallowed.
        _timeout : int | None
            Request timeout in seconds.  Defaults to 120.
        """
        payload = {
            "wstoken": self.token,
            "wsfunction": wsfunction,
            "moodlewsrestformat": "json",
            **params,
        }
        resp = requests.post(self._endpoint, data=payload, timeout=_timeout or 120)
        resp.raise_for_status()
        result = resp.json()
        if isinstance(result, dict) and result.get("exception"):
            raise RuntimeError(
                f"Moodle API error: {result.get('message', result.get('exception'))}"
            )
        if _check_warnings and isinstance(result, dict) and result.get("warnings"):
            warnings = result["warnings"]
            msgs = "; ".join(
                f"{w.get('item', '')}#{w.get('itemid', '')}: "
                f"{w.get('message', w.get('warningcode', 'unknown'))}"
                for w in warnings
            )
            raise RuntimeError(f"Moodle API warnings ({wsfunction}): {msgs}")
        return result

    # ------------------------------------------------------------------
    # Course operations
    # ------------------------------------------------------------------

    def get_courses(self, timeout: int = 300) -> list[dict]:
        """Return all courses (id, shortname, fullname, idnumber, …).

        Parameters
        ----------
        timeout : int
            Request timeout in seconds.  Default 300 (5 min) since large
            Moodle instances may have thousands of courses.
        """
        return self._call("core_course_get_courses", _timeout=timeout)

    def search_courses(self, query: str, per_page: int = 20) -> list[dict]:
        """Text search for courses by name (uses core_course_search_courses).

        Returns up to *per_page* courses whose shortname or fullname
        contains the query string.
        """
        result = self._call(
            "core_course_search_courses",
            criterianame="search",
            criteriavalue=query,
            perpage=per_page,
        )
        return result.get("courses", [])

    def search_courses_by_field(
        self, field: str = "shortname", value: str = ""
    ) -> list[dict]:
        """Search courses by a specific field (shortname, idnumber, etc.)."""
        result = self._call(
            "core_course_get_courses_by_field", field=field, value=value
        )
        return result.get("courses", [])

    def update_courses(self, courses: list[dict]) -> None:
        """Update one or more courses.

        Each dict in *courses* must have ``id`` and the fields to update,
        e.g. ``{"id": 42, "idnumber": "SF-001"}``.

        Raises ``RuntimeError`` if Moodle returns warnings (e.g. permission
        denied on ``changeidnumber``).
        """
        params: dict[str, Any] = {}
        for i, course in enumerate(courses):
            for key, val in course.items():
                params[f"courses[{i}][{key}]"] = val
        self._call("core_course_update_courses", _check_warnings=True, **params)

    # ------------------------------------------------------------------
    # User operations
    # ------------------------------------------------------------------

    def get_users_by_field(
        self, field: str = "email", values: list[str] | None = None
    ) -> list[dict]:
        """Look up users by a field (email, username, idnumber, …)."""
        values = values or []
        params: dict[str, Any] = {"field": field}
        for i, v in enumerate(values):
            params[f"values[{i}]"] = v
        return self._call("core_user_get_users_by_field", **params)

    def update_users(self, users: list[dict]) -> None:
        """Update one or more users.

        Each dict in *users* must have ``id`` and the fields to update,
        e.g. ``{"id": 99, "idnumber": "0015f00000ABC123"}``.

        Raises ``RuntimeError`` if Moodle returns warnings.
        """
        params: dict[str, Any] = {}
        for i, user in enumerate(users):
            for key, val in user.items():
                params[f"users[{i}][{key}]"] = val
        self._call("core_user_update_users", _check_warnings=True, **params)

    def search_users(self, key: str = "email", value: str = "") -> list[dict]:
        """Search for users using core_user_get_users (criteria-based)."""
        return self._call(
            "core_user_get_users",
            **{
                "criteria[0][key]": key,
                "criteria[0][value]": value,
            },
        ).get("users", [])

    # ------------------------------------------------------------------
    # Create operations (for testing)
    # ------------------------------------------------------------------

    def create_courses(self, courses: list[dict]) -> list[dict]:
        """Create courses in Moodle.

        Each dict should have ``fullname``, ``shortname``, ``categoryid`` (default 1).
        Optional: ``idnumber``, ``summary``.
        Returns list of created courses with ``id`` and ``shortname``.
        """
        params: dict[str, Any] = {}
        for i, course in enumerate(courses):
            course.setdefault("categoryid", 1)
            for key, val in course.items():
                params[f"courses[{i}][{key}]"] = val
        return self._call("core_course_create_courses", **params)

    def create_users(self, users: list[dict]) -> list[dict]:
        """Create users in Moodle.

        Each dict should have ``username``, ``password``, ``firstname``,
        ``lastname``, ``email``.  Optional: ``idnumber``.
        Returns list of created users with ``id`` and ``username``.
        """
        params: dict[str, Any] = {}
        for i, user in enumerate(users):
            for key, val in user.items():
                params[f"users[{i}][{key}]"] = val
        return self._call("core_user_create_users", **params)
