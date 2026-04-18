"""Tests for MigrationVerifier — schema and row-count validation."""

import pytest

from pipelines.publish import MigrationVerifier


class TestMigrationVerifier:
    """Tests for schema comparison and row-count tolerance checks."""

    @pytest.fixture()
    def verifier(self):
        return MigrationVerifier()

    # ── Schema matching ──────────────────────────────────────────────────────

    def test_schema_match(self, verifier):
        """Identical schemas should pass verification."""
        source_schema = {
            "columns": [
                {"name": "patient_id", "type": "INTEGER"},
                {"name": "first_name", "type": "VARCHAR"},
                {"name": "last_name", "type": "VARCHAR"},
                {"name": "age", "type": "INTEGER"},
            ]
        }
        target_schema = {
            "columns": [
                {"name": "patient_id", "type": "INTEGER"},
                {"name": "first_name", "type": "STRING"},
                {"name": "last_name", "type": "STRING"},
                {"name": "age", "type": "INTEGER"},
            ]
        }
        result = verifier.compare_schemas(source_schema, target_schema)
        assert result["match"] is True
        assert len(result.get("mismatches", [])) == 0

    def test_schema_mismatch(self, verifier):
        """Missing or extra columns should be flagged as mismatches."""
        source_schema = {
            "columns": [
                {"name": "patient_id", "type": "INTEGER"},
                {"name": "first_name", "type": "VARCHAR"},
                {"name": "last_name", "type": "VARCHAR"},
                {"name": "age", "type": "INTEGER"},
            ]
        }
        target_schema = {
            "columns": [
                {"name": "patient_id", "type": "INTEGER"},
                {"name": "first_name", "type": "STRING"},
                # last_name is missing
                {"name": "age", "type": "INTEGER"},
                {"name": "extra_col", "type": "STRING"},  # unexpected
            ]
        }
        result = verifier.compare_schemas(source_schema, target_schema)
        assert result["match"] is False
        assert len(result.get("mismatches", [])) > 0

    # ── Row-count tolerance ──────────────────────────────────────────────────

    def test_row_count_within_tolerance(self, verifier):
        """Row counts within the tolerance threshold should pass."""
        result = verifier.check_row_counts(
            source_count=10000,
            target_count=10050,
            tolerance_pct=1.0,
        )
        assert result["pass"] is True

    def test_row_count_exceeds_tolerance(self, verifier):
        """Row counts exceeding the tolerance should fail."""
        result = verifier.check_row_counts(
            source_count=10000,
            target_count=11500,
            tolerance_pct=1.0,
        )
        assert result["pass"] is False
        assert result["deviation_pct"] > 1.0
