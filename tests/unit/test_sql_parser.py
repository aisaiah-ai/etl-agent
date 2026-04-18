"""Tests for RedshiftSQLParser — SQL parsing and metadata extraction."""

import pytest

from pipelines.discovery import RedshiftSQLParser


SIMPLE_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_patient_demographics AS
SELECT p.patient_id, p.first_name, p.last_name
FROM public.patients p
WHERE p.is_active = 1;
"""

JOIN_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_claims_enriched AS
SELECT c.claim_id, c.patient_id, p.first_name, pr.provider_name
FROM public.claims c
INNER JOIN public.patients p ON c.patient_id = p.patient_id
INNER JOIN public.providers pr ON c.provider_id = pr.provider_id;
"""

SUBQUERY_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_top_claims AS
SELECT claim_id, billed_amount
FROM (
    SELECT claim_id, billed_amount
    FROM public.claims
    WHERE billed_amount > 1000
) sub
INNER JOIN public.patients p ON sub.patient_id = p.patient_id;
"""

SPECTRUM_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_claims_with_payer AS
SELECT c.claim_id, ext.payer_name
FROM public.claims c
LEFT JOIN spectrum_schema.payer_plans ext ON c.payer_plan_id = ext.plan_id;
"""

DEPENDENT_VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_high_cost_claims AS
SELECT ce.claim_id, ce.billed_amount
FROM public.vw_claims_enriched ce
WHERE ce.billed_amount > 10000;
"""

COLUMN_LINEAGE_SQL = """
CREATE OR REPLACE VIEW public.vw_patient_demographics AS
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    DATEDIFF(year, p.date_of_birth, GETDATE()) AS age,
    NVL(p.primary_language, 'English') AS primary_language
FROM public.patients p
WHERE p.is_active = 1;
"""


class TestRedshiftSQLParser:
    """Tests for source table extraction, external table detection, and lineage."""

    @pytest.fixture()
    def parser(self):
        return RedshiftSQLParser()

    # ── extract_source_tables ────────────────────────────────────────────────

    def test_extract_source_tables_single(self, parser):
        """Single-table SELECT should return one source table."""
        result = parser.parse(SIMPLE_VIEW_SQL)
        tables = result["source_tables"]
        assert "public.patients" in tables

    def test_extract_source_tables_joins(self, parser):
        """JOIN query should list all joined tables."""
        result = parser.parse(JOIN_VIEW_SQL)
        tables = result["source_tables"]
        assert "public.claims" in tables
        assert "public.patients" in tables
        assert "public.providers" in tables

    def test_extract_source_tables_subqueries(self, parser):
        """Tables inside subqueries should still be discovered."""
        result = parser.parse(SUBQUERY_VIEW_SQL)
        tables = result["source_tables"]
        assert "public.claims" in tables
        assert "public.patients" in tables

    # ── detect_external_tables ───────────────────────────────────────────────

    def test_detect_external_tables_spectrum(self, parser):
        """Tables under spectrum_schema should be flagged as external."""
        result = parser.parse(SPECTRUM_VIEW_SQL)
        external = result.get("external_tables", [])
        assert "spectrum_schema.payer_plans" in external

    def test_no_external_tables_when_absent(self, parser):
        """Standard schema references should not appear in external_tables."""
        result = parser.parse(SIMPLE_VIEW_SQL)
        external = result.get("external_tables", [])
        assert len(external) == 0

    # ── parse_view_definition ────────────────────────────────────────────────

    def test_parse_view_definition(self, parser):
        """Parser should extract the view name and body."""
        result = parser.parse(SIMPLE_VIEW_SQL)
        views = result.get("views", [])
        assert len(views) >= 1
        view = views[0]
        assert view["name"] == "vw_patient_demographics"
        assert view["schema"] == "public"
        assert "SELECT" in view["body"].upper()

    def test_parse_view_dependency(self, parser):
        """View referencing another view should list it as a dependency."""
        result = parser.parse(DEPENDENT_VIEW_SQL)
        tables = result["source_tables"]
        # vw_claims_enriched is a view, but at the SQL level it appears as a table reference
        assert "public.vw_claims_enriched" in tables

    # ── extract_column_lineage ───────────────────────────────────────────────

    def test_extract_column_lineage(self, parser):
        """Column lineage should map output columns to source expressions."""
        result = parser.parse(COLUMN_LINEAGE_SQL)
        views = result.get("views", [])
        assert len(views) >= 1
        columns = views[0].get("columns", [])
        column_names = [c["name"] for c in columns]
        assert "patient_id" in column_names
        assert "age" in column_names
        assert "primary_language" in column_names
