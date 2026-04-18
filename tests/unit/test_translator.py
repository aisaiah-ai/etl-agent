"""Tests for SQLToPySparkTranslator — Redshift SQL to PySpark code generation."""

import pytest

from pipelines.redshift_to_glue.translator import SQLToPySparkTranslator


SIMPLE_SELECT_SQL = """
CREATE OR REPLACE VIEW public.vw_patient_demographics AS
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    NVL(p.primary_language, 'English') AS primary_language
FROM public.patients p
WHERE p.is_active = 1;
"""

JOIN_SQL = """
CREATE OR REPLACE VIEW public.vw_claims_enriched AS
SELECT
    c.claim_id,
    c.patient_id,
    p.first_name || ' ' || p.last_name AS patient_name,
    pr.provider_name
FROM public.claims c
INNER JOIN public.patients p ON c.patient_id = p.patient_id
INNER JOIN public.providers pr ON c.provider_id = pr.provider_id;
"""

AGGREGATION_SQL = """
CREATE OR REPLACE VIEW public.vw_monthly_claim_summary AS
SELECT
    DATE_TRUNC('month', c.service_date) AS claim_month,
    COUNT(*) AS claim_count,
    SUM(c.billed_amount) AS total_billed,
    MEDIAN(c.paid_amount) AS median_paid
FROM public.claims c
GROUP BY DATE_TRUNC('month', c.service_date)
HAVING COUNT(*) >= 5;
"""

REDSHIFT_FUNCTIONS_SQL = """
CREATE OR REPLACE VIEW public.vw_redshift_funcs AS
SELECT
    NVL(p.primary_language, 'English')          AS lang,
    GETDATE()                                   AS current_ts,
    DATEDIFF(year, p.date_of_birth, GETDATE())  AS age,
    CONVERT_TIMEZONE('UTC', 'US/Eastern', GETDATE()) AS eastern_time,
    DECODE(p.gender, 'M', 'Male', 'F', 'Female', 'Other') AS gender_label
FROM public.patients p;
"""


class TestSQLToPySparkTranslator:
    """Tests for SQL-to-PySpark translation accuracy."""

    @pytest.fixture()
    def translator(self):
        return SQLToPySparkTranslator()

    # ── Simple SELECT ────────────────────────────────────────────────────────

    def test_simple_select_translation(self, translator):
        """Simple SELECT should produce valid PySpark with spark.table() call."""
        result = translator.translate(SIMPLE_SELECT_SQL)
        assert "spark" in result.lower()
        assert "patients" in result.lower()
        # Should reference the output view name
        assert "vw_patient_demographics" in result

    def test_simple_select_produces_valid_python(self, translator):
        """Translated output should be syntactically valid Python."""
        result = translator.translate(SIMPLE_SELECT_SQL)
        compile(result, "<translated>", "exec")  # raises SyntaxError if invalid

    # ── JOINs ────────────────────────────────────────────────────────────────

    def test_join_translation(self, translator):
        """JOIN query should produce PySpark join operations."""
        result = translator.translate(JOIN_SQL)
        assert "join" in result.lower()
        assert "patients" in result.lower()
        assert "providers" in result.lower()
        assert "claims" in result.lower()

    def test_join_translation_valid_python(self, translator):
        """Translated JOIN output should be syntactically valid Python."""
        result = translator.translate(JOIN_SQL)
        compile(result, "<translated>", "exec")

    # ── Aggregations ─────────────────────────────────────────────────────────

    def test_aggregation_translation(self, translator):
        """Aggregation query should include groupBy and agg functions."""
        result = translator.translate(AGGREGATION_SQL)
        lower = result.lower()
        assert "groupby" in lower or "group_by" in lower or "groupBy" in result
        assert "count" in lower
        assert "sum" in lower

    def test_aggregation_translation_valid_python(self, translator):
        """Translated aggregation output should be syntactically valid Python."""
        result = translator.translate(AGGREGATION_SQL)
        compile(result, "<translated>", "exec")

    # ── Redshift function mapping ────────────────────────────────────────────

    def test_redshift_nvl_to_coalesce(self, translator):
        """NVL() should be translated to coalesce()."""
        result = translator.translate(REDSHIFT_FUNCTIONS_SQL)
        assert "coalesce" in result.lower()
        assert "nvl(" not in result.lower()

    def test_redshift_getdate_to_current_timestamp(self, translator):
        """GETDATE() should be translated to current_timestamp()."""
        result = translator.translate(REDSHIFT_FUNCTIONS_SQL)
        assert "current_timestamp" in result.lower()
        assert "getdate(" not in result.lower()

    def test_redshift_decode_to_when(self, translator):
        """DECODE() should be translated to a when/otherwise chain or CASE expression."""
        result = translator.translate(REDSHIFT_FUNCTIONS_SQL)
        lower = result.lower()
        # Accept either .when().otherwise() or SQL CASE expression
        assert "when" in lower or "case" in lower
        assert "decode(" not in lower

    def test_redshift_functions_valid_python(self, translator):
        """Translated Redshift function output should be syntactically valid Python."""
        result = translator.translate(REDSHIFT_FUNCTIONS_SQL)
        compile(result, "<translated>", "exec")


class TestJDBCReadMode:
    """Tests for JDBC source read mode."""

    @pytest.fixture()
    def jdbc_translator(self):
        return SQLToPySparkTranslator(
            source_read_mode="jdbc",
            connection_name="etl-agent-redshift",
            redshift_tmp_dir="s3://bucket/redshift-tmp/",
        )

    @pytest.fixture()
    def catalog_translator(self):
        return SQLToPySparkTranslator(source_read_mode="catalog")

    def test_jdbc_mode_uses_from_options(self, jdbc_translator):
        """JDBC mode should generate from_options with connection_type='redshift'."""
        result = jdbc_translator.translate(SIMPLE_SELECT_SQL)
        assert "from_options" in result
        assert 'connection_type="redshift"' in result
        assert 'connectionName' in result
        assert "etl-agent-redshift" in result

    def test_jdbc_mode_includes_tmp_dir(self, jdbc_translator):
        """JDBC mode should include redshiftTmpDir."""
        result = jdbc_translator.translate(SIMPLE_SELECT_SQL)
        assert "redshiftTmpDir" in result
        assert "s3://bucket/redshift-tmp/" in result

    def test_jdbc_mode_includes_dbtable(self, jdbc_translator):
        """JDBC mode should reference the source table as dbtable."""
        result = jdbc_translator.translate(SIMPLE_SELECT_SQL)
        assert "dbtable" in result
        assert "patients" in result

    def test_catalog_mode_uses_from_catalog(self, catalog_translator):
        """Catalog mode should generate from_catalog (default behavior)."""
        result = catalog_translator.translate(SIMPLE_SELECT_SQL)
        assert "from_catalog" in result
        assert "from_options" not in result

    def test_invalid_read_mode_raises(self):
        """Invalid source_read_mode should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid source_read_mode"):
            SQLToPySparkTranslator(source_read_mode="invalid")

    def test_default_is_catalog(self):
        """Default read mode should be catalog."""
        translator = SQLToPySparkTranslator()
        assert translator.source_read_mode == "catalog"

    def test_jdbc_join_query(self, jdbc_translator):
        """JDBC mode should work with JOIN queries."""
        result = jdbc_translator.translate(JOIN_SQL)
        assert "from_options" in result
        assert "join" in result.lower()
        # Should have multiple from_options calls (one per source table)
        assert result.count("from_options") >= 2
