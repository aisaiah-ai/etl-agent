"""End-to-end integration test for the ETL Agent pipeline.

Reads sample SQL, parses it, translates to PySpark, generates a Glue job script,
and verifies the output is syntactically valid Python.

Set ETL_LOCAL=1 to run against Docker Compose services (PostgreSQL + LocalStack).
"""

import ast
import os
import pathlib

import pytest

from pipelines.redshift_to_glue.sql_parser import RedshiftSQLParser
from pipelines.redshift_to_glue.translator import SQLToPySparkTranslator
from pipelines.redshift_to_glue.glue_job_generator import GlueJobGenerator

SAMPLE_SQL_PATH = pathlib.Path(__file__).parent.parent / "test_data" / "sample_views.sql"


class TestEndToEnd:
    """Integration test: SQL -> parse -> translate -> generate -> validate."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.sql_text = SAMPLE_SQL_PATH.read_text()
        self.parser = RedshiftSQLParser()
        self.translator = SQLToPySparkTranslator()
        self.generator = GlueJobGenerator(default_output_bucket="etl-agent-scripts-dev")

    def _extract_view_blocks(self):
        """Split SQL file into individual CREATE VIEW statements."""
        blocks = []
        current = []
        for line in self.sql_text.splitlines():
            if line.strip().upper().startswith("CREATE") and "VIEW" in line.upper():
                if current:
                    blocks.append("\n".join(current))
                current = [line]
            else:
                current.append(line)
        if current:
            blocks.append("\n".join(current))
        return [b for b in blocks if "VIEW" in b.upper()]

    def test_parse_and_translate_each_view(self):
        """Parse each view, translate to PySpark, generate Glue script, verify valid Python."""
        view_blocks = self._extract_view_blocks()
        assert len(view_blocks) >= 1, "Should find at least one CREATE VIEW statement"

        for view_sql in view_blocks:
            parsed = self.parser.parse_view_definition(view_sql)
            pyspark_code = self.translator.translate(parsed)
            assert len(pyspark_code) > 0, "Translation should produce non-empty output"

            glue_script = self.generator.generate(
                view_name="test_view",
                translated_spark=pyspark_code,
                source_tables=parsed.source_tables,
                output_path="s3://test-bucket/output/",
            )

            try:
                ast.parse(glue_script)
            except SyntaxError as exc:
                pytest.fail(f"Generated Glue script is not valid Python: {exc}\n\n{glue_script}")

    def test_source_tables_extracted(self):
        """Parser should find source tables from sample SQL views."""
        view_blocks = self._extract_view_blocks()
        all_tables = set()
        for view_sql in view_blocks:
            parsed = self.parser.parse_view_definition(view_sql)
            all_tables.update(parsed.source_tables)

        # At minimum, patients and claims should appear
        table_names = {t.split(".")[-1] for t in all_tables}
        assert "patients" in table_names or any("patient" in t.lower() for t in all_tables)

    def test_external_tables_detected(self):
        """Parser should detect three-part names as external (Spectrum) tables."""
        # Use a known external table reference
        sql = "SELECT * FROM spectrum_schema.ext_db.payer_plans"
        external = self.parser.detect_external_tables(sql)
        assert len(external) > 0


@pytest.mark.skipif(
    os.environ.get("ETL_LOCAL") != "1",
    reason="Requires Docker Compose services (set ETL_LOCAL=1)",
)
class TestLocalIntegration:
    """Integration tests that require Docker Compose (PostgreSQL + LocalStack)."""

    def test_discover_local_views(self):
        """Connect to local PostgreSQL and discover views."""
        from pipelines.discovery.schema_discovery import SchemaDiscovery

        sd = SchemaDiscovery.local()
        views = sd.discover_redshift_views()
        assert len(views) > 0, "Should find views in local PostgreSQL"

        # Check that we can get columns for each view
        for view in views:
            assert len(view.columns) > 0, f"View {view.full_name} should have columns"

    def test_discover_local_glue_tables(self):
        """Connect to LocalStack and discover Glue catalog tables."""
        from pipelines.discovery.schema_discovery import SchemaDiscovery

        sd = SchemaDiscovery.local()
        tables = sd.discover_glue_tables("etl_agent_local")
        assert len(tables) > 0, "Should find tables in LocalStack Glue catalog"

    def test_run_query_on_local_redshift(self):
        """Execute a query against local PostgreSQL."""
        from pipelines.discovery.schema_discovery import SchemaDiscovery

        sd = SchemaDiscovery.local()
        rows = sd.run_query("SELECT COUNT(*) FROM public.patients")
        assert rows[0][0] > 0, "Should have patient data"

    def test_find_missing_tables(self):
        """Cross-reference views against Glue catalog."""
        from pipelines.discovery.schema_discovery import SchemaDiscovery

        sd = SchemaDiscovery.local()
        views = sd.discover_redshift_views()
        glue_tables = sd.discover_glue_tables("etl_agent_local")
        missing = sd.find_missing_tables(views, glue_tables)
        # This will surface any tables in view definitions that aren't in Glue
        assert isinstance(missing, list)

    def test_full_local_pipeline(self):
        """Full pipeline: discover -> parse -> translate -> generate -> verify syntax."""
        from pipelines.discovery.schema_discovery import SchemaDiscovery

        sd = SchemaDiscovery.local()
        views = sd.discover_redshift_views()

        parser = RedshiftSQLParser()
        translator = SQLToPySparkTranslator()
        generator = GlueJobGenerator(default_output_bucket="etl-agent-data-local")

        for view in views[:3]:  # Limit to first 3 views for speed
            view_ddl = f"CREATE VIEW {view.full_name} AS {view.definition}"
            parsed = parser.parse_view_definition(view_ddl)
            pyspark = translator.translate(parsed)
            script = generator.generate(
                view_name=view.view_name,
                translated_spark=pyspark,
                source_tables=parsed.source_tables,
                output_path=f"s3://etl-agent-data-local/output/{view.view_name}/",
            )

            # Verify valid Python
            ast.parse(script)

            # Verify baseline row count
            rows = sd.run_query(f"SELECT COUNT(*) FROM {view.full_name}")
            assert rows[0][0] >= 0
