"""Tests for GlueJobGenerator — Glue job script and config generation."""

import pytest

from pipelines.redshift_to_glue.glue_job_generator import GlueJobGenerator


SAMPLE_PYSPARK = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

spark = SparkSession.builder.getOrCreate()

patients_df = spark.table("etl_agent_db.patients")

vw_patient_demographics_df = (
    patients_df.alias("p")
    .filter(col("p.is_active") == 1)
    .select(
        col("p.patient_id"),
        col("p.first_name"),
        col("p.last_name"),
        coalesce(col("p.primary_language"), lit("English")).alias("primary_language"),
    )
)
"""


class TestGlueJobGenerator:
    """Tests for Glue job script and configuration generation."""

    @pytest.fixture()
    def generator(self):
        return GlueJobGenerator(
            database_name="etl_agent_db",
            role_name="etl-agent-glue-role",
            script_bucket="etl-agent-scripts-dev",
        )

    # ── Script generation ────────────────────────────────────────────────────

    def test_generate_produces_valid_script(self, generator):
        """Generated script should be syntactically valid Python."""
        script = generator.generate_script(
            view_name="vw_patient_demographics",
            pyspark_code=SAMPLE_PYSPARK,
        )
        compile(script, "<glue_script>", "exec")

    def test_generate_includes_glue_context(self, generator):
        """Generated script should include GlueContext and job lifecycle calls."""
        script = generator.generate_script(
            view_name="vw_patient_demographics",
            pyspark_code=SAMPLE_PYSPARK,
        )
        lower = script.lower()
        assert "gluecontext" in lower or "glue_context" in lower
        assert "job.init" in lower or "job_init" in lower
        assert "job.commit" in lower or "job_commit" in lower

    def test_generate_includes_imports(self, generator):
        """Generated script should import awsglue modules."""
        script = generator.generate_script(
            view_name="vw_patient_demographics",
            pyspark_code=SAMPLE_PYSPARK,
        )
        assert "awsglue" in script

    # ── Job config ───────────────────────────────────────────────────────────

    def test_generate_job_config(self, generator):
        """Job config should include required Glue job parameters."""
        config = generator.generate_job_config(
            view_name="vw_patient_demographics",
        )
        assert config["job_name"] == "etl-agent-vw_patient_demographics"
        assert config["role_name"] == "etl-agent-glue-role"
        assert "script_location" in config
        assert config["script_location"].startswith("s3://")
        assert config.get("glue_version") is not None
        assert config.get("worker_type") is not None

    def test_generate_job_config_defaults(self, generator):
        """Job config should have sensible defaults for worker settings."""
        config = generator.generate_job_config(
            view_name="vw_test_view",
        )
        assert config.get("number_of_workers", 0) >= 2
        assert config.get("timeout", 0) > 0


class TestGlueJobGeneratorJDBC:
    """Tests for Glue job config with JDBC connection."""

    @pytest.fixture()
    def jdbc_generator(self):
        return GlueJobGenerator(
            default_output_bucket="etl-agent-scripts-dev",
            connection_name="etl-agent-redshift",
            redshift_tmp_dir="s3://etl-agent-artifacts/redshift-tmp/",
        )

    def test_jdbc_config_includes_connection(self, jdbc_generator):
        """Job config with connection_name should include Connections block."""
        config = jdbc_generator.generate_job_config(view_name="vw_test_view")
        assert "Connections" in config
        assert "etl-agent-redshift" in config["Connections"]["Connections"]

    def test_jdbc_config_includes_temp_dir(self, jdbc_generator):
        """Job config with JDBC should set --TempDir."""
        config = jdbc_generator.generate_job_config(view_name="vw_test_view")
        assert config["DefaultArguments"]["--TempDir"] == "s3://etl-agent-artifacts/redshift-tmp/"

    def test_no_connection_by_default(self):
        """Default generator should not include Connections in config."""
        generator = GlueJobGenerator(default_output_bucket="bucket")
        config = generator.generate_job_config(view_name="vw_test")
        assert "Connections" not in config
