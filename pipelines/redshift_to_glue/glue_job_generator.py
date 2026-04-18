from __future__ import annotations

from textwrap import dedent


GLUE_JOB_TEMPLATE = dedent("""\
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # ---------- Transformation Logic ----------
    {spark_code}
    # ---------- Write Output ----------
    from awsglue.dynamicframe import DynamicFrame

    output_dyf = DynamicFrame.fromDF(result_df, glueContext, "output_dyf")
    glueContext.write_dynamic_frame.from_options(
        frame=output_dyf,
        connection_type="s3",
        connection_options={{"path": "{output_path}"}},
        format="parquet",
    )

    job.commit()
""")

DEFAULT_GLUE_VERSION = "4.0"
DEFAULT_WORKER_TYPE = "G.1X"
DEFAULT_NUM_WORKERS = 10
DEFAULT_TIMEOUT_MINUTES = 120
DEFAULT_MAX_RETRIES = 1


class GlueJobGenerator:
    """Generates complete AWS Glue PySpark job scripts and job configurations."""

    def __init__(
        self,
        default_output_bucket: str = "",
        glue_version: str = DEFAULT_GLUE_VERSION,
        worker_type: str = DEFAULT_WORKER_TYPE,
        num_workers: int = DEFAULT_NUM_WORKERS,
        connection_name: str = "",
        redshift_tmp_dir: str = "",
    ):
        self.default_output_bucket = default_output_bucket
        self.glue_version = glue_version
        self.worker_type = worker_type
        self.num_workers = num_workers
        self.connection_name = connection_name
        self.redshift_tmp_dir = redshift_tmp_dir

    def generate(
        self,
        view_name: str,
        translated_spark: str,
        source_tables: list[str],
        output_path: str,
    ) -> str:
        indented = "\n    ".join(translated_spark.splitlines())

        header_lines = [
            f"# Glue job generated for view: {view_name}",
            f"# Source tables: {', '.join(source_tables)}",
        ]
        header = "\n    ".join(header_lines)

        return GLUE_JOB_TEMPLATE.format(
            spark_code=f"{header}\n    {indented}",
            output_path=output_path,
        )

    def generate_job_config(self, view_name: str) -> dict:
        job_name = self._job_name(view_name)
        script_location = (
            f"s3://{self.default_output_bucket}/glue-scripts/{job_name}.py"
            if self.default_output_bucket
            else f"s3://glue-scripts/{job_name}.py"
        )

        default_args = {
            "--job-language": "python",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
        }

        config = {
            "Name": job_name,
            "Role": "",  # must be set before deployment
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": "3",
            },
            "GlueVersion": self.glue_version,
            "WorkerType": self.worker_type,
            "NumberOfWorkers": self.num_workers,
            "Timeout": DEFAULT_TIMEOUT_MINUTES,
            "MaxRetries": DEFAULT_MAX_RETRIES,
            "DefaultArguments": default_args,
        }

        if self.connection_name:
            config["Connections"] = {"Connections": [self.connection_name]}
            if self.redshift_tmp_dir:
                default_args["--TempDir"] = self.redshift_tmp_dir

        return config

    @staticmethod
    def _job_name(view_name: str) -> str:
        return view_name.replace(".", "_").replace(" ", "_").lower()
