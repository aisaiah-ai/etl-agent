from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"}


@dataclass
class JobRunResult:
    """Result of a completed Glue job run."""

    job_name: str
    run_id: str
    state: str
    started_on: str = ""
    completed_on: str = ""
    execution_time_seconds: int = 0
    error_message: str = ""
    output_path: str = ""

    @property
    def succeeded(self) -> bool:
        return self.state == "SUCCEEDED"


class GlueJobDeployer:
    """Deploys and manages AWS Glue ETL jobs."""

    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str = "glue-scripts",
        role_arn: str = "",
        region: str = "us-east-1",
    ):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip("/")
        self.role_arn = role_arn
        self.region = region
        self._glue = boto3.client("glue", region_name=region)
        self._s3 = boto3.client("s3", region_name=region)

    def deploy(self, job_name: str, script_content: str, config: dict) -> str:
        script_key = f"{self.s3_prefix}/{job_name}.py"
        script_location = f"s3://{self.s3_bucket}/{script_key}"

        self._s3.put_object(
            Bucket=self.s3_bucket,
            Key=script_key,
            Body=script_content.encode("utf-8"),
            ContentType="text/x-python",
        )
        logger.info("Uploaded script to %s", script_location)

        config["Command"]["ScriptLocation"] = script_location
        if self.role_arn:
            config["Role"] = self.role_arn

        try:
            self._glue.get_job(JobName=job_name)
            update_config = {k: v for k, v in config.items() if k != "Name"}
            self._glue.update_job(JobName=job_name, JobUpdate=update_config)
            logger.info("Updated existing Glue job: %s", job_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                self._glue.create_job(**config)
                logger.info("Created new Glue job: %s", job_name)
            else:
                raise

        return script_location

    def run_job(
        self, job_name: str, arguments: dict[str, str] | None = None
    ) -> str:
        params: dict = {"JobName": job_name}
        if arguments:
            params["Arguments"] = arguments

        response = self._glue.start_job_run(**params)
        run_id = response["JobRunId"]
        logger.info("Started job run %s for %s", run_id, job_name)
        return run_id

    def wait_for_completion(
        self,
        job_name: str,
        run_id: str,
        timeout: int = 3600,
        poll_interval: int = 30,
    ) -> JobRunResult:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            response = self._glue.get_job_run(JobName=job_name, RunId=run_id)
            run = response["JobRun"]
            state = run["JobRunState"]

            if state in TERMINAL_STATES:
                return JobRunResult(
                    job_name=job_name,
                    run_id=run_id,
                    state=state,
                    started_on=str(run.get("StartedOn", "")),
                    completed_on=str(run.get("CompletedOn", "")),
                    execution_time_seconds=run.get("ExecutionTime", 0),
                    error_message=run.get("ErrorMessage", ""),
                )

            logger.info("Job %s run %s state: %s", job_name, run_id, state)
            time.sleep(poll_interval)

        return JobRunResult(
            job_name=job_name,
            run_id=run_id,
            state="TIMEOUT",
            error_message=f"Timed out after {timeout}s",
        )

    def undeploy(self, job_name: str) -> None:
        try:
            self._glue.delete_job(JobName=job_name)
            logger.info("Deleted Glue job: %s", job_name)
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityNotFoundException":
                raise
            logger.warning("Job %s not found, nothing to delete", job_name)

        script_key = f"{self.s3_prefix}/{job_name}.py"
        try:
            self._s3.delete_object(Bucket=self.s3_bucket, Key=script_key)
            logger.info("Deleted script s3://%s/%s", self.s3_bucket, script_key)
        except ClientError:
            logger.warning("Could not delete script for %s", job_name)
