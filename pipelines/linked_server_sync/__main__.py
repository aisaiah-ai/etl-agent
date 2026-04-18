"""Deploy and run the linked-server sync Glue jobs.

Usage:
    python3 -m pipelines.linked_server_sync --deploy --env uat
    python3 -m pipelines.linked_server_sync --run-extract --env uat
    python3 -m pipelines.linked_server_sync --run-join --env uat
    python3 -m pipelines.linked_server_sync --run-all --env uat
"""

from __future__ import annotations

import argparse
import logging
import os
import time

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

EXTRACT_SCRIPT = "pipelines/linked_server_sync/extract_tables.py"
JOIN_SCRIPT = "pipelines/linked_server_sync/message_case_join.py"

EXTRACT_JOB = "glue-etl-tlmain-extract-tables"
JOIN_JOB = "glue-etl-tlmain-message-case-join"

# Glue connection provides VPC networking to reach SQL Server
GLUE_CONNECTION = "glue-etl-rds-prod-new"



def deploy_scripts(bucket: str, role_arn: str, env: str):
    """Upload Glue scripts to S3 and create/update Glue jobs."""
    s3 = boto3.client("s3")
    glue = boto3.client("glue")

    for script_path, job_name, extra_args, connections in [
        (EXTRACT_SCRIPT, EXTRACT_JOB, {"--ENV": env}, [GLUE_CONNECTION]),
        (JOIN_SCRIPT, JOIN_JOB, {"--ENV": env}, []),
    ]:
        s3_key = f"glue-scripts/{job_name}.py"
        s3.upload_file(script_path, bucket, s3_key)
        s3_path = f"s3://{bucket}/{s3_key}"
        logger.info("Uploaded %s → %s", script_path, s3_path)

        job_update = {
            "Role": role_arn,
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": s3_path,
                "PythonVersion": "3",
            },
            "GlueVersion": "4.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 4,
            "DefaultArguments": {
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--datalake-formats": "delta",
                **extra_args,
            },
            "Timeout": 480,
            "MaxRetries": 0,
            **({"Connections": {"Connections": connections}} if connections else {}),
        }

        try:
            glue.update_job(JobName=job_name, JobUpdate=job_update)
            logger.info("Updated Glue job: %s", job_name)
        except glue.exceptions.EntityNotFoundException:
            glue.create_job(Name=job_name, **job_update)
            logger.info("Created Glue job: %s", job_name)


def wait_for_job(glue, job_name: str, run_id: str, poll_interval: int = 30):
    """Poll until job run completes."""
    while True:
        resp = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = resp["JobRun"]["JobRunState"]
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"):
            logger.info("Job %s run %s finished: %s", job_name, run_id, state)
            return state
        logger.info("Job %s run %s: %s ...", job_name, run_id, state)
        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="tlMain287 Linked-Server Glue Pipeline")
    parser.add_argument("--deploy", action="store_true", help="Deploy Glue jobs")
    parser.add_argument("--run-extract", action="store_true", help="Run extraction job")
    parser.add_argument("--run-join", action="store_true", help="Run join job")
    parser.add_argument("--run-all", action="store_true", help="Run extract then join")
    parser.add_argument("--env", default="uat", choices=["uat", "prd", "prod", "prod_replica"])
    parser.add_argument("--bucket", default=os.environ.get("ARTIFACTS_BUCKET", "aws-glue-assets-442594162630-us-east-1"))
    parser.add_argument("--role-arn", default=os.environ.get("GLUE_ROLE_ARN", ""))
    parser.add_argument("--tables", default="all", help="Comma-separated tables to extract (e.g. tblcase,tblmessage). Default: all")
    parser.add_argument("--wait", action="store_true", help="Wait for job completion")
    args = parser.parse_args()

    glue = boto3.client("glue")

    if args.deploy:
        deploy_scripts(args.bucket, args.role_arn, args.env)

    if args.run_extract or args.run_all:
        logger.info("Starting extraction job...")
        run = glue.start_job_run(
            JobName=EXTRACT_JOB,
            Arguments={"--ENV": args.env, "--TABLES": args.tables},
        )
        run_id = run["JobRunId"]
        logger.info("Extract job run: %s", run_id)
        if args.wait or args.run_all:
            state = wait_for_job(glue, EXTRACT_JOB, run_id)
            if state != "SUCCEEDED":
                logger.error("Extract job failed — skipping join")
                return

    if args.run_join or args.run_all:
        logger.info("Starting join job...")
        run = glue.start_job_run(
            JobName=JOIN_JOB,
            Arguments={"--ENV": args.env},
        )
        run_id = run["JobRunId"]
        logger.info("Join job run: %s", run_id)
        if args.wait:
            wait_for_job(glue, JOIN_JOB, run_id)


if __name__ == "__main__":
    main()
