"""Local development configuration — auto-detects local vs AWS environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

import boto3


@dataclass
class RedshiftConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class AWSConfig:
    region: str
    endpoint_url: str | None
    glue_database: str


def is_local() -> bool:
    return os.environ.get("ETL_LOCAL", "0") == "1"


def get_redshift_config() -> RedshiftConfig:
    if is_local():
        return RedshiftConfig(
            host=os.environ.get("REDSHIFT_HOST", "localhost"),
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ.get("REDSHIFT_DATABASE", "etl_agent_db"),
            user=os.environ.get("REDSHIFT_USER", "admin"),
            password=os.environ.get("REDSHIFT_PASSWORD", "local_dev_password"),
        )
    return RedshiftConfig(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        database=os.environ["REDSHIFT_DATABASE"],
        user=os.environ.get("REDSHIFT_USER", "admin"),
        password=os.environ["REDSHIFT_PASSWORD"],
    )


def get_aws_config() -> AWSConfig:
    if is_local():
        return AWSConfig(
            region="us-east-1",
            endpoint_url=os.environ.get("LOCALSTACK_URL", "http://localhost:4566"),
            glue_database=os.environ.get("GLUE_DATABASE", "etl_agent_local"),
        )
    return AWSConfig(
        region=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=None,
        glue_database=os.environ.get("GLUE_DATABASE", "etl_agent_dev"),
    )


def get_glue_client(config: AWSConfig | None = None):
    cfg = config or get_aws_config()
    kwargs = {"region_name": cfg.region}
    if cfg.endpoint_url:
        kwargs["endpoint_url"] = cfg.endpoint_url
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    return boto3.client("glue", **kwargs)


def get_s3_client(config: AWSConfig | None = None):
    cfg = config or get_aws_config()
    kwargs = {"region_name": cfg.region}
    if cfg.endpoint_url:
        kwargs["endpoint_url"] = cfg.endpoint_url
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    return boto3.client("s3", **kwargs)
