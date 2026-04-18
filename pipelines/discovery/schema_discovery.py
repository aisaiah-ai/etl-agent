from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

import boto3

logger = logging.getLogger(__name__)

VIEW_DDL_QUERY = """
SELECT
    schemaname,
    viewname,
    definition
FROM pg_views
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, viewname;
"""

VIEW_DDL_SINGLE = """
SELECT definition
FROM pg_views
WHERE viewname = %s;
"""

REDSHIFT_COLUMNS_QUERY = """
SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.ordinal_position,
    c.is_nullable,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale
FROM information_schema.columns c
JOIN pg_views v ON c.table_name = v.viewname AND c.table_schema = v.schemaname
WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
"""

# Query to list all tables (not just views) — useful for discovering source tables
ALL_TABLES_QUERY = """
SELECT
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
"""

TABLE_COLUMNS_QUERY = """
SELECT
    c.column_name,
    c.data_type,
    c.ordinal_position,
    c.is_nullable,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale
FROM information_schema.columns c
WHERE c.table_schema = %s AND c.table_name = %s
ORDER BY c.ordinal_position;
"""


@dataclass
class Column:
    """Represents a single column in a table or view."""

    name: str
    data_type: str
    ordinal_position: int = 0
    is_nullable: bool = True
    max_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None


@dataclass
class ViewDefinition:
    """Represents a Redshift view and its metadata."""

    schema_name: str
    view_name: str
    definition: str
    columns: list[Column] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.view_name}"


@dataclass
class TableDefinition:
    """Represents a Glue catalog table."""

    database: str
    table_name: str
    columns: list[Column] = field(default_factory=list)
    location: str = ""
    input_format: str = ""
    serde: str = ""
    partition_keys: list[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.table_name}"


def _is_local() -> bool:
    return os.environ.get("ETL_LOCAL", "0") == "1"


def _get_db_connection(host, port, database, user, password):
    """Connect using psycopg2 (local PostgreSQL) or redshift_connector (AWS Redshift)."""
    if _is_local():
        try:
            import psycopg2

            return psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
            )
        except ImportError:
            logger.info("psycopg2 not available, falling back to redshift_connector")

    import redshift_connector

    return redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


class SchemaDiscovery:
    """Discovers schemas from Redshift views and AWS Glue Data Catalog tables.

    In local mode (ETL_LOCAL=1), connects to PostgreSQL on localhost:5439 and
    LocalStack Glue on localhost:4566. In AWS mode, connects to real Redshift
    and Glue services.
    """

    def __init__(
        self,
        redshift_host: str = "",
        redshift_port: int = 5439,
        redshift_database: str = "",
        redshift_user: str = "",
        redshift_password: str = "",
        aws_region: str = "us-east-1",
        glue_endpoint_url: str | None = None,
    ):
        self._rs_host = redshift_host or os.environ.get("REDSHIFT_HOST", "localhost")
        self._rs_port = redshift_port or int(os.environ.get("REDSHIFT_PORT", "5439"))
        self._rs_database = redshift_database or os.environ.get("REDSHIFT_DATABASE", "etl_agent_db")
        self._rs_user = redshift_user or os.environ.get("REDSHIFT_USER", "admin")
        self._rs_password = redshift_password or os.environ.get("REDSHIFT_PASSWORD", "local_dev_password")
        self._region = aws_region
        self._glue_endpoint_url = glue_endpoint_url
        self._glue_client = None

    @classmethod
    def local(cls) -> SchemaDiscovery:
        """Factory for local development — connects to Docker Compose services."""
        return cls(
            redshift_host="localhost",
            redshift_port=5439,
            redshift_database="etl_agent_db",
            redshift_user="admin",
            redshift_password="local_dev_password",
            glue_endpoint_url="http://localhost:4566",
        )

    def _get_connection(self):
        return _get_db_connection(
            self._rs_host,
            self._rs_port,
            self._rs_database,
            self._rs_user,
            self._rs_password,
        )

    def _get_glue_client(self):
        if self._glue_client is None:
            kwargs = {"region_name": self._region}
            if self._glue_endpoint_url:
                kwargs["endpoint_url"] = self._glue_endpoint_url
                kwargs["aws_access_key_id"] = "test"
                kwargs["aws_secret_access_key"] = "test"
            self._glue_client = boto3.client("glue", **kwargs)
        return self._glue_client

    def discover_redshift_views(
        self, connection=None,
    ) -> list[ViewDefinition]:
        conn = connection or self._get_connection()
        own_conn = connection is None
        try:
            views = self._fetch_views(conn)
            columns_map = self._fetch_view_columns(conn)
            for view in views:
                key = (view.schema_name, view.view_name)
                view.columns = columns_map.get(key, [])
            return views
        finally:
            if own_conn:
                conn.close()

    def discover_glue_tables(self, database: str) -> list[TableDefinition]:
        client = self._get_glue_client()
        tables: list[TableDefinition] = []
        paginator = client.get_paginator("get_tables")

        for page in paginator.paginate(DatabaseName=database):
            for tbl in page["TableList"]:
                columns = [
                    Column(
                        name=col["Name"],
                        data_type=col["Type"],
                        ordinal_position=i,
                    )
                    for i, col in enumerate(tbl.get("StorageDescriptor", {}).get("Columns", []), 1)
                ]
                sd = tbl.get("StorageDescriptor", {})
                partition_keys = [pk["Name"] for pk in tbl.get("PartitionKeys", [])]

                tables.append(
                    TableDefinition(
                        database=database,
                        table_name=tbl["Name"],
                        columns=columns,
                        location=sd.get("Location", ""),
                        input_format=sd.get("InputFormat", ""),
                        serde=sd.get("SerdeInfo", {}).get("SerializationLibrary", ""),
                        partition_keys=partition_keys,
                    )
                )
        return tables

    def find_missing_tables(
        self,
        redshift_views: list[ViewDefinition],
        glue_tables: list[TableDefinition],
    ) -> list[str]:
        from pipelines.redshift_to_glue.sql_parser import RedshiftSQLParser

        parser = RedshiftSQLParser()
        glue_names = {t.table_name.lower() for t in glue_tables}

        referenced: set[str] = set()
        for view in redshift_views:
            tables = parser.extract_source_tables(view.definition)
            for t in tables:
                table_name = t.split(".")[-1].lower()
                referenced.add(table_name)

        missing = sorted(referenced - glue_names)
        return missing

    def discover_redshift_tables(self, connection=None) -> list[dict]:
        """List all tables and views in the database (useful for local exploration)."""
        conn = connection or self._get_connection()
        own_conn = connection is None
        try:
            with conn.cursor() as cur:
                cur.execute(ALL_TABLES_QUERY)
                return [
                    {"schema": row[0], "name": row[1], "type": row[2]}
                    for row in cur.fetchall()
                ]
        finally:
            if own_conn:
                conn.close()

    def get_table_columns(
        self, schema_name: str, table_name: str, connection=None
    ) -> list[Column]:
        """Get columns for a specific table or view."""
        conn = connection or self._get_connection()
        own_conn = connection is None
        try:
            with conn.cursor() as cur:
                cur.execute(TABLE_COLUMNS_QUERY, (schema_name, table_name))
                return [
                    Column(
                        name=row[0],
                        data_type=row[1],
                        ordinal_position=row[2],
                        is_nullable=row[3] == "YES",
                        max_length=row[4],
                        numeric_precision=row[5],
                        numeric_scale=row[6],
                    )
                    for row in cur.fetchall()
                ]
        finally:
            if own_conn:
                conn.close()

    def run_query(self, sql: str, connection=None) -> list[tuple]:
        """Execute an arbitrary read-only query and return results."""
        conn = connection or self._get_connection()
        own_conn = connection is None
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            if own_conn:
                conn.close()

    def get_view_ddl(
        self,
        view_name: str,
        connection=None,
    ) -> str:
        conn = connection or self._get_connection()
        own_conn = connection is None
        try:
            with conn.cursor() as cur:
                cur.execute(VIEW_DDL_SINGLE, (view_name,))
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"View not found: {view_name}")
                return row[0]
        finally:
            if own_conn:
                conn.close()

    # -- internal helpers --

    @staticmethod
    def _fetch_views(conn) -> list[ViewDefinition]:
        with conn.cursor() as cur:
            cur.execute(VIEW_DDL_QUERY)
            return [
                ViewDefinition(
                    schema_name=row[0],
                    view_name=row[1],
                    definition=row[2],
                )
                for row in cur.fetchall()
            ]

    @staticmethod
    def _fetch_view_columns(conn) -> dict[tuple[str, str], list[Column]]:
        columns_map: dict[tuple[str, str], list[Column]] = {}
        with conn.cursor() as cur:
            cur.execute(REDSHIFT_COLUMNS_QUERY)
            for row in cur.fetchall():
                key = (row[0], row[1])
                col = Column(
                    name=row[2],
                    data_type=row[3],
                    ordinal_position=row[4],
                    is_nullable=row[5] == "YES",
                    max_length=row[6],
                    numeric_precision=row[7],
                    numeric_scale=row[8],
                )
                columns_map.setdefault(key, []).append(col)
        return columns_map
