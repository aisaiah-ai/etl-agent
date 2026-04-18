from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

import boto3

from pipelines.discovery.schema_discovery import Column, ViewDefinition

logger = logging.getLogger(__name__)


@dataclass
class SchemaAnalysis:
    """AI-generated analysis of a Redshift view schema."""

    view_name: str
    column_types: dict[str, str] = field(default_factory=dict)
    relationships: list[dict[str, str]] = field(default_factory=list)
    suggested_transformations: list[str] = field(default_factory=list)
    complexity_score: int = 0
    notes: str = ""


@dataclass
class MigrationPlan:
    """AI-generated migration plan for a set of views."""

    views_in_order: list[str] = field(default_factory=list)
    dependency_graph: dict[str, list[str]] = field(default_factory=dict)
    estimated_effort_hours: float = 0.0
    risks: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)


class AIDiscoveryAgent:
    """Uses Amazon Bedrock (Claude) for AI-powered schema analysis and migration planning."""

    def __init__(
        self,
        bedrock_model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
        region: str = "us-east-1",
    ):
        self.model_id = bedrock_model_id
        self.region = region
        self._client = boto3.client("bedrock-runtime", region_name=region)

    def analyze_schema(
        self, view_ddl: str, source_tables: list[str]
    ) -> SchemaAnalysis:
        prompt = (
            "Analyze this Redshift view DDL and its source tables. "
            "Return a JSON object with keys: column_types (map of column name to recommended Glue type), "
            "relationships (list of {from_table, to_table, join_key}), "
            "suggested_transformations (list of strings), "
            "complexity_score (1-10), notes (string).\n\n"
            f"DDL:\n{view_ddl}\n\n"
            f"Source tables: {', '.join(source_tables)}"
        )

        response_text = self._invoke(prompt)
        data = self._parse_json(response_text)

        return SchemaAnalysis(
            view_name=self._extract_view_name(view_ddl),
            column_types=data.get("column_types", {}),
            relationships=data.get("relationships", []),
            suggested_transformations=data.get("suggested_transformations", []),
            complexity_score=data.get("complexity_score", 0),
            notes=data.get("notes", ""),
        )

    def suggest_glue_schema(self, redshift_columns: list[Column]) -> list[Column]:
        columns_desc = [
            {"name": c.name, "type": c.data_type, "nullable": c.is_nullable}
            for c in redshift_columns
        ]
        prompt = (
            "Convert these Redshift column definitions to Glue-compatible types. "
            "Return a JSON array of {name, data_type, is_nullable} objects "
            "using Glue/Hive type names (string, int, bigint, double, decimal, boolean, date, timestamp).\n\n"
            f"Columns: {json.dumps(columns_desc)}"
        )

        response_text = self._invoke(prompt)
        data = self._parse_json(response_text)

        if not isinstance(data, list):
            data = data.get("columns", [])

        return [
            Column(
                name=col["name"],
                data_type=col["data_type"],
                is_nullable=col.get("is_nullable", True),
            )
            for col in data
        ]

    def generate_migration_plan(
        self, views: list[ViewDefinition]
    ) -> MigrationPlan:
        views_desc = [
            {"name": v.full_name, "ddl": v.definition[:2000]}
            for v in views
        ]
        prompt = (
            "Create a migration plan for moving these Redshift views to AWS Glue. "
            "Determine dependency order, estimate effort, and identify risks. "
            "Return JSON with keys: views_in_order (list of view names in migration order), "
            "dependency_graph (map of view name to list of dependencies), "
            "estimated_effort_hours (float), risks (list of strings), "
            "recommendations (list of strings).\n\n"
            f"Views: {json.dumps(views_desc)}"
        )

        response_text = self._invoke(prompt)
        data = self._parse_json(response_text)

        return MigrationPlan(
            views_in_order=data.get("views_in_order", []),
            dependency_graph=data.get("dependency_graph", {}),
            estimated_effort_hours=data.get("estimated_effort_hours", 0.0),
            risks=data.get("risks", []),
            recommendations=data.get("recommendations", []),
        )

    def _invoke(self, prompt: str, max_tokens: int = 4096) -> str:
        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            }
        )

        response = self._client.invoke_model(
            modelId=self.model_id,
            contentType="application/json",
            accept="application/json",
            body=body,
        )

        result = json.loads(response["body"].read())
        return result["content"][0]["text"]

    @staticmethod
    def _parse_json(text: str) -> dict | list:
        text = text.strip()
        start = text.find("{")
        bracket_start = text.find("[")

        if bracket_start != -1 and (start == -1 or bracket_start < start):
            start = bracket_start
            end = text.rfind("]") + 1
        else:
            end = text.rfind("}") + 1

        if start == -1 or end <= start:
            logger.warning("Could not extract JSON from model response")
            return {}

        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            logger.warning("Failed to parse JSON from model response")
            return {}

    @staticmethod
    def _extract_view_name(ddl: str) -> str:
        import re

        match = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([\w.]+)",
            ddl,
            re.IGNORECASE,
        )
        return match.group(1) if match else "unknown"
