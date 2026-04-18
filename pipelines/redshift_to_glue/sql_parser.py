from __future__ import annotations

import re
from dataclasses import dataclass, field

import sqlparse
from sqlparse.sql import (
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Parenthesis,
    Where,
)
from sqlparse.tokens import DML, Keyword


@dataclass
class ColumnLineage:
    """Maps a source column to its output column."""

    source_table: str
    source_column: str
    output_column: str
    transformation: str | None = None


@dataclass
class JoinCondition:
    """Represents a JOIN between two tables."""

    join_type: str
    left_table: str
    right_table: str
    condition: str


@dataclass
class Aggregation:
    """Represents an aggregation function call."""

    function: str
    column: str
    alias: str | None = None


@dataclass
class ParsedSQL:
    """Result of parsing a Redshift SQL statement."""

    source_tables: list[str] = field(default_factory=list)
    schemas: dict[str, str] = field(default_factory=dict)
    joins: list[JoinCondition] = field(default_factory=list)
    where_clause: str | None = None
    aggregations: list[Aggregation] = field(default_factory=list)
    column_lineage: list[ColumnLineage] = field(default_factory=list)
    external_tables: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    raw_sql: str = ""


class RedshiftSQLParser:
    """Parses Redshift SQL views and queries to extract structural metadata."""

    EXTERNAL_SCHEMA_PATTERN = re.compile(
        r"(?:FROM|JOIN)\s+([\w]+)\.([\w]+)\.([\w]+)", re.IGNORECASE
    )
    TABLE_REF_PATTERN = re.compile(
        r"(?:FROM|JOIN)\s+((?:[\w]+\.)?[\w]+(?:\.[\w]+)?)", re.IGNORECASE
    )
    VIEW_BODY_PATTERN = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+[\w.]+\s+AS\s+(.+)",
        re.IGNORECASE | re.DOTALL,
    )
    AGG_PATTERN = re.compile(
        r"\b(COUNT|SUM|AVG|MIN|MAX|LISTAGG)\s*\(([^)]*)\)",
        re.IGNORECASE,
    )

    def parse_view_definition(self, sql: str) -> ParsedSQL:
        match = self.VIEW_BODY_PATTERN.search(sql)
        body = match.group(1).rstrip(";").strip() if match else sql.strip().rstrip(";")
        return self._parse_select(body)

    def extract_source_tables(self, sql: str) -> list[str]:
        parsed = self._parse_select(sql)
        return parsed.source_tables

    def extract_column_lineage(self, sql: str) -> list[ColumnLineage]:
        parsed = self._parse_select(sql)
        return parsed.column_lineage

    def detect_external_tables(self, sql: str) -> list[str]:
        return [
            f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
            for m in self.EXTERNAL_SCHEMA_PATTERN.finditer(sql)
        ]

    # -- internal helpers --

    def _parse_select(self, sql: str) -> ParsedSQL:
        result = ParsedSQL(raw_sql=sql)
        statements = sqlparse.parse(sql)
        if not statements:
            return result

        stmt = statements[0]
        self._extract_tables(stmt, result)
        self._extract_where(stmt, result)
        self._extract_aggregations(sql, result)
        self._extract_group_by(sql, result)
        self._extract_lineage(stmt, result)
        result.external_tables = self.detect_external_tables(sql)
        return result

    def _extract_tables(self, stmt, result: ParsedSQL) -> None:
        from_seen = False
        join_type = None

        for token in stmt.tokens:
            if token.ttype is Keyword and token.normalized in (
                "FROM",
                "INNER JOIN",
                "LEFT JOIN",
                "LEFT OUTER JOIN",
                "RIGHT JOIN",
                "RIGHT OUTER JOIN",
                "FULL JOIN",
                "FULL OUTER JOIN",
                "CROSS JOIN",
                "JOIN",
            ):
                if token.normalized == "FROM":
                    from_seen = True
                    join_type = None
                else:
                    from_seen = False
                    join_type = token.normalized
                continue

            if from_seen and isinstance(token, (Identifier, IdentifierList)):
                self._add_table_refs(token, result)
                from_seen = False

            if join_type and isinstance(token, Identifier):
                table_name = self._resolve_table_name(token)
                if table_name:
                    result.source_tables.append(table_name)
                    schema, name = self._split_schema(table_name)
                    if schema:
                        result.schemas[name] = schema
                join_type = None

        if not result.source_tables:
            for m in self.TABLE_REF_PATTERN.finditer(result.raw_sql):
                ref = m.group(1)
                if ref.upper() not in ("SELECT", "SET", "VALUES"):
                    result.source_tables.append(ref)

        result.source_tables = list(dict.fromkeys(result.source_tables))
        self._extract_joins(result)

    def _add_table_refs(self, token, result: ParsedSQL) -> None:
        identifiers = (
            token.get_identifiers() if isinstance(token, IdentifierList) else [token]
        )
        for ident in identifiers:
            if isinstance(ident, Identifier):
                name = self._resolve_table_name(ident)
                if name:
                    result.source_tables.append(name)
                    schema, tbl = self._split_schema(name)
                    if schema:
                        result.schemas[tbl] = schema

    @staticmethod
    def _resolve_table_name(ident: Identifier) -> str | None:
        real_name = ident.get_real_name()
        if not real_name:
            return None
        parent = ident.get_parent_name()
        return f"{parent}.{real_name}" if parent else real_name

    @staticmethod
    def _split_schema(table_ref: str) -> tuple[str | None, str]:
        parts = table_ref.split(".")
        if len(parts) >= 2:
            return parts[-2], parts[-1]
        return None, parts[0]

    def _extract_joins(self, result: ParsedSQL) -> None:
        join_pattern = re.compile(
            r"((?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*(?:OUTER\s+)?JOIN)\s+"
            r"([\w.]+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+?)(?=(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*(?:OUTER\s+)?JOIN\b|WHERE\b|GROUP\s+BY\b|ORDER\s+BY\b|HAVING\b|LIMIT\b|$)",
            re.IGNORECASE | re.DOTALL,
        )
        for m in join_pattern.finditer(result.raw_sql):
            jtype = m.group(1).strip().upper()
            right = m.group(2)
            condition = m.group(4).strip().rstrip(";")
            left = result.source_tables[0] if result.source_tables else "unknown"
            result.joins.append(
                JoinCondition(
                    join_type=jtype,
                    left_table=left,
                    right_table=right,
                    condition=condition,
                )
            )

    def _extract_where(self, stmt, result: ParsedSQL) -> None:
        for token in stmt.tokens:
            if isinstance(token, Where):
                clause = str(token).strip()
                if clause.upper().startswith("WHERE"):
                    clause = clause[5:].strip()
                result.where_clause = clause
                return

    def _extract_aggregations(self, sql: str, result: ParsedSQL) -> None:
        for m in self.AGG_PATTERN.finditer(sql):
            func = m.group(1).upper()
            col = m.group(2).strip()
            alias_match = re.search(
                re.escape(m.group(0)) + r"\s+(?:AS\s+)?(\w+)", sql, re.IGNORECASE
            )
            alias = alias_match.group(1) if alias_match else None
            result.aggregations.append(Aggregation(function=func, column=col, alias=alias))

    def _extract_group_by(self, sql: str, result: ParsedSQL) -> None:
        gb = re.search(
            r"GROUP\s+BY\s+(.+?)(?=HAVING\b|ORDER\s+BY\b|LIMIT\b|$)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if gb:
            cols = [c.strip().rstrip(";") for c in gb.group(1).split(",")]
            result.group_by = [c for c in cols if c]

    def _extract_lineage(self, stmt, result: ParsedSQL) -> None:
        select_items = self._get_select_columns(result.raw_sql)
        alias_map = self._build_alias_map(result)

        for item in select_items:
            item = item.strip()
            if not item or item == "*":
                continue

            alias = None
            expr = item
            as_match = re.match(r"(.+?)\s+AS\s+(\w+)$", item, re.IGNORECASE)
            if as_match:
                expr = as_match.group(1).strip()
                alias = as_match.group(2)
            else:
                parts = item.rsplit(None, 1)
                if (
                    len(parts) == 2
                    and not parts[1].startswith("(")
                    and parts[1].isidentifier()
                ):
                    expr = parts[0].strip()
                    alias = parts[1]

            output_col = alias or expr.split(".")[-1]
            transformation = None if "." in expr and "(" not in expr else expr

            source_table, source_col = self._resolve_source(expr, alias_map)
            result.column_lineage.append(
                ColumnLineage(
                    source_table=source_table,
                    source_column=source_col,
                    output_column=output_col,
                    transformation=transformation,
                )
            )

    def _get_select_columns(self, sql: str) -> list[str]:
        match = re.search(
            r"SELECT\s+(.*?)\s+FROM\b", sql, re.IGNORECASE | re.DOTALL
        )
        if not match:
            return []

        cols_str = match.group(1).strip()
        if cols_str.upper().startswith("DISTINCT"):
            cols_str = cols_str[8:].strip()

        columns: list[str] = []
        depth = 0
        current: list[str] = []
        for ch in cols_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                columns.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            columns.append("".join(current).strip())
        return columns

    @staticmethod
    def _build_alias_map(result: ParsedSQL) -> dict[str, str]:
        alias_map: dict[str, str] = {}
        for table in result.source_tables:
            parts = table.split(".")
            alias_map[parts[-1]] = table
        return alias_map

    @staticmethod
    def _resolve_source(
        expr: str, alias_map: dict[str, str]
    ) -> tuple[str, str]:
        clean = expr.strip()
        if "(" in clean:
            col_match = re.search(r"(\w+)\.(\w+)", clean)
            if col_match:
                tbl = alias_map.get(col_match.group(1), col_match.group(1))
                return tbl, col_match.group(2)
            return "derived", clean

        if "." in clean:
            parts = clean.rsplit(".", 1)
            tbl = alias_map.get(parts[0], parts[0])
            return tbl, parts[1]

        return "unknown", clean
