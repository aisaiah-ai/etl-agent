from __future__ import annotations

import re
from dataclasses import dataclass, field
from textwrap import dedent

from pipelines.redshift_to_glue.sql_parser import (
    Aggregation,
    JoinCondition,
    ParsedSQL,
)

REDSHIFT_TO_PYSPARK: dict[str, str] = {
    "NVL": "coalesce",
    "NVL2": "when({0}.isNotNull(), {1}).otherwise({2})",
    "GETDATE": "current_timestamp",
    "SYSDATE": "current_timestamp",
    "DATEADD": "date_add",
    "DATEDIFF": "datediff",
    "LISTAGG": "collect_list",
    "LEN": "length",
    "STRTOL": "conv",
    "ISNULL": "coalesce",
    "DECODE": "when",
    "TOP": "limit",
    "CONVERT_TIMEZONE": "from_utc_timestamp",
    "REGEXP_SUBSTR": "regexp_extract",
    "CHARINDEX": "locate",
    "POSITION": "locate",
    "TO_NUMBER": "cast",
    "TRUNC": "trunc",
}

REDSHIFT_TYPE_MAP: dict[str, str] = {
    "VARCHAR": "StringType",
    "CHAR": "StringType",
    "BPCHAR": "StringType",
    "TEXT": "StringType",
    "INT": "IntegerType",
    "INT2": "ShortType",
    "INT4": "IntegerType",
    "INT8": "LongType",
    "INTEGER": "IntegerType",
    "SMALLINT": "ShortType",
    "BIGINT": "LongType",
    "FLOAT": "DoubleType",
    "FLOAT4": "FloatType",
    "FLOAT8": "DoubleType",
    "REAL": "FloatType",
    "DOUBLE PRECISION": "DoubleType",
    "NUMERIC": "DecimalType",
    "DECIMAL": "DecimalType",
    "BOOL": "BooleanType",
    "BOOLEAN": "BooleanType",
    "DATE": "DateType",
    "TIMESTAMP": "TimestampType",
    "TIMESTAMPTZ": "TimestampType",
}


@dataclass
class TranslationResult:
    """Output of SQL-to-PySpark translation."""

    pyspark_code: str
    source_tables: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class SQLToPySparkTranslator:
    """Translates parsed Redshift SQL into PySpark DataFrame operations.

    Args:
        source_read_mode: How to read source tables — "catalog" uses the Glue Data
            Catalog (default), "jdbc" reads directly from Redshift via JDBC.
        connection_name: Glue Connection name for JDBC mode.
        redshift_tmp_dir: S3 path for Redshift JDBC temp files (required for JDBC).
    """

    def __init__(
        self,
        source_read_mode: str = "catalog",
        connection_name: str = "",
        redshift_tmp_dir: str = "",
    ):
        if source_read_mode not in ("catalog", "jdbc"):
            raise ValueError(f"Invalid source_read_mode: {source_read_mode!r}")
        self.source_read_mode = source_read_mode
        self.connection_name = connection_name
        self.redshift_tmp_dir = redshift_tmp_dir

    def translate(self, parsed_sql: ParsedSQL) -> str:
        lines: list[str] = []
        warnings: list[str] = []

        table_vars = self._generate_table_reads(parsed_sql, lines)

        if not table_vars:
            lines.append("# No source tables detected; using raw SQL fallback")
            lines.append(f'result_df = spark.sql("""{parsed_sql.raw_sql}""")')
            return "\n".join(lines)

        base_var = table_vars[list(table_vars.keys())[0]]

        if parsed_sql.joins:
            base_var = self._apply_joins(parsed_sql, table_vars, lines, base_var)

        if parsed_sql.where_clause:
            translated_filter = self._translate_functions(parsed_sql.where_clause)
            lines.append(f'result_df = {base_var}.filter("{translated_filter}")')
            base_var = "result_df"

        if parsed_sql.aggregations and parsed_sql.group_by:
            base_var = self._apply_aggregations(parsed_sql, lines, base_var)

        select_cols = self._build_select(parsed_sql)
        if select_cols:
            lines.append(f"result_df = {base_var}.selectExpr({select_cols})")
        else:
            if base_var != "result_df":
                lines.append(f"result_df = {base_var}")

        return "\n".join(lines)

    def translate_join(self, join_info: JoinCondition) -> str:
        spark_join = self._map_join_type(join_info.join_type)
        condition = self._translate_join_condition(join_info.condition)
        right_var = self._table_var(join_info.right_table)
        return f'.join({right_var}, on={condition}, how="{spark_join}")'

    def translate_aggregation(self, agg_info: Aggregation) -> str:
        func = agg_info.function.upper()
        col = agg_info.column

        if func == "LISTAGG":
            inner = col.split(",")[0].strip() if "," in col else col
            return f'F.concat_ws(",", F.collect_list(F.col("{inner}")))'

        spark_func = func.lower()
        if col in ("*", "1"):
            return f"F.{spark_func}(F.lit(1))" if func == "COUNT" else f'F.{spark_func}("*")'

        return f'F.{spark_func}(F.col("{col}"))'

    # -- internal helpers --

    def _generate_table_reads(
        self, parsed_sql: ParsedSQL, lines: list[str]
    ) -> dict[str, str]:
        table_vars: dict[str, str] = {}
        for table in parsed_sql.source_tables:
            var = self._table_var(table)
            if self.source_read_mode == "jdbc":
                lines.append(self._jdbc_read(table, var))
            else:
                lines.append(self._catalog_read(table, var))
            table_vars[table] = var
        return table_vars

    def _catalog_read(self, table: str, var: str) -> str:
        return (
            f'{var} = glueContext.create_dynamic_frame.from_catalog('
            f'database="{self._extract_db(table)}", '
            f'table_name="{self._extract_table(table)}"'
            f").toDF()"
        )

    def _jdbc_read(self, table: str, var: str) -> str:
        schema = self._extract_db(table)
        tbl = self._extract_table(table)
        dbtable = f"{schema}.{tbl}"
        opts = (
            f'{{"useConnectionProperties": "true", '
            f'"connectionName": "{self.connection_name}", '
            f'"dbtable": "{dbtable}", '
            f'"redshiftTmpDir": "{self.redshift_tmp_dir}"}}'
        )
        return (
            f'{var} = glueContext.create_dynamic_frame.from_options('
            f'connection_type="redshift", '
            f'connection_options={opts}'
            f').toDF()'
        )

    def _apply_joins(
        self,
        parsed_sql: ParsedSQL,
        table_vars: dict[str, str],
        lines: list[str],
        base_var: str,
    ) -> str:
        for join in parsed_sql.joins:
            join_code = self.translate_join(join)
            lines.append(f"result_df = {base_var}{join_code}")
            base_var = "result_df"
        return base_var

    def _apply_aggregations(
        self, parsed_sql: ParsedSQL, lines: list[str], base_var: str
    ) -> str:
        group_cols = ", ".join(f'"{c}"' for c in parsed_sql.group_by)
        agg_exprs: list[str] = []
        for agg in parsed_sql.aggregations:
            expr = self.translate_aggregation(agg)
            if agg.alias:
                expr += f'.alias("{agg.alias}")'
            agg_exprs.append(expr)

        agg_str = ", ".join(agg_exprs)
        lines.append(
            f"result_df = {base_var}.groupBy({group_cols}).agg({agg_str})"
        )
        return "result_df"

    def _build_select(self, parsed_sql: ParsedSQL) -> str | None:
        if not parsed_sql.column_lineage:
            return None

        exprs: list[str] = []
        for cl in parsed_sql.column_lineage:
            if cl.transformation:
                translated = self._translate_functions(cl.transformation)
                expr = f"{translated} AS {cl.output_column}"
            elif cl.source_table != "unknown":
                expr = f"{cl.source_column} AS {cl.output_column}"
            else:
                expr = cl.output_column
            exprs.append(f'"{expr}"')

        return ", ".join(exprs) if exprs else None

    def _translate_functions(self, expr: str) -> str:
        result = expr
        for rs_func, spark_func in REDSHIFT_TO_PYSPARK.items():
            pattern = re.compile(rf"\b{rs_func}\b", re.IGNORECASE)
            if "{" not in spark_func:
                result = pattern.sub(spark_func, result)
        result = self._translate_dateadd(result)
        result = self._translate_datediff(result)
        result = self._translate_convert_timezone(result)
        return result

    @staticmethod
    def _translate_dateadd(expr: str) -> str:
        pattern = re.compile(
            r"date_add\s*\(\s*'?(\w+)'?\s*,\s*(.+?)\s*,\s*(.+?)\s*\)",
            re.IGNORECASE,
        )
        def replace(m):
            part = m.group(1).lower()
            interval = m.group(2).strip()
            date_expr = m.group(3).strip()
            if part == "day":
                return f"date_add({date_expr}, {interval})"
            if part == "month":
                return f"add_months({date_expr}, {interval})"
            return f"({date_expr} + interval {interval} {part}s)"
        return pattern.sub(replace, expr)

    @staticmethod
    def _translate_datediff(expr: str) -> str:
        pattern = re.compile(
            r"datediff\s*\(\s*'?(\w+)'?\s*,\s*(.+?)\s*,\s*(.+?)\s*\)",
            re.IGNORECASE,
        )
        def replace(m):
            part = m.group(1).lower()
            start = m.group(2).strip()
            end = m.group(3).strip()
            if part == "day":
                return f"datediff({end}, {start})"
            return f"datediff({end}, {start})"
        return pattern.sub(replace, expr)

    @staticmethod
    def _translate_convert_timezone(expr: str) -> str:
        pattern = re.compile(
            r"from_utc_timestamp\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*(.+?)\s*\)",
            re.IGNORECASE,
        )
        def replace(m):
            target_tz = m.group(2)
            ts = m.group(3).strip()
            return f"from_utc_timestamp({ts}, '{target_tz}')"
        return pattern.sub(replace, expr)

    @staticmethod
    def _map_join_type(redshift_join: str) -> str:
        mapping = {
            "JOIN": "inner",
            "INNER JOIN": "inner",
            "LEFT JOIN": "left",
            "LEFT OUTER JOIN": "left",
            "RIGHT JOIN": "right",
            "RIGHT OUTER JOIN": "right",
            "FULL JOIN": "outer",
            "FULL OUTER JOIN": "outer",
            "CROSS JOIN": "cross",
        }
        return mapping.get(redshift_join.upper(), "inner")

    @staticmethod
    def _translate_join_condition(condition: str) -> str:
        parts = re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE)
        if len(parts) == 1:
            m = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", condition.strip())
            if m:
                return f'[F.col("{m.group(1)}.{m.group(2)}") == F.col("{m.group(3)}.{m.group(4)}")]'
        conditions = []
        for part in parts:
            m = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", part.strip())
            if m:
                conditions.append(
                    f'F.col("{m.group(1)}.{m.group(2)}") == F.col("{m.group(3)}.{m.group(4)}")'
                )
        if conditions:
            return "[" + " & ".join(conditions) + "]"
        return f'[F.expr("{condition.strip()}")]'

    @staticmethod
    def _table_var(table: str) -> str:
        return table.replace(".", "_").lower() + "_df"

    @staticmethod
    def _extract_db(table: str) -> str:
        parts = table.split(".")
        return parts[0] if len(parts) >= 2 else "default"

    @staticmethod
    def _extract_table(table: str) -> str:
        return table.split(".")[-1]
