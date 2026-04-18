from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pipelines.discovery.schema_discovery import Column

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    """Result of verifying a single aspect of migration correctness."""

    check_name: str
    passed: bool
    details: str = ""
    mismatches: list[str] = field(default_factory=list)


@dataclass
class ComparisonResult:
    """Result of comparing sample data between Glue output and Redshift source."""

    total_rows_compared: int = 0
    matching_rows: int = 0
    mismatched_rows: int = 0
    missing_in_glue: int = 0
    extra_in_glue: int = 0
    column_mismatches: dict[str, int] = field(default_factory=dict)

    @property
    def match_rate(self) -> float:
        if self.total_rows_compared == 0:
            return 0.0
        return self.matching_rows / self.total_rows_compared


class MigrationVerifier:
    """Verifies that migrated Glue jobs produce output matching the original Redshift views."""

    def verify_schema(
        self,
        glue_output_schema: list[Column],
        redshift_schema: list[Column],
    ) -> VerificationResult:
        glue_cols = {c.name.lower(): c for c in glue_output_schema}
        rs_cols = {c.name.lower(): c for c in redshift_schema}

        mismatches: list[str] = []

        missing = set(rs_cols.keys()) - set(glue_cols.keys())
        for col_name in sorted(missing):
            mismatches.append(f"Column '{col_name}' missing in Glue output")

        extra = set(glue_cols.keys()) - set(rs_cols.keys())
        for col_name in sorted(extra):
            mismatches.append(f"Column '{col_name}' extra in Glue output (not in Redshift)")

        for col_name in sorted(set(rs_cols.keys()) & set(glue_cols.keys())):
            rs_type = self._normalize_type(rs_cols[col_name].data_type)
            glue_type = self._normalize_type(glue_cols[col_name].data_type)
            if rs_type != glue_type and not self._types_compatible(rs_type, glue_type):
                mismatches.append(
                    f"Column '{col_name}' type mismatch: "
                    f"Redshift={rs_cols[col_name].data_type}, "
                    f"Glue={glue_cols[col_name].data_type}"
                )

        return VerificationResult(
            check_name="schema_verification",
            passed=len(mismatches) == 0,
            details=f"Compared {len(rs_cols)} Redshift columns against {len(glue_cols)} Glue columns",
            mismatches=mismatches,
        )

    def verify_row_counts(
        self,
        glue_count: int,
        redshift_count: int,
        tolerance: float = 0.01,
    ) -> bool:
        if redshift_count == 0:
            return glue_count == 0
        diff = abs(glue_count - redshift_count) / redshift_count
        return diff <= tolerance

    def sample_comparison(
        self,
        glue_df,
        redshift_df,
        sample_size: int = 1000,
    ) -> ComparisonResult:
        glue_sample = glue_df.limit(sample_size).toPandas()
        rs_sample = redshift_df.limit(sample_size).toPandas()

        common_cols = list(
            set(glue_sample.columns) & set(rs_sample.columns)
        )
        if not common_cols:
            return ComparisonResult(
                total_rows_compared=0,
                mismatched_rows=0,
                column_mismatches={},
            )

        glue_sub = glue_sample[common_cols].reset_index(drop=True)
        rs_sub = rs_sample[common_cols].reset_index(drop=True)

        min_rows = min(len(glue_sub), len(rs_sub))
        glue_sub = glue_sub.iloc[:min_rows]
        rs_sub = rs_sub.iloc[:min_rows]

        column_mismatches: dict[str, int] = {}
        matching = 0
        mismatched = 0

        for idx in range(min_rows):
            row_match = True
            for col in common_cols:
                g_val = glue_sub.at[idx, col]
                r_val = rs_sub.at[idx, col]
                if not self._values_equal(g_val, r_val):
                    row_match = False
                    column_mismatches[col] = column_mismatches.get(col, 0) + 1
            if row_match:
                matching += 1
            else:
                mismatched += 1

        return ComparisonResult(
            total_rows_compared=min_rows,
            matching_rows=matching,
            mismatched_rows=mismatched,
            missing_in_glue=max(0, len(rs_sample) - len(glue_sample)),
            extra_in_glue=max(0, len(glue_sample) - len(rs_sample)),
            column_mismatches=column_mismatches,
        )

    def generate_report(self, results: list[VerificationResult]) -> dict:
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed

        checks = []
        for r in results:
            check = {
                "name": r.check_name,
                "passed": r.passed,
                "details": r.details,
            }
            if r.mismatches:
                check["mismatches"] = r.mismatches
            checks.append(check)

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_checks": len(results),
                "passed": passed,
                "failed": failed,
                "overall_status": "PASSED" if failed == 0 else "FAILED",
            },
            "checks": checks,
        }

    @staticmethod
    def _normalize_type(data_type: str) -> str:
        t = data_type.lower().split("(")[0].strip()
        mapping = {
            "varchar": "string",
            "character varying": "string",
            "char": "string",
            "bpchar": "string",
            "text": "string",
            "int": "int",
            "int2": "smallint",
            "int4": "int",
            "int8": "bigint",
            "integer": "int",
            "smallint": "smallint",
            "bigint": "bigint",
            "float": "double",
            "float4": "float",
            "float8": "double",
            "real": "float",
            "double precision": "double",
            "numeric": "decimal",
            "decimal": "decimal",
            "bool": "boolean",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "timestamp without time zone": "timestamp",
            "timestamp with time zone": "timestamp",
            "timestamptz": "timestamp",
        }
        return mapping.get(t, t)

    @staticmethod
    def _types_compatible(type_a: str, type_b: str) -> bool:
        compatible_groups = [
            {"int", "bigint", "smallint"},
            {"float", "double", "decimal"},
            {"timestamp", "date"},
        ]
        for group in compatible_groups:
            if type_a in group and type_b in group:
                return True
        return False

    @staticmethod
    def _values_equal(a, b) -> bool:
        import math

        if a is None and b is None:
            return True
        try:
            import pandas as pd

            if pd.isna(a) and pd.isna(b):
                return True
        except (TypeError, ValueError):
            pass
        if isinstance(a, float) and isinstance(b, float):
            if math.isnan(a) and math.isnan(b):
                return True
            return math.isclose(a, b, rel_tol=1e-6)
        return a == b
