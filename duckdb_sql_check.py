"""
Result-set comparison validation for full row-by-row data diff.

Design note: this runner is connection-agnostic. The caller supplies
``source_connection`` and ``target_connection`` objects that implement
``.execute(sql) -> list[dict]``. A DuckDB-backed connection (``DuckDBConnection``
in the sample notebooks) is the primary intended usage, but any connection
returning ``list[dict]`` will work.
"""

import json
import logging
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)

class DuckDBSqlCheckRunner:
    """Runs user-provided source and target SQL queries and compares result-sets row by row.

    The runner fetches all rows from both sides into memory and performs a
    duplicate-aware, order-insensitive multiset comparison using
    ``collections.Counter``. It mirrors the ``SqlCheckRunner`` contract.
    """

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run DuckDB SQL checks for each table configuration."""
        results = []
        for config in configs:
            duckdb_sql_config = self._normalize_config(config.get("duckdb_sql_check", False))

            if not duckdb_sql_config["is_enabled"]:
                results.append(self._skip_result(config, duckdb_sql_config))
                continue

            source_query_raw = duckdb_sql_config.get("source_query", "")
            target_query_raw = duckdb_sql_config.get("target_query", "")
            source_where_raw = duckdb_sql_config.get("source_where", "")
            target_where_raw = duckdb_sql_config.get("target_where", "")

            if not isinstance(source_query_raw, str) or not isinstance(target_query_raw, str):
                raise ValueError("duckdb_sql_check 'source_query' and 'target_query' must be strings")

            source_query = source_query_raw.strip()
            target_query = target_query_raw.strip()
            source_where = source_where_raw.strip()
            target_where = target_where_raw.strip()

            if source_where and not target_where and target_query.lower().startswith("where "):
                target_where = target_query
                target_query = ""

            if source_where:
                if not target_where:
                    target_where = source_where
                
                source_table = f"{config['source_database']}.{config['source_table']}"
                target_table = f"{config['target_database']}.{config['target_table']}"
                
                source_columns = self._get_column_names(self.source_connection, source_table)
                target_columns = self._get_column_names(self.target_connection, target_table)
                
                common_cols = [col for col in source_columns if col in target_columns]
                if not common_cols:
                    raise ValueError(f"No common columns found between {source_table} and {target_table} for duckdb_sql_check")
                
                select_clause = ", ".join(common_cols)
                
                sw = f" {source_where}" if source_where and not source_where.startswith(" ") else source_where
                tw = f" {target_where}" if target_where and not target_where.startswith(" ") else target_where
                
                source_query = f"SELECT {select_clause} FROM {source_table}{sw}"
                target_query = f"SELECT {select_clause} FROM {target_table}{tw}"

            if not source_query or not target_query:
                raise ValueError("duckdb_sql_check requires non-empty 'source_query' and 'target_query', or 'source_where'")

            max_sample_rows = duckdb_sql_config["max_sample_rows"]

            logger.info("Running source duckdb_sql_check query for %s.%s", config["source_database"], config["source_table"])
            source_rows = self.source_connection.execute(source_query)
            logger.info("Running target duckdb_sql_check query for %s.%s", config["target_database"], config["target_table"])
            target_rows = self.target_connection.execute(target_query)

            comparison = self._compare_rows(source_rows, target_rows, max_sample_rows)
            passed = (
                comparison["missing_in_target_count"] == 0
                and comparison["extra_in_target_count"] == 0
            )

            results.append({
                "source_database": config["source_database"],
                "source_table": config["source_table"],
                "target_database": config["target_database"],
                "target_table": config["target_table"],
                "duckdb_sql_check": duckdb_sql_config,
                "source_row_count": len(source_rows),
                "target_row_count": len(target_rows),
                "missing_in_target_count": comparison["missing_in_target_count"],
                "extra_in_target_count": comparison["extra_in_target_count"],
                "missing_in_target_sample": comparison["missing_in_target_sample"],
                "extra_in_target_sample": comparison["extra_in_target_sample"],
                "status": "passed" if passed else "failed",
                "message": (
                    f"DuckDB SQL query results match ({len(source_rows)} rows)"
                    if passed else (
                        f"DuckDB SQL query results differ: "
                        f"{comparison['missing_in_target_count']} source row(s) missing in target, "
                        f"{comparison['extra_in_target_count']} extra target row(s)"
                    )
                ),
            })
        return results

    def _normalize_config(self, duckdb_sql_check: Any) -> dict[str, Any]:
        if isinstance(duckdb_sql_check, bool):
            return {
                "is_enabled": duckdb_sql_check,
                "source_query": "",
                "target_query": "",
                "source_where": "",
                "target_where": "",
                "max_sample_rows": 20,
            }
        if isinstance(duckdb_sql_check, dict):
            return {
                "is_enabled": bool(duckdb_sql_check.get("is_enabled", False)),
                "source_query": duckdb_sql_check.get("source_query", ""),
                "target_query": duckdb_sql_check.get("target_query", ""),
                "source_where": duckdb_sql_check.get("source_where", ""),
                "target_where": duckdb_sql_check.get("target_where", ""),
                "max_sample_rows": self._parse_max_sample_rows(duckdb_sql_check.get("max_sample_rows", 20)),
            }
        return {
            "is_enabled": False,
            "source_query": "",
            "target_query": "",
            "source_where": "",
            "target_where": "",
            "max_sample_rows": 20,
        }

    def _compare_rows(
        self, source_rows: list[dict], target_rows: list[dict], max_sample_rows: int
    ) -> dict[str, Any]:
        def canonical_row(row: dict) -> str:
            normalized = {str(key).lower(): self._normalize_value(value) for key, value in row.items()}
            return json.dumps(normalized, sort_keys=True, default=str)

        source_counter = Counter(canonical_row(row) for row in source_rows)
        target_counter = Counter(canonical_row(row) for row in target_rows)
        missing_counter = source_counter - target_counter
        extra_counter = target_counter - source_counter
        return {
            "missing_in_target_count": sum(missing_counter.values()),
            "extra_in_target_count": sum(extra_counter.values()),
            "missing_in_target_sample": self._sample_rows(missing_counter, max_sample_rows),
            "extra_in_target_sample": self._sample_rows(extra_counter, max_sample_rows),
        }
    def _sample_rows(
        self, row_counter: Counter, max_sample_rows: int
    ) -> list[dict[str, Any]]:
        if max_sample_rows == 0:
            return []
        samples: list[dict[str, Any]] = []
        for row_json, count in row_counter.items():
            row = json.loads(row_json)
            row["_difference_count"] = count
            samples.append(row)
            if len(samples) >= max_sample_rows:
                break
        return samples
    @staticmethod
    def _normalize_value(value):
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
    @staticmethod
    def _parse_max_sample_rows(value):
        if value in (None, ""):
            return 20
        parsed = int(value)
        if parsed < 0:
            raise ValueError("duckdb_sql_check max_sample_rows must be >= 0")
        return parsed
    def _skip_result(self, config: dict, duckdb_sql_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "duckdb_sql_check": duckdb_sql_config,
            "status": "skipped",
            "message": "DuckDB SQL check disabled",
        }

    def _get_column_names(self, connection: Any, full_table_name: str) -> list[str]:
        """Get column names from table."""
        query = f"SELECT * FROM {full_table_name} LIMIT 0"
        logger.info(f"Fetching column names from {full_table_name}")

        if hasattr(connection, "connection") and hasattr(connection.connection, "cursor"):
            cursor = connection.connection.cursor()
            try:
                cursor.execute(query)
                return [desc[0] for desc in cursor.description]
            finally:
                cursor.close()
        else:
            query_limit_1 = f"SELECT * FROM {full_table_name} LIMIT 1"
            res = connection.execute(query_limit_1)
            if res and len(res) > 0:
                return list(res[0].keys())
            return []
