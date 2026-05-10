"""
Minimal count check for recon validators.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class CountCheckRunner:
    """Runs source vs target count checks from a small JSON-style config list."""

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run count checks only for entries where count_check is true."""
        results = []

        for config in configs:
            count_check_config = self._normalize_count_check_config(
                config.get("count_check", False)
            )

            if not count_check_config["is_enabled"]:
                results.append(
                    {
                        "source_database": config["source_database"],
                        "source_table": config["source_table"],
                        "target_database": config["target_database"],
                        "target_table": config["target_table"],
                        "count_check": count_check_config,
                        "status": "skipped",
                        "message": "Count check disabled",
                    }
                )
                continue

            source_table = self._full_table_name(
                config["source_database"], config["source_table"]
            )
            target_table = self._full_table_name(
                config["target_database"], config["target_table"]
            )

            source_count = self._fetch_count(self.source_connection, source_table)
            target_count = self._fetch_count(self.target_connection, target_table)
            difference = abs(source_count - target_count)
            difference_percent = self._calculate_difference_percent(
                source_count, target_count
            )
            tolerance_percent = count_check_config["tolerance_in_percent"]
            passed = difference_percent <= tolerance_percent

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "count_check": count_check_config,
                    "source_count": source_count,
                    "target_count": target_count,
                    "difference": difference,
                    "difference_percent": difference_percent,
                    "tolerance_in_percent": tolerance_percent,
                    "status": "passed" if passed else "failed",
                    "message": (
                        f"Counts match within tolerance: source={source_count}, target={target_count}"
                        if passed
                        else (
                            f"Count mismatch: source={source_count}, target={target_count}, "
                            f"difference_percent={difference_percent}% exceeds tolerance={tolerance_percent}%"
                        )
                    ),
                }
            )

        return results

    def _normalize_count_check_config(self, count_check: Any) -> dict[str, Any]:
        if isinstance(count_check, bool):
            return {
                "is_enabled": count_check,
                "tolerance_in_percent": 0.0,
            }

        if isinstance(count_check, dict):
            return {
                "is_enabled": bool(count_check.get("is_enabled", False)),
                "tolerance_in_percent": self._parse_tolerance(
                    count_check.get("tolerance_in_percent", 0)
                ),
            }

        return {
            "is_enabled": False,
            "tolerance_in_percent": 0.0,
        }

    def _parse_tolerance(self, value: Any) -> float:
        if value in (None, ""):
            return 0.0

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            cleaned_value = value.strip().replace("%", "")
            return float(cleaned_value) if cleaned_value else 0.0

        raise ValueError(f"Unsupported tolerance_in_percent value: {value}")

    def _calculate_difference_percent(
        self, source_count: int, target_count: int
    ) -> float:
        difference = abs(source_count - target_count)

        if source_count == 0:
            return 0.0 if target_count == 0 else 100.0

        return round((difference / source_count) * 100, 2)

    def _fetch_count(self, connection: Any, full_table_name: str) -> int:
        query = f"SELECT COUNT(*) AS row_count FROM {full_table_name}"
        logger.info("Running count query on %s", full_table_name)
        rows = connection.execute(query)

        if not rows:
            return 0

        first_row = rows[0]
        for key in ("row_count", "ROW_COUNT", "count", "COUNT", "_col0"):
            if key in first_row:
                return int(first_row[key])

        return int(next(iter(first_row.values())))

    @staticmethod
    def _full_table_name(database: str, table: str) -> str:
        return f"{database}.{table}"
