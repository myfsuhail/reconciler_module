"""
String length validation for character columns.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class LengthCheckRunner:
    """Validates max/min string lengths match between source and target."""

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run length checks for each table configuration."""
        results = []

        for config in configs:
            length_config = self._normalize_config(config.get("length_check", True))

            if not length_config["is_enabled"]:
                results.append(self._skip_result(config, length_config))
                continue

            source_table = f"{config['source_database']}.{config['source_table']}"
            target_table = f"{config['target_database']}.{config['target_table']}"

            string_columns = self._get_string_columns(
                self.source_connection, source_table
            )

            if not string_columns:
                results.append(
                    {
                        "source_database": config["source_database"],
                        "source_table": config["source_table"],
                        "target_database": config["target_database"],
                        "target_table": config["target_table"],
                        "length_check": length_config,
                        "status": "skipped",
                        "message": "No string columns found to validate",
                    }
                )
                continue

            source_stats = self._get_length_stats(
                self.source_connection, source_table, string_columns
            )
            target_stats = self._get_length_stats(
                self.target_connection, target_table, string_columns
            )

            mismatches = []
            for col in string_columns:
                if col in source_stats and col in target_stats:
                    src = source_stats[col]
                    tgt = target_stats[col]
                    if (
                        src["max_length"] != tgt["max_length"]
                        or src["min_length"] != tgt["min_length"]
                    ):
                        mismatches.append(
                            {
                                "column": col,
                                "source_max": src["max_length"],
                                "source_min": src["min_length"],
                                "target_max": tgt["max_length"],
                                "target_min": tgt["min_length"],
                            }
                        )

            passed = len(mismatches) == 0

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "length_check": length_config,
                    "columns_checked": string_columns,
                    "mismatches": mismatches,
                    "status": "passed" if passed else "failed",
                    "message": (
                        f"Length validation passed for {len(string_columns)} columns"
                        if passed
                        else f"Found {len(mismatches)} length mismatches"
                    ),
                }
            )

        return results

    def _normalize_config(self, length_check: Any) -> dict[str, Any]:
        """Normalize length_check config to standard dict format."""
        if isinstance(length_check, bool):
            return {"is_enabled": length_check}

        if isinstance(length_check, dict):
            return {"is_enabled": bool(length_check.get("is_enabled", True))}

        return {"is_enabled": True}

    def _get_string_columns(self, connection: Any, full_table_name: str) -> list[str]:
        """Get string/varchar column names."""
        query = f"SELECT * FROM {full_table_name} LIMIT 0"
        cursor = connection.connection.cursor()
        try:
            cursor.execute(query)
            string_types = {"STRING", "VARCHAR", "CHAR", "TEXT"}
            return [
                desc[0]
                for desc in cursor.description
                if str(desc[1]).upper() in string_types
            ]
        finally:
            cursor.close()

    def _get_length_stats(
        self, connection: Any, full_table_name: str, columns: list[str]
    ) -> dict:
        """Get max and min lengths for string columns."""
        if not columns:
            return {}

        length_exprs = [
            f"MAX(LENGTH({col})) AS max_{col}, MIN(LENGTH({col})) AS min_{col}"
            for col in columns
        ]
        query = f"SELECT {', '.join(length_exprs)} FROM {full_table_name}"

        logger.info(f"Fetching length stats from {full_table_name}")
        rows = connection.execute(query)

        if not rows:
            return {}

        row = rows[0]
        stats = {}
        for col in columns:
            stats[col] = {
                "max_length": row.get(f"max_{col}", 0) or 0,
                "min_length": row.get(f"min_{col}", 0) or 0,
            }

        return stats

    def _skip_result(self, config: dict, length_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "length_check": length_config,
            "status": "skipped",
            "message": "Length check disabled",
        }
