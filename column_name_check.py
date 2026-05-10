"""
Column name validation between source and target tables.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ColumnNameCheckRunner:
    """Validates that source and target tables have matching column names."""

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run column name checks for each table configuration."""
        results = []

        for config in configs:
            column_name_config = self._normalize_config(
                config.get("column_name_check", True)
            )

            if not column_name_config["is_enabled"]:
                results.append(self._skip_result(config, column_name_config))
                continue

            source_table = f"{config['source_database']}.{config['source_table']}"
            target_table = f"{config['target_database']}.{config['target_table']}"

            source_columns = self._get_column_names(
                self.source_connection, source_table
            )
            target_columns = self._get_column_names(
                self.target_connection, target_table
            )

            skip_columns = set(column_name_config.get("skip_columns", []))
            source_columns = [col for col in source_columns if col not in skip_columns]
            target_columns = [col for col in target_columns if col not in skip_columns]

            missing_in_target = set(source_columns) - set(target_columns)
            extra_in_target = set(target_columns) - set(source_columns)
            passed = len(missing_in_target) == 0 and len(extra_in_target) == 0

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "column_name_check": column_name_config,
                    "source_column_count": len(source_columns),
                    "target_column_count": len(target_columns),
                    "matching_columns_count": len(
                        set(source_columns) & set(target_columns)
                    ),
                    "missing_in_target": list(missing_in_target),
                    "extra_in_target": list(extra_in_target),
                    "status": "passed" if passed else "failed",
                    "message": (
                        f"Column names match ({len(source_columns)} columns)"
                        if passed
                        else f"Mismatch: {len(missing_in_target)} missing in target, {len(extra_in_target)} extra in target"
                    ),
                }
            )

        return results

    def _normalize_config(self, column_name_check: Any) -> dict[str, Any]:
        """Normalize column_name_check config to standard dict format."""
        if isinstance(column_name_check, bool):
            return {"is_enabled": column_name_check, "skip_columns": []}

        if isinstance(column_name_check, dict):
            return {
                "is_enabled": bool(column_name_check.get("is_enabled", True)),
                "skip_columns": column_name_check.get("skip_columns", []),
            }

        return {"is_enabled": True, "skip_columns": []}

    def _get_column_names(self, connection: Any, full_table_name: str) -> list[str]:
        """Get column names from table."""
        query = f"SELECT * FROM {full_table_name} LIMIT 0"
        logger.info(f"Fetching column names from {full_table_name}")

        cursor = connection.connection.cursor()
        try:
            cursor.execute(query)
            return [desc[0] for desc in cursor.description]
        finally:
            cursor.close()

    def _skip_result(self, config: dict, column_name_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "column_name_check": column_name_config,
            "status": "skipped",
            "message": "Column name check disabled",
        }
