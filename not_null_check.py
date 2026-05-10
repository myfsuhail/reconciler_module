"""
NOT NULL validation for specified columns.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class NotNullCheckRunner:
    """Validates that specified columns have no NULL values."""

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run not-null checks for each table configuration."""
        results = []

        for config in configs:
            not_null_config = self._normalize_config(
                config.get("not_null_check", False)
            )

            if not not_null_config["is_enabled"]:
                results.append(self._skip_result(config, not_null_config))
                continue

            validation_list = not_null_config.get("validation_list", [])
            if not validation_list:
                results.append(
                    {
                        "source_database": config["source_database"],
                        "source_table": config["source_table"],
                        "target_database": config["target_database"],
                        "target_table": config["target_table"],
                        "not_null_check": not_null_config,
                        "status": "skipped",
                        "message": "No columns specified in validation_list",
                    }
                )
                continue

            source_table = f"{config['source_database']}.{config['source_table']}"
            target_table = f"{config['target_database']}.{config['target_table']}"

            # Get actual columns from tables to validate only existing columns
            actual_columns = self._get_existing_columns(
                self.source_connection, source_table, validation_list
            )
            columns_not_found = [
                col for col in validation_list if col not in actual_columns
            ]

            source_nulls = self._check_nulls(
                self.source_connection, source_table, actual_columns
            )
            target_nulls = self._check_nulls(
                self.target_connection, target_table, actual_columns
            )

            # Build field-level results for all columns in validation_list
            field_results = []

            # Add results for columns that exist
            for col in actual_columns:
                src_count = source_nulls.get(col, 0)
                tgt_count = target_nulls.get(col, 0)
                has_nulls = src_count > 0 or tgt_count > 0

                field_results.append(
                    {
                        "column_name": col,
                        "source_null_count": src_count,
                        "target_null_count": tgt_count,
                        "status": "failed" if has_nulls else "passed",
                        "message": (
                            f"No NULL values found"
                            if not has_nulls
                            else f"Found {src_count} NULLs in source, {tgt_count} NULLs in target"
                        ),
                    }
                )

            # Add results for columns that don't exist
            for col in columns_not_found:
                field_results.append(
                    {
                        "column_name": col,
                        "source_null_count": None,
                        "target_null_count": None,
                        "status": "skipped",
                        "message": "Column Not Found",
                    }
                )

            passed_count = sum(1 for f in field_results if f["status"] == "passed")
            failed_count = sum(1 for f in field_results if f["status"] == "failed")
            skipped_count = sum(1 for f in field_results if f["status"] == "skipped")
            overall_status = (
                "passed"
                if failed_count == 0 and passed_count > 0
                else ("failed" if failed_count > 0 else "skipped")
            )

            message = f"{passed_count} columns passed, {failed_count} columns failed"
            if skipped_count > 0:
                message += f" ({skipped_count} columns not found)"

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "not_null_check": not_null_config,
                    "field_results": field_results,
                    "status": overall_status,
                    "message": message,
                }
            )

        return results

    def _normalize_config(self, not_null_check: Any) -> dict[str, Any]:
        """Normalize not_null_check config to standard dict format."""
        if isinstance(not_null_check, bool):
            return {"is_enabled": not_null_check, "validation_list": []}

        if isinstance(not_null_check, dict):
            return {
                "is_enabled": bool(not_null_check.get("is_enabled", False)),
                "validation_list": not_null_check.get("validation_list", []),
            }

        return {"is_enabled": False, "validation_list": []}

    def _get_existing_columns(
        self, connection: Any, full_table_name: str, validation_list: list[str]
    ) -> list[str]:
        """Get columns that actually exist in the table from validation list."""
        query = f"SELECT * FROM {full_table_name} LIMIT 0"
        cursor = connection.connection.cursor()
        try:
            cursor.execute(query)
            actual_columns = {desc[0].lower() for desc in cursor.description}
            # Filter validation_list to only include existing columns (case-insensitive)
            return [col for col in validation_list if col.lower() in actual_columns]
        except Exception as e:
            logger.warning(f"Could not get columns from {full_table_name}: {e}")
            return []
        finally:
            cursor.close()

    def _check_nulls(
        self, connection: Any, full_table_name: str, columns: list[str]
    ) -> dict[str, int]:
        """Count NULL values for specified columns."""
        if not columns:
            return {}

        null_exprs = [
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_{col}"
            for col in columns
        ]
        query = f"SELECT {', '.join(null_exprs)} FROM {full_table_name}"

        logger.info(f"Checking NULL values in {full_table_name}")
        rows = connection.execute(query)

        if not rows:
            return {}

        row = rows[0]
        return {col: int(row.get(f"null_{col}", 0) or 0) for col in columns}

    def _skip_result(self, config: dict, not_null_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "not_null_check": not_null_config,
            "status": "skipped",
            "message": "NOT NULL check disabled",
        }
