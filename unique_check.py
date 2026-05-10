"""
Uniqueness validation for specified columns or column combinations.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class UniqueCheckRunner:
    """Validates that specified columns have unique values (no duplicates)."""

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run uniqueness checks for each table configuration."""
        results = []

        for config in configs:
            unique_config = self._normalize_config(config.get("unique_check", False))

            if not unique_config["is_enabled"]:
                results.append(self._skip_result(config, unique_config))
                continue

            validation_list = unique_config.get("validation_list", [])
            if not validation_list:
                results.append(
                    {
                        "source_database": config["source_database"],
                        "source_table": config["source_table"],
                        "target_database": config["target_database"],
                        "target_table": config["target_table"],
                        "unique_check": unique_config,
                        "status": "skipped",
                        "message": "No columns specified in validation_list",
                    }
                )
                continue

            source_table = f"{config['source_database']}.{config['source_table']}"
            target_table = f"{config['target_database']}.{config['target_table']}"

            # Get actual columns from table
            actual_table_columns = self._get_existing_columns(
                self.source_connection, source_table
            )

            constraint_results = []

            for item in validation_list:
                columns = [item] if isinstance(item, str) else item

                # Check if all columns in this constraint exist
                missing_cols = [
                    col for col in columns if col.lower() not in actual_table_columns
                ]
                if missing_cols:
                    constraint_results.append(
                        {
                            "columns": columns,
                            "constraint_type": (
                                "single_column"
                                if len(columns) == 1
                                else "composite_key"
                            ),
                            "status": "skipped",
                            "message": f"Columns not found: {missing_cols}",
                            "source_duplicate_count": None,
                            "target_duplicate_count": None,
                        }
                    )
                    continue

                src_dup_count = self._count_duplicates(
                    self.source_connection, source_table, columns
                )
                tgt_dup_count = self._count_duplicates(
                    self.target_connection, target_table, columns
                )

                has_duplicates = src_dup_count > 0 or tgt_dup_count > 0
                constraint_results.append(
                    {
                        "columns": columns,
                        "constraint_type": (
                            "single_column" if len(columns) == 1 else "composite_key"
                        ),
                        "source_duplicate_count": src_dup_count,
                        "target_duplicate_count": tgt_dup_count,
                        "status": "failed" if has_duplicates else "passed",
                        "message": (
                            f"No duplicates found"
                            if not has_duplicates
                            else f"Found {src_dup_count} duplicates in source, {tgt_dup_count} in target"
                        ),
                    }
                )

            passed_count = sum(1 for c in constraint_results if c["status"] == "passed")
            failed_count = sum(1 for c in constraint_results if c["status"] == "failed")
            skipped_count = sum(
                1 for c in constraint_results if c["status"] == "skipped"
            )
            overall_status = (
                "passed"
                if failed_count == 0 and passed_count > 0
                else ("failed" if failed_count > 0 else "skipped")
            )

            message = (
                f"{passed_count} constraints passed, {failed_count} constraints failed"
            )
            if skipped_count > 0:
                message += f" ({skipped_count} constraints skipped)"

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "unique_check": unique_config,
                    "constraint_results": constraint_results,
                    "status": overall_status,
                    "message": message,
                }
            )

        return results

    def _normalize_config(self, unique_check: Any) -> dict[str, Any]:
        """Normalize unique_check config to standard dict format."""
        if isinstance(unique_check, bool):
            return {"is_enabled": unique_check, "validation_list": []}

        if isinstance(unique_check, dict):
            return {
                "is_enabled": bool(unique_check.get("is_enabled", False)),
                "validation_list": unique_check.get("validation_list", []),
            }

        return {"is_enabled": False, "validation_list": []}

    def _get_existing_columns(self, connection: Any, full_table_name: str) -> set[str]:
        """Get all column names from the table (lowercase)."""
        query = f"SELECT * FROM {full_table_name} LIMIT 0"
        cursor = connection.connection.cursor()
        try:
            cursor.execute(query)
            return {desc[0].lower() for desc in cursor.description}
        except Exception as e:
            logger.warning(f"Could not get columns from {full_table_name}: {e}")
            return set()
        finally:
            cursor.close()

    def _count_duplicates(
        self, connection: Any, full_table_name: str, columns: list[str]
    ) -> int:
        """Count duplicate rows for given column(s)."""
        if not columns:
            return 0

        col_list = ", ".join(columns)
        query = f"""
            SELECT COUNT(*) AS dup_count
            FROM (
                SELECT {col_list}, COUNT(*) AS cnt
                FROM {full_table_name}
                GROUP BY {col_list}
                HAVING COUNT(*) > 1
            ) duplicates
        """

        logger.info(f"Checking uniqueness of [{col_list}] in {full_table_name}")
        rows = connection.execute(query)

        if not rows:
            return 0

        return int(rows[0].get("dup_count", 0) or 0)

    def _skip_result(self, config: dict, unique_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "unique_check": unique_config,
            "status": "skipped",
            "message": "Unique check disabled",
        }
