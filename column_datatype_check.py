"""
Column datatype validation between source and target tables.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ColumnDatatypeCheckRunner:
    """Validates that source and target tables have matching column datatypes."""

    def __init__(self, source_connection: Any, target_connection: Any):
        self.source_connection = source_connection
        self.target_connection = target_connection

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run column datatype checks for each table configuration."""
        results = []

        for config in configs:
            datatype_config = self._normalize_config(
                config.get("column_datatype_check", True)
            )

            if not datatype_config["is_enabled"]:
                results.append(self._skip_result(config, datatype_config))
                continue

            source_table = f"{config['source_database']}.{config['source_table']}"
            target_table = f"{config['target_database']}.{config['target_table']}"

            source_types = self._get_column_types(self.source_connection, source_table)
            target_types = self._get_column_types(self.target_connection, target_table)

            skip_columns = {
                col.lower() for col in datatype_config.get("skip_columns", [])
            }
            if skip_columns:
                source_types = {
                    col_name: col_type
                    for col_name, col_type in source_types.items()
                    if col_name.lower() not in skip_columns
                }
                target_types = {
                    col_name: col_type
                    for col_name, col_type in target_types.items()
                    if col_name.lower() not in skip_columns
                }

            mismatches = []
            for col_name, src_type in source_types.items():
                if col_name in target_types:
                    tgt_type = target_types[col_name]
                    if not self._types_compatible(src_type, tgt_type):
                        mismatches.append(
                            {
                                "column": col_name,
                                "source_type": src_type,
                                "target_type": tgt_type,
                            }
                        )

            passed = len(mismatches) == 0

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "column_datatype_check": datatype_config,
                    "mismatches": mismatches,
                    "status": "passed" if passed else "failed",
                    "message": (
                        f"All column datatypes compatible ({len(source_types)} columns)"
                        if passed
                        else f"Found {len(mismatches)} datatype mismatches"
                    ),
                }
            )

        return results

    def _normalize_config(self, datatype_check: Any) -> dict[str, Any]:
        """Normalize column_datatype_check config to standard dict format."""
        if isinstance(datatype_check, bool):
            return {"is_enabled": datatype_check, "skip_columns": []}

        if isinstance(datatype_check, dict):
            return {
                "is_enabled": bool(datatype_check.get("is_enabled", True)),
                "skip_columns": datatype_check.get("skip_columns", []),
            }

        return {"is_enabled": True, "skip_columns": []}

    def _get_column_types(
        self, connection: Any, full_table_name: str
    ) -> dict[str, str]:
        """Get column names and types from table."""
        query = f"SELECT * FROM {full_table_name} LIMIT 0"
        logger.info(f"Fetching column types from {full_table_name}")

        cursor = connection.connection.cursor()
        try:
            cursor.execute(query)
            # For JDBC: desc = (name, type_code, display_size, internal_size, precision, scale, null_ok)
            # For PyAthena: desc = (name, type_code, ...)
            column_types = {}
            for desc in cursor.description:
                col_name = desc[0]
                type_code = desc[1]
                # Try to get type name from JDBC metadata if available
                type_name = self._extract_type_name(cursor, col_name, type_code)
                column_types[col_name] = self._normalize_type(type_name)
            return column_types
        finally:
            cursor.close()

    def _extract_type_name(self, cursor: Any, col_name: str, type_code: Any) -> str:
        """Extract type name from JDBC metadata or type code."""
        # If it's already a string (PyAthena), use it
        if isinstance(type_code, str):
            return type_code

        # Try to get type name from JDBC cursor metadata
        try:
            # JDBC cursors may have _meta attribute with column info
            if hasattr(cursor, "_meta") and cursor._meta:
                for col_meta in cursor._meta:
                    if col_meta[0] == col_name:
                        # col_meta typically includes type name
                        if len(col_meta) > 1:
                            type_name = col_meta[1]
                            if isinstance(type_name, str):
                                return type_name
        except Exception:
            pass

        # Fallback: convert type_code to string
        return str(type_code)

    def _normalize_type(self, type_name: str) -> str:
        """Normalize type name to standard format."""
        type_str = str(type_name).upper()

        # Extract actual type from JDBC type object string representation
        # e.g., "DBAPITYPEOBJECT('STRING')" -> "STRING"
        if "DBAPITYPEOBJECT" in type_str:
            # Try to extract first type from the list
            import re

            match = re.search(r"'([^']+)'", type_str)
            if match:
                return match.group(1).upper()

        return type_str

    def _types_compatible(self, src_type: str, tgt_type: str) -> bool:
        """Check if source and target types are compatible."""
        src_type = src_type.upper()
        tgt_type = tgt_type.upper()

        # Exact match
        if src_type == tgt_type:
            return True

        # Common compatible mappings
        compatible_groups = [
            {"STRING", "VARCHAR", "CHAR", "TEXT"},
            {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"},
            {"DECIMAL", "NUMERIC", "DOUBLE", "FLOAT"},
            {"TIMESTAMP", "DATETIME"},
        ]

        for group in compatible_groups:
            if src_type in group and tgt_type in group:
                return True

        return False

    def _skip_result(self, config: dict, datatype_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "column_datatype_check": datatype_config,
            "status": "skipped",
            "message": "Column datatype check disabled",
        }
