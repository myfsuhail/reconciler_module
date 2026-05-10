"""
Table availability guard helpers for ValidationRunner.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class RunnerTableGuardMixin:
    """Mixin for source/target table existence handling."""

    def _check_table_availability(self, config: dict[str, Any]) -> dict[str, Any]:
        """Check if source and target tables exist and are queryable."""
        source_table = f"{config['source_database']}.{config['source_table']}"
        target_table = f"{config['target_database']}.{config['target_table']}"

        source_exists, source_error = self._table_exists(
            self.source_connection, source_table
        )
        target_exists, target_error = self._table_exists(
            self.target_connection, target_table
        )

        return {
            "source_exists": source_exists,
            "target_exists": target_exists,
            "source_table": source_table,
            "target_table": target_table,
            "source_error": source_error,
            "target_error": target_error,
        }

    def _table_exists(
        self, connection: Any, full_table_name: str
    ) -> tuple[bool, str | None]:
        """Best-effort table existence check using LIMIT 0."""
        try:
            connection.execute(f"SELECT * FROM {full_table_name} LIMIT 0")
            return True, None
        except Exception as exc:
            error_message = str(exc)
            if self._is_table_not_found_error(error_message):
                logger.warning("Table not found: %s", full_table_name)
                return False, error_message
            raise

    @staticmethod
    def _is_table_not_found_error(error_message: str) -> bool:
        """Return True when exception text indicates missing table/object."""
        if not error_message:
            return False

        msg = error_message.lower()
        indicators = (
            "table not found",
            "table does not exist",
            "does not exist",
            "not found",
            "object not found",
            "entitynotfoundexception",
            "could not resolve table",
            "semanticexception",
        )
        return any(token in msg for token in indicators)

    def _build_table_not_found_checks(
        self,
        validations: list[dict[str, Any]],
        availability: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Return skipped checks for tables missing in source/target."""
        missing_sides: list[str] = []
        if not availability["source_exists"]:
            missing_sides.append("source")
        if not availability["target_exists"]:
            missing_sides.append("target")

        if len(missing_sides) == 2:
            base_message = "Table not found in source and target"
        elif missing_sides[0] == "source":
            base_message = "Table not found in source"
        else:
            base_message = "Table not found in target"

        checks: list[dict[str, Any]] = []
        for validation in validations:
            validation_type = validation["type"]
            check_key = self.TYPE_TO_CHECK_KEY[validation_type]
            validation_config = validation["config"]

            error_parts = []
            if not availability["source_exists"] and availability.get("source_error"):
                error_parts.append(f"source_error={availability['source_error']}")
            if not availability["target_exists"] and availability.get("target_error"):
                error_parts.append(f"target_error={availability['target_error']}")

            message = base_message
            if error_parts:
                message = f"{base_message}: {'; '.join(error_parts)}"

            checks.append(
                {
                    "check_type": check_key,
                    check_key: validation_config,
                    "status": "skipped",
                    "message": message,
                    "validation_key": validation["key"],
                    "validation_type": validation_type,
                }
            )

        return checks
