"""
Unified validation runner that executes all checks based on table configuration.
"""

import logging
import os
from datetime import datetime
from time import perf_counter
from typing import Any

try:
    from .count_check import CountCheckRunner
    from .column_name_check import ColumnNameCheckRunner
    from .column_datatype_check import ColumnDatatypeCheckRunner
    from .length_check import LengthCheckRunner
    from .not_null_check import NotNullCheckRunner
    from .spark_sql_check import SparkSqlCheckRunner
    from .sql_check import SqlCheckRunner
    from .unique_check import UniqueCheckRunner
    from .duckdb_sql_check import DuckDBSqlCheckRunner
    from .runner_config_mixin import RunnerConfigMixin
    from .runner_table_guard_mixin import RunnerTableGuardMixin
    from .runner_output_mixin import RunnerOutputMixin
except ImportError:  # Backward compatibility for direct source execution
    from count_check import CountCheckRunner
    from column_name_check import ColumnNameCheckRunner
    from column_datatype_check import ColumnDatatypeCheckRunner
    from length_check import LengthCheckRunner
    from not_null_check import NotNullCheckRunner
    from spark_sql_check import SparkSqlCheckRunner
    from sql_check import SqlCheckRunner
    from unique_check import UniqueCheckRunner
    from duckdb_sql_check import DuckDBSqlCheckRunner
    from runner_config_mixin import RunnerConfigMixin
    from runner_table_guard_mixin import RunnerTableGuardMixin
    from runner_output_mixin import RunnerOutputMixin


logger = logging.getLogger(__name__)


class ValidationRunner(RunnerConfigMixin, RunnerTableGuardMixin, RunnerOutputMixin):
    """Unified runner that executes all validation checks for configured tables."""

    TABLE_ID_KEYS = {
        "source_database",
        "source_table",
        "target_database",
        "target_table",
    }

    TYPE_TO_CHECK_KEY = {
        "count_check": "count_check",
        "column_name_check": "column_name_check",
        "column_datatype_check": "column_datatype_check",
        "not_null_check": "not_null_check",
        "unique_check": "unique_check",
        "length_check": "length_check",
        "sql_check": "sql_check",
        "spark_sql_check": "spark_sql_check",
        "duckdb_sql_check": "duckdb_sql_check",
    }

    DEFAULT_ENABLED_TYPES = {
        "count_check",
        "column_name_check",
        "column_datatype_check",
    }

    def __init__(
        self,
        source_connection: Any,
        target_connection: Any,
        executed_by: str = None,
        spark_session: Any = None,
    ):
        self.source_connection = source_connection
        self.target_connection = target_connection
        self.executed_by = executed_by or os.getenv("USER", "unknown")
        self.spark_session = spark_session

        # Initialize all validators
        self.count_runner = CountCheckRunner(source_connection, target_connection)
        self.column_name_runner = ColumnNameCheckRunner(
            source_connection, target_connection
        )
        self.datatype_runner = ColumnDatatypeCheckRunner(
            source_connection, target_connection
        )
        self.length_runner = LengthCheckRunner(source_connection, target_connection)
        self.not_null_runner = NotNullCheckRunner(source_connection, target_connection)
        self.sql_runner = SqlCheckRunner(source_connection, target_connection)
        self.spark_sql_runner = SparkSqlCheckRunner(spark_session=spark_session)
        self.unique_runner = UniqueCheckRunner(source_connection, target_connection)
        self.duckdb_sql_runner = DuckDBSqlCheckRunner(source_connection, target_connection)

        self.type_to_runner = {
            "count_check": self.count_runner,
            "column_name_check": self.column_name_runner,
            "column_datatype_check": self.datatype_runner,
            "length_check": self.length_runner,
            "not_null_check": self.not_null_runner,
            "sql_check": self.sql_runner,
            "spark_sql_check": self.spark_sql_runner,
            "unique_check": self.unique_runner,
            "duckdb_sql_check": self.duckdb_sql_runner,
        }

        # Track execution timing
        self.execution_start = None
        self.execution_end = None

    def run(self, table_configs: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Run all validation checks for configured tables.
        Processes one table at a time, running all checks before moving to next table.

        Args:
            table_configs: List of table configurations with validation settings

        Returns:
            Structured results with execution metadata and table-level grouping
        """
        normalized_table_configs = self._normalize_table_configs(table_configs)

        self.execution_start = datetime.utcnow()
        execution_id = f"exec-{self.execution_start.strftime('%Y%m%d-%H%M%S')}"

        logger.info(
            "Starting validation for %s table(s) - Execution ID: %s",
            len(normalized_table_configs),
            execution_id,
        )

        tables_results = []

        # Process one table at a time
        for idx, config in enumerate(normalized_table_configs, 1):
            table_name = f"{config['source_database']}.{config['source_table']}"
            logger.info(
                "Processing table %s/%s: %s",
                idx,
                len(normalized_table_configs),
                table_name,
            )

            table_start = datetime.utcnow()
            checks = self._run_table_checks(config)

            table_end = datetime.utcnow()

            # Build table result structure
            table_result = self._build_table_result(
                config=config,
                table_start=table_start,
                table_end=table_end,
                checks=checks,
            )

            tables_results.append(table_result)
            logger.info(
                "Completed table %s/%s: %s in %.3fs (passed=%s, failed=%s, skipped=%s)",
                idx,
                len(normalized_table_configs),
                table_name,
                table_result["table_execution_duration_seconds"],
                table_result["checks_passed"],
                table_result["checks_failed"],
                table_result["checks_skipped"],
            )

        self.execution_end = datetime.utcnow()

        # Build final output structure
        output = self._build_output_structure(execution_id, tables_results)

        logger.info(
            "Validation complete - Execution ID: %s, Duration: %.3fs",
            execution_id,
            (self.execution_end - self.execution_start).total_seconds(),
        )
        return output

    def _run_table_checks(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        """Run configured validations for a single table config."""
        validations = self._extract_validations(config)
        checks: list[dict[str, Any]] = []

        query_only_types = {"sql_check", "spark_sql_check"}
        needs_table_guard = any(
            v["type"] not in query_only_types
            and bool(v["config"].get("is_enabled", False))
            for v in validations
        )

        availability = {
            "source_exists": True,
            "target_exists": True,
            "source_error": None,
            "target_error": None,
        }
        if needs_table_guard:
            availability = self._check_table_availability(config)

        for validation in validations:
            validation_type = validation["type"]
            check_key = self.TYPE_TO_CHECK_KEY[validation_type]
            validation_key = validation["key"]
            check_start = perf_counter()

            source_name = f"{config['source_database']}.{config['source_table']}"
            target_name = f"{config['target_database']}.{config['target_table']}"
            is_enabled = bool(validation["config"].get("is_enabled", False))

            logger.info(
                "Starting check %s (%s) for %s -> %s",
                validation_type,
                validation_key,
                source_name,
                target_name,
            )
            logger.debug(
                "Check context: type=%s, key=%s, enabled=%s, source=%s, target=%s",
                validation_type,
                validation_key,
                is_enabled,
                source_name,
                target_name,
            )

            if (
                validation_type not in query_only_types
                and is_enabled
                and (
                    not availability["source_exists"]
                    or not availability["target_exists"]
                )
            ):
                skipped_checks = self._build_table_not_found_checks(
                    [validation], availability
                )
                checks.extend(skipped_checks)
                duration = perf_counter() - check_start
                logger.info(
                    "Completed check %s (%s) for %s -> %s with status=skipped in %.3fs",
                    validation_type,
                    validation_key,
                    source_name,
                    target_name,
                    duration,
                )
                continue

            runner = self.type_to_runner[validation_type]

            single_config = {
                "source_database": config["source_database"],
                "source_table": config["source_table"],
                "target_database": config["target_database"],
                "target_table": config["target_table"],
                check_key: validation["config"],
            }

            try:
                result = runner.run([single_config])[0]
            except Exception:
                duration = perf_counter() - check_start
                logger.exception(
                    "Failed check %s (%s) for %s -> %s after %.3fs",
                    validation_type,
                    validation_key,
                    source_name,
                    target_name,
                    duration,
                )
                raise

            formatted = self._format_check(result, check_key)
            formatted["validation_key"] = validation_key
            formatted["validation_type"] = validation_type
            checks.append(formatted)

            duration = perf_counter() - check_start
            logger.info(
                "Completed check %s (%s) for %s -> %s with status=%s in %.3fs",
                validation_type,
                validation_key,
                source_name,
                target_name,
                formatted.get("status", "unknown"),
                duration,
            )

        return checks

    def _extract_validations(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract validations from config using strict type-driven format."""
        validations: list[dict[str, Any]] = []

        for key, value in config.items():
            if key in self.TABLE_ID_KEYS:
                continue

            if not isinstance(value, dict):
                raise ValueError(f"Validation '{key}' must be a dict containing 'type'")

            validation_type = value.get("type")
            if not isinstance(validation_type, str) or not validation_type.strip():
                raise ValueError(f"Validation '{key}' must include non-empty 'type'")

            validation_type = validation_type.strip()
            if validation_type not in self.TYPE_TO_CHECK_KEY:
                raise ValueError(
                    f"Unsupported validation type '{validation_type}' in '{key}'. "
                    f"Supported types: {sorted(self.TYPE_TO_CHECK_KEY.keys())}"
                )

            validation_config = dict(value)
            if "is_enabled" not in validation_config:
                validation_config["is_enabled"] = (
                    validation_type in self.DEFAULT_ENABLED_TYPES
                )

            validations.append(
                {
                    "key": key,
                    "type": validation_type,
                    "config": validation_config,
                }
            )

        configured_types = {v["type"] for v in validations}
        for validation_type in sorted(self.DEFAULT_ENABLED_TYPES - configured_types):
            validations.append(
                {
                    "key": f"default_{validation_type}",
                    "type": validation_type,
                    "config": {
                        "type": validation_type,
                        "is_enabled": True,
                    },
                }
            )

        return validations

    def _build_table_result(
        self,
        config: dict,
        table_start: datetime,
        table_end: datetime,
        checks: list[dict[str, Any]],
    ) -> dict:
        """Build table-level result structure."""

        # Calculate table-level summary
        total_checks = len(checks)
        checks_passed = sum(1 for c in checks if c.get("status") == "passed")
        checks_failed = sum(1 for c in checks if c.get("status") == "failed")
        checks_skipped = sum(1 for c in checks if c.get("status") == "skipped")

        duration = (table_end - table_start).total_seconds()

        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "table_execution_start": table_start.isoformat() + "Z",
            "table_execution_end": table_end.isoformat() + "Z",
            "table_execution_duration_seconds": round(duration, 3),
            "total_checks": total_checks,
            "checks_passed": checks_passed,
            "checks_failed": checks_failed,
            "checks_skipped": checks_skipped,
            "checks": checks,
        }

    def _format_check(self, result: dict, check_type: str) -> dict:
        """Format check result removing redundant table identifiers."""
        formatted = {"check_type": check_type}

        # Copy all fields except table identifiers
        exclude_keys = {
            "source_database",
            "source_table",
            "target_database",
            "target_table",
        }
        for key, value in result.items():
            if key not in exclude_keys:
                formatted[key] = value

        return formatted

    def print_summary(self, results: dict[str, Any]):
        """Print summary of all validation results."""
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)

        metadata = results.get("execution_metadata", {})
        print(f"Execution ID: {metadata.get('execution_id')}")
        print(f"Duration: {metadata.get('execution_duration_seconds')}s")
        print(f"Tables Validated: {metadata.get('total_tables_validated')}")
        print(f"Total Checks: {metadata.get('total_checks_executed')}")
        print(f"  ✓ Passed: {metadata.get('total_passed')}")
        print(f"  ✗ Failed: {metadata.get('total_failed')}")
        print(f"  ⊘ Skipped: {metadata.get('total_skipped')}")

        print("\n" + "-" * 80)
        print("TABLE-LEVEL SUMMARY")
        print("-" * 80)

        for table in results.get("tables", []):
            status_icon = "✓" if table["checks_failed"] == 0 else "✗"
            print(f"\n{status_icon} {table['source_table']} → {table['target_table']}")
            print(
                f"   Passed: {table['checks_passed']}, Failed: {table['checks_failed']}, Skipped: {table['checks_skipped']}"
            )
            print(f"   Duration: {table['table_execution_duration_seconds']}s")

    def print_detailed_results(self, results: dict[str, Any]):
        """Print detailed results for all validation checks."""

        for table in results.get("tables", []):
            print("\n" + "=" * 80)
            print(f"TABLE: {table['source_table']} → {table['target_table']}")
            print("=" * 80)

            for check in table.get("checks", []):
                check_type = check["check_type"].replace("_", " ").upper()
                status_icon = {"passed": "✓", "failed": "✗", "skipped": "⊘"}.get(
                    check["status"], "?"
                )

                print(f"\n{status_icon} {check_type}: {check['status'].upper()}")
                print(f"   {check['message']}")

                # Show specific details based on check type
                if (
                    check["check_type"] == "count_check"
                    and check["status"] != "skipped"
                ):
                    print(
                        f"   Source: {check.get('source_count')}, Target: {check.get('target_count')}"
                    )

                elif check["check_type"] == "column_name_check":
                    if check.get("missing_in_target"):
                        print(f"   Missing: {check['missing_in_target']}")
                    if check.get("extra_in_target"):
                        print(f"   Extra: {check['extra_in_target']}")

                elif check["check_type"] == "column_datatype_check":
                    if check.get("mismatches"):
                        for mm in check["mismatches"][:3]:
                            print(
                                f"   ✗ {mm['column']}: {mm['source_type']} → {mm['target_type']}"
                            )

                elif check["check_type"] == "length_check":
                    if check.get("mismatches"):
                        for lm in check["mismatches"][:3]:
                            print(
                                f"   {lm['column']}: src[{lm['source_min']}-{lm['source_max']}] vs tgt[{lm['target_min']}-{lm['target_max']}]"
                            )

                elif check["check_type"] == "not_null_check":
                    if check.get("field_results"):
                        for nv in check["field_results"][:5]:
                            if nv.get("status") == "failed":
                                print(
                                    f"   {nv['column_name']}: source_nulls={nv['source_null_count']}, target_nulls={nv['target_null_count']}"
                                )

                elif check["check_type"] == "unique_check":
                    if check.get("constraint_results"):
                        for uv in check["constraint_results"][:5]:
                            if uv.get("status") == "failed":
                                cols = ", ".join(uv["columns"])
                                print(
                                    f"   [{cols}]: source_dups={uv['source_duplicate_count']}, target_dups={uv['target_duplicate_count']}"
                                )

                elif (
                    check["check_type"] == "sql_check" and check["status"] != "skipped"
                ):
                    print(
                        f"   Source rows: {check.get('source_row_count')}, Target rows: {check.get('target_row_count')}"
                    )
                    print(
                        f"   Missing in target: {check.get('missing_in_target_count')}, Extra in target: {check.get('extra_in_target_count')}"
                    )

                elif (
                    check["check_type"] == "spark_sql_check"
                    and check["status"] != "skipped"
                ):
                    print(f"   Engine: {check.get('execution_engine', 'spark')}")
                    print(
                        f"   Source rows: {check.get('source_row_count')}, Target rows: {check.get('target_row_count')}"
                    )
                    print(
                        f"   Missing in target: {check.get('missing_in_target_count')}, Extra in target: {check.get('extra_in_target_count')}"
                    )
