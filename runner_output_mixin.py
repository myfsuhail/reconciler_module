"""
Output construction and persistence helpers for ValidationRunner.
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class RunnerOutputMixin:
    """Mixin for result packaging and output persistence targets."""

    def _build_output_structure(self, execution_id: str, tables_results: list) -> dict:
        """Build final output structure with execution metadata."""

        # Calculate overall statistics
        total_checks = sum(t["total_checks"] for t in tables_results)
        total_passed = sum(t["checks_passed"] for t in tables_results)
        total_failed = sum(t["checks_failed"] for t in tables_results)
        total_skipped = sum(t["checks_skipped"] for t in tables_results)

        duration = (self.execution_end - self.execution_start).total_seconds()

        return {
            "execution_metadata": {
                "execution_id": execution_id,
                "execution_start": self.execution_start.isoformat() + "Z",
                "execution_end": self.execution_end.isoformat() + "Z",
                "execution_duration_seconds": round(duration, 3),
                "total_tables_validated": len(tables_results),
                "total_checks_executed": total_checks,
                "total_passed": total_passed,
                "total_failed": total_failed,
                "total_skipped": total_skipped,
                "source_connection_type": "Impala-JDBC",
                "target_connection_type": "Athena-PyAthena",
                "executed_by": self.executed_by,
            },
            "tables": tables_results,
        }

    def save_to_json(
        self, results: dict[str, Any], output_dir: str = "validation_results"
    ):
        """Save validation results to JSON file."""

        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        # Generate filename with execution ID
        execution_id = results["execution_metadata"]["execution_id"]
        filename = f"{execution_id}.json"
        filepath = output_path / filename

        # Write JSON file
        with open(filepath, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Results saved to: {filepath}")
        print(f"\n✓ Results saved to: {filepath}")
        return str(filepath)

    def save_to_postgres(self, results: dict[str, Any], *args, **kwargs):
        """Future sink: persist validation results to Postgres."""
        raise NotImplementedError("Postgres output sink is not implemented yet")

    def save_to_snowflake(self, results: dict[str, Any], *args, **kwargs):
        """Future sink: persist validation results to Snowflake."""
        raise NotImplementedError("Snowflake output sink is not implemented yet")
