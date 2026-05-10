"""
Spark SQL result-set comparison validation for large datasets.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class SparkSqlCheckRunner:
    """Runs user-provided source and target SQL queries using Spark and compares results."""

    def __init__(self, spark_session: Any = None):
        self.spark_session = spark_session

    def run(self, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run Spark SQL checks for each table configuration."""
        results = []

        for config in configs:
            spark_sql_config = self._normalize_config(
                config.get("spark_sql_check", False)
            )

            if not spark_sql_config["is_enabled"]:
                results.append(self._skip_result(config, spark_sql_config))
                continue

            spark = spark_sql_config.get("spark_session") or self.spark_session
            if spark is None:
                logger.error(
                    "spark_sql_check is enabled for %s.%s -> %s.%s but no Spark session is available",
                    config["source_database"],
                    config["source_table"],
                    config["target_database"],
                    config["target_table"],
                )
                raise ValueError(
                    "spark_sql_check is enabled but no Spark session is available. "
                    "Pass spark_session to ValidationRunner(..., spark_session=spark)."
                )

            source_query_raw = spark_sql_config.get("source_query", "")
            target_query_raw = spark_sql_config.get("target_query", "")
            source_where_raw = spark_sql_config.get("source_where", "")
            target_where_raw = spark_sql_config.get("target_where", "")

            if not isinstance(source_query_raw, str) or not isinstance(target_query_raw, str):
                raise ValueError(
                    "spark_sql_check 'source_query' and 'target_query' must be strings"
                )

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
                
                source_columns = spark.sql(f"SELECT * FROM {source_table} LIMIT 0").columns
                target_columns = spark.sql(f"SELECT * FROM {target_table} LIMIT 0").columns
                
                common_cols = [col for col in source_columns if col in target_columns]
                if not common_cols:
                    raise ValueError(f"No common columns found between {source_table} and {target_table} for spark_sql_check")
                
                select_clause = ", ".join(common_cols)
                
                sw = f" {source_where}" if source_where and not source_where.startswith(" ") else source_where
                tw = f" {target_where}" if target_where and not target_where.startswith(" ") else target_where
                
                source_query = f"SELECT {select_clause} FROM {source_table}{sw}"
                target_query = f"SELECT {select_clause} FROM {target_table}{tw}"

            if not source_query or not target_query:
                raise ValueError(
                    "spark_sql_check requires non-empty 'source_query' and 'target_query', or 'source_where'"
                )

            max_sample_rows = spark_sql_config["max_sample_rows"]

            logger.info(
                "Running source spark_sql_check query for %s.%s",
                config["source_database"],
                config["source_table"],
            )
            source_df_raw = spark.sql(source_query)
            logger.info(
                "Running target spark_sql_check query for %s.%s",
                config["target_database"],
                config["target_table"],
            )
            target_df_raw = spark.sql(target_query)

            source_df = self._canonicalize_dataframe(source_df_raw)
            target_df = self._canonicalize_dataframe(target_df_raw)

            if source_df.columns != target_df.columns:
                raise ValueError(
                    "spark_sql_check query outputs must have matching logical columns. "
                    f"source_columns={source_df.columns}, target_columns={target_df.columns}"
                )

            comparison = self._compare_dataframes(source_df, target_df, max_sample_rows)
            passed = (
                comparison["missing_in_target_count"] == 0
                and comparison["extra_in_target_count"] == 0
            )

            results.append(
                {
                    "source_database": config["source_database"],
                    "source_table": config["source_table"],
                    "target_database": config["target_database"],
                    "target_table": config["target_table"],
                    "spark_sql_check": self._output_config(spark_sql_config),
                    "execution_engine": "spark",
                    "source_row_count": comparison["source_row_count"],
                    "target_row_count": comparison["target_row_count"],
                    "missing_in_target_count": comparison["missing_in_target_count"],
                    "extra_in_target_count": comparison["extra_in_target_count"],
                    "missing_in_target_sample": comparison["missing_in_target_sample"],
                    "extra_in_target_sample": comparison["extra_in_target_sample"],
                    "status": "passed" if passed else "failed",
                    "message": (
                        f"Spark SQL query results match ({comparison['source_row_count']} rows)"
                        if passed
                        else (
                            "Spark SQL query results differ: "
                            f"{comparison['missing_in_target_count']} source row(s) missing in target, "
                            f"{comparison['extra_in_target_count']} extra target row(s)"
                        )
                    ),
                }
            )

        return results

    def _canonicalize_dataframe(self, dataframe: Any):
        """Lowercase/sort columns and cast values to string for stable comparison."""
        try:
            from pyspark.sql import functions as F
        except ImportError as exc:
            raise ImportError(
                "spark_sql_check requires pyspark in the runtime environment"
            ) from exc

        lowered_names = [column.lower() for column in dataframe.columns]
        if len(set(lowered_names)) != len(lowered_names):
            raise ValueError(
                "spark_sql_check query output contains duplicate column names after lowercasing. "
                f"columns={dataframe.columns}"
            )

        lowered_df = dataframe.select(
            [F.col(column).alias(column.lower()) for column in dataframe.columns]
        )
        ordered_columns = sorted(lowered_df.columns)
        return lowered_df.select(
            [F.col(column).cast("string").alias(column) for column in ordered_columns]
        )

    def _compare_dataframes(
        self, source_df: Any, target_df: Any, max_sample_rows: int
    ) -> dict[str, Any]:
        """Compare two Spark DataFrames as multisets (duplicate-aware, order-insensitive)."""
        from pyspark.sql import functions as F

        all_columns = source_df.columns

        source_grouped = (
            source_df.groupBy(*all_columns)
            .count()
            .withColumnRenamed("count", "_source_count")
        )
        target_grouped = (
            target_df.groupBy(*all_columns)
            .count()
            .withColumnRenamed("count", "_target_count")
        )

        source_row_count = int(
            source_grouped.select(
                F.coalesce(F.sum("_source_count"), F.lit(0)).alias("total")
            ).first()["total"]
        )
        target_row_count = int(
            target_grouped.select(
                F.coalesce(F.sum("_target_count"), F.lit(0)).alias("total")
            ).first()["total"]
        )

        join_condition = [
            F.col(f"src.{column}").eqNullSafe(F.col(f"tgt.{column}"))
            for column in all_columns
        ]
        compared = source_grouped.alias("src").join(
            target_grouped.alias("tgt"), join_condition, "full_outer"
        )

        src_count = F.coalesce(F.col("_source_count"), F.lit(0))
        tgt_count = F.coalesce(F.col("_target_count"), F.lit(0))
        compared = compared.withColumn(
            "_missing_count", F.greatest(src_count - tgt_count, F.lit(0))
        )
        compared = compared.withColumn(
            "_extra_count", F.greatest(tgt_count - src_count, F.lit(0))
        )

        missing_in_target_count = int(
            compared.select(
                F.coalesce(F.sum("_missing_count"), F.lit(0)).alias("total")
            ).first()["total"]
        )
        extra_in_target_count = int(
            compared.select(
                F.coalesce(F.sum("_extra_count"), F.lit(0)).alias("total")
            ).first()["total"]
        )

        missing_in_target_sample = self._sample_diff_rows(
            compared=compared,
            columns=all_columns,
            side_prefix="src",
            diff_column="_missing_count",
            max_sample_rows=max_sample_rows,
        )
        extra_in_target_sample = self._sample_diff_rows(
            compared=compared,
            columns=all_columns,
            side_prefix="tgt",
            diff_column="_extra_count",
            max_sample_rows=max_sample_rows,
        )

        return {
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "missing_in_target_count": missing_in_target_count,
            "extra_in_target_count": extra_in_target_count,
            "missing_in_target_sample": missing_in_target_sample,
            "extra_in_target_sample": extra_in_target_sample,
        }

    def _sample_diff_rows(
        self,
        compared: Any,
        columns: list[str],
        side_prefix: str,
        diff_column: str,
        max_sample_rows: int,
    ) -> list[dict[str, Any]]:
        """Collect bounded mismatch samples from distributed comparison output."""
        if max_sample_rows == 0:
            return []

        from pyspark.sql import functions as F

        row_expressions = [
            F.col(f"{side_prefix}.{column}").alias(column) for column in columns
        ]
        sample_df = (
            compared.where(F.col(diff_column) > 0)
            .select(*row_expressions, F.col(diff_column).alias("_difference_count"))
            .limit(max_sample_rows)
        )

        return [row.asDict(recursive=True) for row in sample_df.collect()]

    def _normalize_config(self, spark_sql_check: Any) -> dict[str, Any]:
        """Normalize spark_sql_check config to standard dict format."""
        if isinstance(spark_sql_check, bool):
            return {
                "is_enabled": spark_sql_check,
                "source_query": "",
                "target_query": "",
                "source_where": "",
                "target_where": "",
                "max_sample_rows": 20,
            }

        if isinstance(spark_sql_check, dict):
            return {
                "is_enabled": bool(spark_sql_check.get("is_enabled", False)),
                "source_query": spark_sql_check.get("source_query", ""),
                "target_query": spark_sql_check.get("target_query", ""),
                "source_where": spark_sql_check.get("source_where", ""),
                "target_where": spark_sql_check.get("target_where", ""),
                "max_sample_rows": self._parse_max_sample_rows(
                    spark_sql_check.get("max_sample_rows", 20)
                ),
                "spark_session": spark_sql_check.get("spark_session"),
            }

        return {
            "is_enabled": False,
            "source_query": "",
            "target_query": "",
            "source_where": "",
            "target_where": "",
            "max_sample_rows": 20,
            "spark_session": None,
        }

    @staticmethod
    def _parse_max_sample_rows(value: Any) -> int:
        if value in (None, ""):
            return 20
        parsed = int(value)
        if parsed < 0:
            raise ValueError(
                "spark_sql_check max_sample_rows must be greater than or equal to 0"
            )
        return parsed

    def _skip_result(self, config: dict, spark_sql_config: dict) -> dict:
        """Generate result for skipped check."""
        return {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
            "spark_sql_check": self._output_config(spark_sql_config),
            "execution_engine": "spark",
            "status": "skipped",
            "message": "Spark SQL check disabled",
        }

    @staticmethod
    def _output_config(spark_sql_config: dict[str, Any]) -> dict[str, Any]:
        """Return a JSON-safe config payload for output contracts."""
        sanitized = dict(spark_sql_config)
        sanitized.pop("spark_session", None)
        return sanitized
