# Architecture Guide for AI Contributors

This guide summarizes the current code structure and contracts so future AI contributors can make safe, scoped changes.

## Core principles

- Keep `ValidationRunner.run()` return shape stable.
- Use `count_check` as the canonical count validation type.
- Preserve legacy explicit table configs when adding compact config behavior.
- Prefer additive output fields over renaming/removing existing fields.
- Do not commit secrets, passwords, private endpoints, or account-specific identifiers in examples.

## Module map

### Orchestration

- `runner.py`
  - Defines `ValidationRunner`.
  - Owns table iteration, validation extraction, per-table aggregation, and console summaries.
  - Imports mixins for config parsing, table existence handling, and output persistence.

### Config parsing

- `runner_config_mixin.py`
  - Normalizes incoming `table_configs` before execution.
  - Supports legacy explicit configs.
  - Supports `recon_type="basic"` compact configs.
  - Supports `recon_type="dq"` compact configs.
  - Parses compact table specs in `tables_list`.

### Table availability guard

- `runner_table_guard_mixin.py`
  - Checks source and target table availability with `SELECT * FROM <table> LIMIT 0`.
  - If a source/target table is missing, all configured checks for that table are returned as `skipped`.
  - Non-table-not-found errors still raise so connection/permission issues are visible.

### Output handling

- `runner_output_mixin.py`
  - Builds final execution output structure.
  - Implements `save_to_json()`.
  - Contains placeholders for future `save_to_postgres()` and `save_to_snowflake()`.

### Validators

- `count_check.py`: source/target row count comparison with tolerance.
- `column_name_check.py`: column-name parity and skip-column support.
- `column_datatype_check.py`: datatype compatibility and skip-column support.
- `length_check.py`: string length min/max comparison.
- `not_null_check.py`: selected column null checks.
- `unique_check.py`: single-column and composite-key duplicate checks.
- `sql_check.py`: user-provided source/target SQL result-set comparison.
- `spark_sql_check.py`: Spark SQL-based distributed result-set comparison for large datasets.
- `duckdb_sql_check.py`: DuckDB-powered duplicate-aware row-to-row query result comparison.

### Connections

- `connections.py`
  - `JDBCImpalaConnection`
  - `AthenaConnection`
  - `ConnectionManager`

## Config contracts

### Legacy explicit config

Legacy configs are one dictionary per table and must continue to work:

```python
table_configs = [
    {
        "source_database": "src_db",
        "source_table": "orders",
        "target_database": "tgt_db",
        "target_table": "orders",
        "tc1": {"type": "count_check", "is_enabled": True, "tolerance_in_percent": 0.0},
        "tc2": {"type": "column_name_check", "is_enabled": True},
    }
]
```

### Basic compact config

`basic` expands every table in `tables_list` into default checks:

- `count_check`
- `column_name_check`
- `column_datatype_check`

Count tolerance is always `0.0`.

```python
table_configs = [
    {
        "recon_type": "basic",
        "source_database": "src_db",
        "target_database": "tgt_db",
        "tables_list": [
            "orders",
            "customers:customers_raw",
            "items:items_raw(updated_at, del_flag)",
        ],
    }
]
```

### DQ compact config

`dq` enforces the same default basic checks and also runs explicit validations in the same config.

```python
table_configs = [
    {
        "recon_type": "dq",
        "source_database": "src_db",
        "source_table": "orders",
        "target_database": "tgt_db",
        "target_table": "orders",
        "tc4": {"type": "not_null_check", "is_enabled": True, "validation_list": ["order_id"]},
        "tc5": {"type": "unique_check", "is_enabled": True, "validation_list": ["order_id"]},
        "sql_1": {
          "type": "sql_check",
          "is_enabled": True,
          "source_query": "SELECT order_id FROM src_db.orders WHERE status = 'ACTIVE'",
          "target_query": "SELECT order_id FROM tgt_db.orders WHERE status = 'ACTIVE'",
        },
        "spark_sql_1": {
          "type": "spark_sql_check",
          "is_enabled": True,
          "source_query": "SELECT order_id FROM src_db.orders WHERE status = 'ACTIVE'",
          "target_query": "SELECT order_id FROM tgt_db.orders WHERE status = 'ACTIVE'",
          "max_sample_rows": 20,
        },
    }
]
```

`sql_check`, `spark_sql_check`, and `duckdb_sql_check` are explicit only; they are not part of the default `basic` checks.

## Primary migration use case

- Source platform: Impala
- Target platform: AWS Glue Iceberg (queried through Athena)
- Execution environments: SMUS/Glue notebooks and SageMaker jobs
- Typical validation suite: `count_check`, `column_name_check`, `column_datatype_check`, `not_null_check`, `unique_check`, `length_check`, and query-driven row-to-row comparison (`sql_check`, `spark_sql_check`, or `duckdb_sql_check`)

## Compact table spec parsing

- `table_name` means source and target table names are identical.
- `source_table:target_table` maps different table names.
- `source_table:target_table(col1, col2)` maps tables and skips `col1`, `col2` for column-name and datatype checks.
- Invalid syntax should raise `ValueError` before validation starts.
- Duplicate table specs are intentionally executed as provided.

## Output contract

`ValidationRunner.run()` returns:

- `execution_metadata`
- `tables`

Each table result contains:

- source/target identifiers
- table execution timestamps and duration
- pass/fail/skipped counts
- `checks`

Each check should contain:

- `check_type`
- original check config block
- `status`
- `message`
- `validation_key`
- `validation_type`

## Logging contract

- `ValidationRunner` logs validation run, table, and individual check lifecycle events.
- Check execution must emit `INFO` logs when a check starts and when it completes, including check type, validation key, source/target table names, final status, and duration.
- Failure paths must log with `ERROR` or `EXCEPTION` immediately before raising, without changing the result payload contract.
- Use `DEBUG` only for non-sensitive diagnostic context; never log SQL text, credentials, JDBC URLs, S3 staging paths, tokens, or secret-bearing config values.
- Future validators must preserve this lifecycle logging so notebook and SageMaker users can see progress before the final result object is returned.

## Extension guidance

### Add a new validation type

1. Create or update the validator module.
2. Add the type to `TYPE_TO_CHECK_KEY` in `runner.py`.
3. Instantiate and register the runner in `ValidationRunner.__init__()`.
4. Update config defaults only if the check should be default-enabled.
5. Update README, REQUIREMENTS, SKILLS, and tests/docs.

### Add a new compact config mode

1. Update `runner_config_mixin.py`.
2. Keep legacy configs unchanged.
3. Add clear validation errors for malformed input.
4. Document examples in README and this file.

### Add Postgres or Snowflake output

1. Implement the sink in `runner_output_mixin.py`.
2. Do not change `ValidationRunner.run()` output.
3. Avoid logging credentials or connection strings.
4. Keep JSON output behavior backward-compatible.

## Validation checklist for agents

- Run `python -m compileall .` when possible.
- Run `python -m build` when packaging files or imports change.
- Search for stale older count-check aliases; there should be no matches.
- Confirm notebook examples use `count_check`.
- Confirm docs use placeholders for sensitive values.
