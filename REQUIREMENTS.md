# Requirements

## Runtime dependencies

- `jaydebeapi`
- `JPype1`
- `pyathena`
- `boto3`

Optional for Spark execution mode:
- `pyspark` (when using `spark_sql_check`)

Optional but recommended for development:
- `pytest`
- `build`
- `twine`
- `ruff` or `flake8`
- `mypy` (as adoption matures)

## System/runtime assumptions

- Access to Impala JDBC jar at runtime
- Network connectivity to Impala endpoint and Athena
- Valid AWS/IAM auth for Athena and S3 staging

Additional assumptions:
- Runtime filesystem has write access for result JSON files (or mounted working directory).
- Environment variables or secret provider is available for credentials.
- Time synchronization is acceptable for timestamped execution metadata.

## Packaging requirements (recommended)

- Python 3.10+
- Build backend via `pyproject.toml` (`setuptools` + `wheel`)
- Package imports should be relative-safe for wheel installation

Recommended packaging outcomes:
- `pip install <wheel>` succeeds in clean environment.
- `import recon_validators` succeeds after installation.
- Runtime dependencies resolve without manual patching.
- Notebook/script examples work with installed package (not only source path).

## Functional requirements

1. Validate source/target table parity using configurable checks.
2. Allow per-check enable/disable behavior at table level.
3. Return structured JSON-compatible results for each execution.
4. Include run-level metadata for traceability.
5. Support `passed`, `failed`, and `skipped` status semantics.
6. Support legacy explicit configs and compact `basic`/`dq` configs.
7. Mark all checks as `skipped` when a configured table is missing in source, target, or both.

## Config requirements

- Canonical validation type names are `count_check`, `column_name_check`, `column_datatype_check`, `length_check`, `not_null_check`, `unique_check`, `sql_check`, and `spark_sql_check`.
- `count_check` replaces older count-check terminology.
- In `basic` and `dq` modes, `count_check`, `column_name_check`, and `column_datatype_check` are enabled by default.
- In `basic` and `dq` modes, count tolerance is forced to `0.0`.
- `sql_check` requires non-empty `source_query` and `target_query` strings when enabled.
- `spark_sql_check` requires non-empty `source_query` and `target_query` strings and an available Spark session when enabled.
- Compact `tables_list` entries must reject malformed table specs before validation begins.

## Output contract requirements

- Preserve top-level result keys and check-level semantics.
- Keep existing key names stable unless version bump is intentional.
- Any new fields must be additive and optional.
- Output must remain JSON serializable.
- JSON output is current production behavior through `save_to_json()`.
- Postgres and Snowflake outputs are planned future sinks and must not change the `run()` return payload.

## Non-functional requirements

- Lightweight footprint suitable for SageMaker environments.
- Deterministic and readable check messages.
- Reasonable logging for operational debugging.
- Stable behavior across repeated runs with same inputs.

## Performance requirements (guideline)

- Avoid unnecessary full-data scans where possible.
- Prefer metadata/minimal queries for schema checks.
- Allow selective check disablement for high-volume tables.
- Surface execution durations for tuning opportunities.

## Security requirements

- No hardcoded passwords in notebooks or source files
- Use environment variables, IAM roles, or secret management services

Additional security controls:
- Do not log secrets, tokens, or full credential-bearing URLs.
- Minimize visibility of account-specific identifiers in docs/examples.
- Restrict S3 write/read access to least privilege.
- Rotate credentials based on enterprise policy.

## Operational requirements

- Validation config should be externalized (JSON/YAML/config service)
- Results must be written to durable storage (S3 or managed artifact store)
- Future deployments may persist the same result payload to JSON, Postgres, and Snowflake.

Recommended operations pattern:
- Schedule recurring validations for critical tables.
- Persist result artifacts with retention policy.
- Monitor fail/skipped trends by domain and run date.
- Integrate with incident workflows for repeated failures.

## Testing requirements

Minimum when changing behavior:
- happy-path test
- failure-path test
- edge-case/empty-case test
- output-shape contract test

Connection-layer changes should also include:
- simulated connection failure handling
- safe close semantics
- SQL execution return-shape consistency

## Release requirements

Before release/build:
- run lint and tests
- verify packaging build success
- verify install in clean env
- update docs for behavior/config changes
- confirm no secrets in committed notebooks/files
