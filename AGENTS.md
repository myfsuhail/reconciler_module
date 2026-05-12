# AGENTS Guide

This file defines how automated contributors should operate in this repository.

## Mission

Keep `recon_validators` stable, lightweight, and easy to run in SageMaker.

## Scope of agent contributions

Agents may:
- fix defects
- add backward-compatible enhancements
- improve logs and observability
- improve packaging readiness for wheel distribution
- add tests and docs aligned to current contracts

Agents must avoid:
- broad refactors without user request
- changing output field names silently
- introducing account-specific hardcoded values
- committing secrets or confidential identifiers

## Rules

1. Keep changes minimal and scoped.
2. Preserve existing JSON output contracts unless explicitly changing version.
3. Add logs for operational visibility; avoid noisy logs.
4. Prefer backward-compatible config handling.
5. Do not hardcode credentials or account-specific secrets.

## Security and compliance guardrails

- Never commit passwords, tokens, private endpoints, or keys.
- Redact sensitive identifiers in examples.
- Prefer placeholders (`<value>`) in docs.
- Ensure no accidental leakage through notebook outputs.

## Code-style and quality expectations

- Keep module responsibilities clear.
- Follow existing naming conventions and structure.
- Avoid introducing unnecessary dependencies.
- Keep function contracts explicit and typed where feasible.
- Preserve compatibility with current Python usage in repo.

## Recommended workflow

1. Read `ARCHITECTURE.md`, `runner.py`, and the runner mixins before editing orchestration.
2. Validate schema compatibility of outputs.
3. Add/adjust tests when changing validation behavior.
4. Confirm imports are package-safe for wheel distribution.
5. Document user-facing behavior changes in README.

## Current architecture map

- `runner.py`: `ValidationRunner` orchestration, table loop, check extraction, summaries.
- `runner_config_mixin.py`: compact/legacy config normalization, `basic` and `dq` expansion, table spec parsing.
- `runner_table_guard_mixin.py`: source/target table existence checks and skipped results for missing tables.
- `runner_output_mixin.py`: output structure, JSON persistence, future Postgres/Snowflake sink placeholders.
- `*_check.py`: individual validation runners. Keep their result payloads stable.

## Current config contract notes

- Canonical count validation type is `count_check`; do not reintroduce older count-check aliases.
- Legacy per-table configs remain supported when `recon_type` is absent.
- `recon_type="basic"` expands `tables_list` into `count_check`, `column_name_check`, and `column_datatype_check`.
- `recon_type="dq"` enforces the same basic checks and also runs explicit DQ checks in the config.
- `sql_check` is explicit only and compares user-provided source/target query results.
- `spark_sql_check` is explicit only and compares user-provided source/target query results with Spark SQL.
- `duckdb_sql_check` is explicit only and compares user-provided source/target query results with DuckDB-backed set semantics.
- For compact table specs, `source:target(skip_col1, skip_col2)` applies skip columns to both column-name and datatype checks.
- Duplicate `tables_list` entries are intentionally executed as provided.

## Change categories and expected actions

### A) Validation logic change

Expected steps:
1. Verify impacted validators and config keys.
2. Confirm JSON output key compatibility.
3. Add/update tests for pass/fail/skipped behavior.
4. Add concise release note entry in README.

### A2) Config parsing change

Expected steps:
1. Update `runner_config_mixin.py` only unless orchestration changes are required.
2. Preserve legacy config behavior and existing `type` names.
3. Validate `basic`, `dq`, and invalid table-spec paths.
4. Update README examples and `ARCHITECTURE.md`.

### B) Connection-layer change

Expected steps:
1. Verify both Impala and Athena paths.
2. Confirm cursor/execute behavior stays consistent.
3. Validate failure paths and error logs.
4. Document any new assumptions.

### C) Packaging/distribution change

Expected steps:
1. Verify package-safe imports.
2. Confirm wheel build/install path.
3. Keep runtime dependencies explicit.
4. Add usage notes for SageMaker deployment.

### D) Output sink change

Expected steps:
1. Update `runner_output_mixin.py` for JSON/Postgres/Snowflake output behavior.
2. Keep `ValidationRunner.run()` return shape unchanged.
3. Keep `save_to_json()` backward-compatible.
4. Never log credentials or database connection strings.

## Output contract discipline

When editing result payloads:
- keep existing keys stable unless explicitly versioning
- if adding fields, make them additive and optional
- avoid renaming keys used by downstream consumers
- keep status semantics consistent (`passed`, `failed`, `skipped`)

## Logging discipline

- Use informative logs at major execution boundaries.
- Every validation change must preserve or add `logging` calls for check start, completion, skip, and failure paths where applicable.
- Use `INFO` for run/table/check lifecycle events, `DEBUG` for non-sensitive diagnostic context, `WARNING` for recoverable unusual conditions, and `ERROR`/`EXCEPTION` before raising failures.
- Avoid row-level or noisy repetitive logs.
- Never log credentials or secret-bearing URLs.
- Include table names and check type in diagnostics.
- Do not log SQL text, passwords, tokens, JDBC URLs, S3 staging paths, or other secret-bearing values.

## Testing guidance

Minimum test expectations when behavior changes:
- one happy-path test
- one failure-path test
- one edge-case/empty-case test
- one output-shape test for contract safety

## Pull request checklist for agents

- [ ] Change is scoped and minimal.
- [ ] No secrets or sensitive data introduced.
- [ ] Output contract unchanged or documented.
- [ ] Docs updated where behavior changed.
- [ ] Tests added/updated for modified behavior.
- [ ] Packaging safety considered for wheel usage.

## Definition of done

- Code compiles/lints.
- No new breaking output field changes unless documented.
- Security-sensitive data is not committed.
- Behavior is understandable from logs and docs.
