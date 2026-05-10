# Skills Map

This map helps contributors quickly find where to implement changes.

## How to use this map

When a requirement arrives, identify the impacted layer first:

1. Connection issue -> Connection layer
2. Validation semantics issue -> Validation layer
3. Config parsing issue -> Config normalization layer
4. Aggregation/reporting issue -> Orchestration layer
5. Output persistence issue -> Output layer
6. Build/install/runtime issue -> Packaging & operations skills

## Connection layer

- `JDBCImpalaConnection`: Impala JDBC connectivity and query execution
- `AthenaConnection`: Athena connectivity and query execution
- `ConnectionManager`: source/target connection lifecycle

### Skills needed

- JDBC driver and classpath handling
- Credential handling without source control leakage
- Query execution and cursor lifecycle safety
- Cross-environment path handling (local vs SageMaker)

### Common tasks

- Add clearer connection failure diagnostics
- Make jar-path discovery robust
- Standardize execute behavior and return shape
- Ensure close semantics are safe and idempotent

## Validation layer

- `CountCheckRunner`: table row count and tolerance checks
- `ColumnNameCheckRunner`: source-target column name parity
- `ColumnDatatypeCheckRunner`: datatype compatibility checks
- `LengthCheckRunner`: min/max string length comparison
- `NotNullCheckRunner`: NULL violation checks for selected columns
- `UniqueCheckRunner`: duplicate checks for key candidates
- `SqlCheckRunner`: user-provided source/target SQL result-set comparison
- `SparkSqlCheckRunner`: distributed Spark SQL result-set comparison for large datasets

### Skills needed

- SQL generation patterns for Impala and Athena
- Spark SQL/DataFrame comparison patterns for distributed execution
- Null-safe and empty-table-safe handling
- Column existence and casing behavior
- Result-set comparison semantics and sample-size controls
- Contract-stable result payload design

### Common tasks

- Add config normalization behavior
- Improve mismatch diagnostics per field/constraint
- Handle absent columns gracefully as `skipped`
- Preserve status semantics and message clarity

## Config normalization layer

- `runner_config_mixin.py`: normalizes legacy and compact configs before validation execution.
- Supports legacy per-table configs, `recon_type="basic"`, and `recon_type="dq"`.
- Uses canonical validation type `count_check`.

### Skills needed

- Backward-compatible schema handling
- Clear `ValueError` messages for invalid user configs
- Table spec parsing for `source`, `source:target`, and `source:target(skip_cols)`

### Common tasks

- Add a new compact config mode
- Adjust default validations
- Update parser validation rules without breaking legacy configs

## Orchestration layer

- `ValidationRunner`: executes all checks per table, aggregates output, prints summary
- `runner_table_guard_mixin.py`: skips all checks for a table when source/target table is missing.

### Skills needed

- Execution metadata design
- Table-level aggregation and summarization
- Stable JSON structure for downstream consumers
- Human-readable and machine-readable output balance

### Common tasks

- Add new check to orchestration sequence
- Improve summary counters/timing fields
- Keep output shape backward-compatible
- Improve save-to-json behavior and file naming

## Output layer

- `runner_output_mixin.py`: builds final output, saves JSON, and reserves future Postgres/Snowflake sinks.

### Skills needed

- Stable JSON payload design
- Safe persistence without logging secrets
- Future database sink design for Postgres and Snowflake

### Common tasks

- Add Postgres output support
- Add Snowflake output support
- Keep `save_to_json()` behavior stable

## Operational skill focus

- Cross-system SQL compatibility (Impala/Athena)
- Output schema consistency
- Config-driven validation behavior
- Package readiness for wheel distribution

## Packaging and release skills

- Python wheel build flow (`pyproject.toml`, build backend)
- Package-safe imports and module layout
- Dependency declaration and version pin strategy
- SageMaker runtime installation patterns

## Observability skills

- Structured logging at check boundaries
- Failure messages that aid triage
- Avoidance of noisy logs and sensitive values
- Run-level traceability using execution IDs

## Decision matrix

### Symptom: "Validation failed unexpectedly"

Investigate:
1. validator-specific config normalization
2. generated SQL correctness
3. source/target schema drift
4. null/duplicate edge-case behavior

### Symptom: "Works in notebook, fails after wheel install"

Investigate:
1. absolute vs relative imports
2. packaging metadata and dependencies
3. runtime path assumptions
4. jar/dependency availability in target environment

### Symptom: "Output parser downstream broke"

Investigate:
1. key name changes in result payload
2. nesting changes in `tables` or `checks`
3. status value changes
4. undocumented format modifications

### Symptom: "Compact config expands incorrectly"

Investigate:
1. `runner_config_mixin.py`
2. `DEFAULT_ENABLED_TYPES` in `runner.py`
3. `TYPE_TO_CHECK_KEY` and `type_to_runner` mappings in `runner.py`
4. README compact config examples

## Skill maturity levels

- **Level 1**: Execute existing checks with config changes only.
- **Level 2**: Modify one validator while preserving contract.
- **Level 3**: Add a new validator and integrate orchestration + docs + tests.
- **Level 4**: Production hardening across packaging, observability, and CI.
