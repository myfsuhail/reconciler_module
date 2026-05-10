# Context

## Repository purpose

`recon_validators` provides a small, config-driven framework to validate parity between source and target datasets.

It is intended for data migration, ingestion validation, and post-load quality assurance.

## Typical runtime

- Source: Impala via JDBC (`jaydebeapi` + `JPype1`)
- Target: Athena (`pyathena`)
- Execution: notebook or script in development, wheel in SageMaker for production-style use

## Execution boundaries

What this repository does:
- run query-based validation checks
- aggregate structured results
- save output JSON to file system today
- reserve output extension points for Postgres and Snowflake

What this repository does not do directly:
- orchestrate multi-step data pipelines
- perform data correction/remediation
- push alerts/notifications (can be integrated externally)

## Input contract

`table_configs` is a list of table-level dictionaries. Each item includes:
- `source_database`, `source_table`
- `target_database`, `target_table`
- optional per-check config blocks (`count_check`, `column_name_check`, etc.)

Two compact formats are also supported:
- `recon_type: "basic"` with `source_database`, `target_database`, and `tables_list`.
- `recon_type: "dq"` with one source/target table pair plus explicit DQ checks.

`count_check` is the canonical count type name. Older count-check aliases should not be used in new configs or docs.

### Compact table spec syntax

- `orders` means source table and target table are both `orders`.
- `orders:orders_raw` means source table is `orders`, target table is `orders_raw`.
- `orders:orders_raw(updated_at, del_flag)` additionally skips those columns in column-name and datatype checks.

### Config philosophy

- Explicit check enablement/disablement
- Optional granularity for field-level checks
- Safe defaults where practical
- Backward compatibility preferred over strict schema evolution

## Output contract

`ValidationRunner.run()` returns:
- execution metadata
- per-table check summaries
- per-check details

Results can be persisted via `save_to_json()`.
Future output sinks are planned through `runner_output_mixin.py` for Postgres and Snowflake while preserving the run return payload.

### Status model

Checks should use stable status values:
- `passed`
- `failed`
- `skipped`

### Contract stability expectations

- Existing keys should not be silently renamed.
- New keys should be additive.
- Shape changes should be documented before release.

## Packaging objective

Target distribution format: Python wheel uploaded to S3 and installed in SageMaker environments.

## Environment assumptions

- Runtime has network path to Impala and Athena endpoints.
- Runtime has AWS permissions for Athena and S3 staging.
- Impala JDBC jar is accessible in runtime filesystem.
- Python runtime supports project dependencies.

## Known operational risks

1. **Schema drift** between source and target tables
2. **Credential/permission issues** causing connection failures
3. **Query dialect nuances** across systems
4. **Large-table performance impact** for expensive checks
5. **Output contract drift** affecting downstream parsers

## Mitigation strategies

- use configurable check enablement by table criticality
- add table-level execution logging and durations
- keep output contract documented and version-aware
- run smoke checks before full validation set
- partition validation execution by domain for scale

## Integration context

Common integration pattern:

1. Upstream pipeline writes target data.
2. Validation job executes using wheel package.
3. JSON result persisted to S3.
4. Governance/reporting pipeline consumes results.
5. Data quality dashboard and alerts are updated.

## Documentation map

- `README.md`: user/operator guide
- `AGENTS.md`: automated contributor rules
- `ARCHITECTURE.md`: AI-focused architecture and extension guide
- `SKILLS.md`: capability and ownership map
- `REQUIREMENTS.md`: runtime, packaging, and security requirements
