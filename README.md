# recon_validators

Lightweight, configuration-driven data reconciliation toolkit focused on validating table parity between a source system and a target system.

Current implementation is optimized for:
- Source: Impala via JDBC
- Target: Athena via PyAthena
- Runtime: notebook/script development, then wheel-based reuse in SageMaker

---

## 1) Why this project exists

`recon_validators` helps answer a common migration and ingestion question:

**"Did my data land correctly in the target platform?"**

It provides a practical validation layer that can run repeatedly across many tables with a JSON-style config.

---

## 2) What checks are supported

Per table, you can enable/disable each check:

1. **Count check** (`count_check`)
	- Compares source and target row counts.
	- Supports tolerance percentage.

2. **Column name check** (`column_name_check`)
	- Compares source vs target column names.
	- Supports skipped columns.

3. **Column datatype check** (`column_datatype_check`)
	- Compares compatible datatypes across systems.

4. **Length check** (`length_check`)
	- Compares min/max observed string lengths.

5. **Not-null check** (`not_null_check`)
	- Validates selected columns do not contain null values.

6. **Unique check** (`unique_check`)
	- Validates uniqueness for selected single columns or composite keys.

7. **SQL check** (`sql_check`)
	- Runs user-provided source and target SQL queries.
	- Compares full query result sets as order-insensitive rows and reports mismatch samples.

8. **Spark SQL check** (`spark_sql_check`)
	- Runs user-provided source and target SQL queries through Spark SQL.
	- Compares large result sets using distributed Spark compute (SMUS/Glue friendly).

---

## 3) Repository layout

- `connections.py`
  - `JDBCImpalaConnection`
  - `AthenaConnection`
  - `ConnectionManager`
- `runner.py`
	- `ValidationRunner` orchestration, check extraction, summaries
- `runner_config_mixin.py`
	- legacy/compact config normalization and table spec parsing
- `runner_table_guard_mixin.py`
	- source/target table existence checks and skipped table results
- `runner_output_mixin.py`
	- JSON output today, future Postgres/Snowflake sink extension points
- `count_check.py`
- `column_name_check.py`
- `column_datatype_check.py`
- `length_check.py`
- `not_null_check.py`
- `unique_check.py`
- `sql_check.py`
- `spark_sql_check.py`
- `test_connections.ipynb` (local integration notebook)

---

## 4) Execution model

`ValidationRunner` processes one table config at a time:

1. Run all checks for table A
2. Aggregate statuses and details for table A
3. Move to table B
4. Return one structured JSON-compatible dictionary containing:
	- execution metadata
	- per-table summaries
	- per-check details

---

## 5) Configuration contract (`table_configs`)

Each item in `table_configs` should include:

- `source_database`, `source_table`
- `target_database`, `target_table`
- Validation entries as unique keys (for example: `tc1`, `tc2`, `check_a`)
- Each validation entry must include a required `type`

Supported `type` values:
- `count_check`
- `column_name_check`
- `column_datatype_check`
- `length_check`
- `not_null_check`
- `unique_check`
- `sql_check`
- `spark_sql_check`

Default behavior when validation is omitted:
- Enabled by default: `count_check`, `column_name_check`, `column_datatype_check`
- Disabled by default: `length_check`, `not_null_check`, `unique_check`, `sql_check`, `spark_sql_check`

### Legacy explicit config

Example (redacted and generic):

```python
table_configs = [
	 {
		  "source_database": "src_db",
		  "source_table": "orders",
		  "target_database": "tgt_db",
		  "target_table": "orders",
		  "tc1": {"type": "count_check", "is_enabled": True, "tolerance_in_percent": 0.5},
		  "tc2": {"type": "column_name_check", "is_enabled": True, "skip_columns": ["updated_at"]},
		  "tc3": {"type": "column_datatype_check", "is_enabled": True},
		  "tc4": {"type": "length_check", "is_enabled": True},
		  "tc5": {"type": "not_null_check", "is_enabled": True, "validation_list": ["order_id", "customer_id"]},
		  "tc6": {
				"type": "unique_check",
				"is_enabled": True,
				"validation_list": ["order_id", ["order_id", "line_number"]]
		  },
		  "sql_1": {
				"type": "sql_check",
				"is_enabled": True,
				"source_query": "SELECT order_id FROM src_db.orders WHERE status = 'ACTIVE'",
				"target_query": "SELECT order_id FROM tgt_db.orders WHERE status = 'ACTIVE'",
				"max_sample_rows": 20,
		  },
		  "spark_sql_1": {
				"type": "spark_sql_check",
				"is_enabled": False,
				"source_query": "SELECT order_id FROM src_db.orders WHERE status = 'ACTIVE'",
				"target_query": "SELECT order_id FROM tgt_db.orders WHERE status = 'ACTIVE'",
				"max_sample_rows": 20,
		  },
	 }
]
```

### Compact `basic` config

Use `basic` when only source database, target database, and table list are needed. It runs `count_check`, `column_name_check`, and `column_datatype_check` by default with count tolerance fixed at `0.0`.

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

Table spec meanings:
- `orders`: source and target table are both `orders`
- `customers:customers_raw`: source is `customers`, target is `customers_raw`
- `items:items_raw(updated_at, del_flag)`: skip columns are applied to column-name and datatype checks

Duplicate entries in `tables_list` are executed as provided.

### Compact `dq` config

Use `dq` when basic reconciliation should always run and additional data-quality checks are provided.

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
			"source_query": """
				SELECT order_id, status
				FROM src_db.orders
				WHERE status = 'ACTIVE'
			""",
			"target_query": """
				SELECT order_id, status
				FROM tgt_db.orders
				WHERE status = 'ACTIVE'
			""",
		},
		"spark_sql_1": {
			"type": "spark_sql_check",
			"is_enabled": True,
			"source_query": """
				SELECT order_id, status
				FROM src_db.orders
				WHERE status = 'ACTIVE'
			""",
			"target_query": """
				SELECT order_id, status
				FROM tgt_db.orders
				WHERE status = 'ACTIVE'
			""",
			"max_sample_rows": 20,
		},
	}
]
```

`sql_check` compares result rows as a multiset, so row order does not matter. Query outputs should return the same logical columns and comparable values on both systems.

`spark_sql_check` uses the same multiset comparison semantics, but executes with Spark SQL for scalable distributed comparison in SMUS/Glue environments.

---

## 6) Typical usage

```python
from connections import JDBCImpalaConnection, AthenaConnection, ConnectionManager
from runner import ValidationRunner

source_conn = JDBCImpalaConnection(
	 jdbc_url="<jdbc-url>",
	 username="<username>",
	 password="<password>",
	 jar_path="/path/to/ImpalaJDBC42.jar",
)

target_conn = AthenaConnection(
	 region_name="us-east-1",
	 s3_staging_dir="s3://<bucket>/<prefix>/",
	 workgroup="<workgroup>",
)

manager = ConnectionManager()
manager.set_source(source_conn)
manager.set_target(target_conn)

runner = ValidationRunner(source_connection=manager.source, target_connection=manager.target)
results = runner.run(table_configs)
runner.print_summary(results)
runner.save_to_json(results, output_dir="validation_results")

manager.close_all()
```

---

## 7) Output structure overview

Top-level keys:

- `execution_metadata`
- `tables`

Execution metadata includes:
- execution id
- start/end timestamps
- durations
- total tables and checks
- passed/failed/skipped totals

Each table result includes:
- source/target identifiers
- table execution timing
- check counts
- `checks` list containing detailed payload per validator

JSON is the current supported persistence path via `save_to_json()`. Future Postgres and Snowflake sinks are reserved in `runner_output_mixin.py`.

---

## 8) SageMaker wheel workflow (recommended)

1. Build a wheel in CI or local build host.
2. Upload wheel artifact to S3.
3. Install wheel in SageMaker notebook/job startup.
4. Load config from controlled source (S3/config service).
5. Run validations.
6. Persist results to S3 for audit and trend analysis.

---

## 9) Operational recommendations

- Keep validation configs externalized and versioned.
- Tag every run with execution metadata and operator identity.
- Store results in S3 paths partitioned by date/run-id.
- Build dashboards using pass/fail/skipped metrics.
- Add alerting when critical tables fail.

### Logging and progress visibility

`recon_validators` uses Python's standard `logging` package. Configure logging in notebooks/jobs before running validations so progress is visible while long-running checks execute:

```python
import logging

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s %(levelname)s %(name)s - %(message)s",
	force=True,
)
logging.getLogger("recon_validators").setLevel(logging.INFO)
```

The runner emits `INFO` lifecycle logs when each validation run, table, and check starts and completes. Use `DEBUG` only for non-sensitive diagnostics; do not log credentials, SQL text, JDBC URLs, tokens, or secret-bearing paths.

---

## 10) Security guidance

- Do **not** commit plaintext credentials.
- Use IAM roles, Secrets Manager, or runtime env vars.
- Treat JDBC URLs, usernames, and staging paths as sensitive.
- Use least-privilege permissions for Athena, Glue, and S3.

---

## 11) Known implementation notes

- Validators are config-driven and return JSON-serializable structures.
- SQL style assumes compatibility with Impala and Athena patterns used in this repository.
- Notebook `test_connections.ipynb` is for smoke/integration testing and should not carry secrets.

---

## 12) Next hardening items (recommended)

- Add formal `pyproject.toml` for wheel builds.
- Normalize internal imports for package-safe wheel execution.
- Add unit tests for each check runner.
- Add schema tests for output contract stability.
- Add CI checks for linting, tests, and build artifacts.

---

## 13) Build wheel and use in SMUS/SageMaker

This repository is now configured for wheel packaging using `pyproject.toml`.

### A) Build wheel locally

From repository root:

1. Install build tool:
	- `python -m pip install --upgrade build`
2. Build package:
	- `python -m build`
3. Wheel will be created under:
	- `dist/recon_validators-0.1.0-py3-none-any.whl`

### B) Upload wheel to S3

Use AWS CLI:

- `aws s3 cp dist/recon_validators-0.1.0-py3-none-any.whl s3://<your-bucket>/<path>/`

### C) Install in SageMaker / SMUS notebook

In a notebook cell:

```python
%pip install --no-cache-dir "s3://<your-bucket>/<path>/recon_validators-0.1.0-py3-none-any.whl"
```

Then import:

```python
from recon_validators import (
	 AthenaConnection,
	 ConnectionManager,
	 JDBCImpalaConnection,
	 ValidationRunner,
)
```

### D) Minimal runtime checklist for SMUS

- JDBC jar is available in runtime path
- Network access to Impala and Athena endpoints
- IAM permission for Athena + S3 staging path
- Secrets supplied via environment/secret store (not notebook plaintext)

### E) Version bump for next release

Before producing a new wheel version:

1. Update `__version__` in `__init__.py`
2. Update `version` in `pyproject.toml`
3. Rebuild wheel and upload to new S3 versioned path
