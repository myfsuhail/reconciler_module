"""
Script version of sample_smus_wheel_install_and_test.ipynb

What this script does:
1) Downloads recon_validators wheel from S3
2) Installs wheel in current Python environment (with dependencies)
3) Imports package classes from wheel
4) Downloads Impala JDBC jar into ./jars (if missing)
5) Creates source and target connections
6) Runs smoke tests + sample validation
7) Saves validation JSON output
"""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

import boto3

# ---- S3 wheel location ----
S3_BUCKET = "amazon-sagemaker-438465132548-us-east-1-242038a46fe7"
S3_KEY = "dzd_aow04z6eei0d47/6k5768rke0n2cn/dev/custom_packages/recon_validators-0.1.0-py3-none-any.whl"

# ---- Connection config ----
IMPALA_JDBC_URL = (
    "jdbc:impala://peanut-impala.cargill.com:21050;"
    "AuthMech=3;"
    "SSL=1;"
    "AllowSelfSignedCerts=1"
)
IMPALA_USERNAME = os.getenv("IMPALA_USERNAME", "ps696636@AP.CORP.CARGILL.COM")
IMPALA_PASSWORD = os.getenv("IMPALA_PASSWORD", "WeRock@123")

ATHENA_REGION = "us-east-1"
ATHENA_S3_STAGING_DIR = "s3://amazon-sagemaker-438465132548-us-east-1-242038a46fe7/dzd_aow04z6eei0d47/6k5768rke0n2cn/dev/sys/athena/"
ATHENA_WORKGROUP = "workgroup-6k5768rke0n2cn-4csydjgqkcyhpj"


def run_cmd(args: list[str]) -> None:
    print("$", " ".join(args))
    subprocess.check_call(args)


def download_wheel() -> Path:
    wheel_local_dir = Path.cwd() / "python_wheels"
    wheel_local_dir.mkdir(parents=True, exist_ok=True)
    wheel_local_path = wheel_local_dir / Path(S3_KEY).name

    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, S3_KEY, str(wheel_local_path))
    print(f"Downloaded wheel to: {wheel_local_path}")
    return wheel_local_path


def install_wheel(wheel_local_path: Path) -> None:
    run_cmd(
        [
            sys.executable,
            "-m",
            "pip",
            "uninstall",
            "-y",
            "recon-validators",
            "recon_validators",
        ]
    )
    run_cmd(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-cache-dir",
            "--force-reinstall",
            str(wheel_local_path),
        ]
    )


def import_from_wheel(wheel_local_path: Path):
    for mod_name in list(sys.modules.keys()):
        if mod_name == "recon_validators" or mod_name.startswith("recon_validators."):
            del sys.modules[mod_name]

    wheel_path_str = str(wheel_local_path)
    if wheel_path_str in sys.path:
        sys.path.remove(wheel_path_str)
    sys.path.insert(0, wheel_path_str)
    importlib.invalidate_caches()

    from recon_validators.connections import (
        JDBCImpalaConnection,
        AthenaConnection,
        ConnectionManager,
    )
    from recon_validators.runner import ValidationRunner
    import recon_validators

    print("✓ Imports loaded")
    print("Loaded from:", recon_validators.__file__)
    return JDBCImpalaConnection, AthenaConnection, ConnectionManager, ValidationRunner


def download_jar() -> str:
    jar_url = "https://repository.cloudera.com/repository/cloudera-repos/Impala/ImpalaJDBC42/2.6.26.1031/ImpalaJDBC42-2.6.26.1031.jar"
    jar_name = "ImpalaJDBC42-2.6.26.1031.jar"

    jars_dir = Path.cwd() / "jars"
    jars_dir.mkdir(parents=True, exist_ok=True)

    jar_path = jars_dir / jar_name
    if jar_path.exists():
        print(f"Jar already exists, skipping download: {jar_path}")
    else:
        print(f"Downloading {jar_name} to {jar_path}...")
        urllib.request.urlretrieve(jar_url, jar_path)
        print(f"Downloaded to: {jar_path}")

    return str(jar_path)


def main() -> None:
    print("Python:", sys.version)
    print("Working directory:", Path.cwd())

    wheel_local_path = download_wheel()
    install_wheel(wheel_local_path)

    JDBCImpalaConnection, AthenaConnection, ConnectionManager, ValidationRunner = (
        import_from_wheel(wheel_local_path)
    )

    jar_path = download_jar()

    conn_manager = ConnectionManager()
    try:
        source_conn = JDBCImpalaConnection(
            jdbc_url=IMPALA_JDBC_URL,
            username=IMPALA_USERNAME,
            password=IMPALA_PASSWORD,
            jar_path=jar_path,
        )

        target_conn = AthenaConnection(
            region_name=ATHENA_REGION,
            s3_staging_dir=ATHENA_S3_STAGING_DIR,
            workgroup=ATHENA_WORKGROUP,
        )

        conn_manager.set_source(source_conn)
        conn_manager.set_target(target_conn)
        print("✓ Connections initialized")

        print("Source test:", conn_manager.source.execute("SELECT 1 AS ok"))
        print("Target test:", conn_manager.target.execute("SELECT 1 AS ok"))

        table_configs = [
            {
                "source_database": "prd_internal_tc1",
                "source_table": "afko",
                "target_database": "minerva_dev_src_sap_cdp_tc1_prd_raw_db",
                "target_table": "afko",
                "tc1": {
                    "type": "count_check",
                    "is_enabled": True,
                    "tolerance_in_percent": 0.0,
                },
                "tc2": {
                    "type": "column_name_check",
                    "is_enabled": True,
                    "skip_columns": ["updated_at"],
                },
                "tc3": {"type": "column_datatype_check", "is_enabled": False},
                "tc4": {
                    "type": "not_null_check",
                    "is_enabled": True,
                    "validation_list": ["aufnr", "xflag"],
                },
                "tc5": {
                    "type": "unique_check",
                    "is_enabled": True,
                    "validation_list": ["aufnr", ["aufnr", "xflag"]],
                },
                "tc6": {"type": "length_check", "is_enabled": False},
            }
        ]

        runner = ValidationRunner(
            source_connection=conn_manager.source,
            target_connection=conn_manager.target,
            executed_by=os.getenv("USER", "smus-user"),
        )

        results = runner.run(table_configs)
        runner.print_summary(results)
        out_file = runner.save_to_json(results, output_dir="validation_results")
        print("Saved:", out_file)

        dq_table_configs = [
            {
                "recon_type": "dq",
                "source_database": "prd_internal_tc1",
                "source_table": "afko",
                "target_database": "minerva_dev_src_sap_cdp_tc1_prd_raw_db",
                "target_table": "afko",
                "tc1": {
                    "type": "count_check",
                    "is_enabled": True,
                    "tolerance_in_percent": 0.0,
                },
                "tc2": {
                    "type": "column_name_check",
                    "is_enabled": True,
                    "skip_columns": ["updated_at"],
                },
                "tc3": {
                    "type": "column_datatype_check",
                    "is_enabled": True,
                    "skip_columns": ["updated_at"],
                },
                "tc4": {
                    "type": "not_null_check",
                    "is_enabled": True,
                    "validation_list": ["aufnr", "xflag"],
                },
                "tc5": {
                    "type": "unique_check",
                    "is_enabled": True,
                    "validation_list": ["aufnr", ["aufnr", "xflag"]],
                },
                "sql_1": {
                    "type": "sql_check",
                    "is_enabled": True,
                    "source_query": """
                        SELECT *
                        FROM prd_internal_tc1.afko
                        WHERE 1 = 1
                          AND gmein = 'KG'
                          AND plnme != 'KG'
                    """,
                    "target_query": """
                        SELECT *
                        FROM minerva_dev_src_sap_cdp_tc1_prd_raw_db.afko
                        WHERE 1 = 1
                          AND gmein = 'KG'
                          AND plnme != 'KG'
                    """,
                    "max_sample_rows": 20,
                },
            }
        ]

        dq_results = runner.run(dq_table_configs)
        runner.print_summary(dq_results)
        dq_out_file = runner.save_to_json(
            dq_results, output_dir="validation_results_dq"
        )
        print("DQ saved:", dq_out_file)

    finally:
        conn_manager.close_all()
        print("✓ Connections closed")


if __name__ == "__main__":
    main()
