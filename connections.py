"""
Lightweight database connection module for Impala (JDBC) and Athena/Glue
"""

import logging
from pathlib import Path
from typing import Any, Optional, Callable

logger = logging.getLogger(__name__)


class JDBCImpalaConnection:
    """Lightweight JDBC connection for Impala"""

    def __init__(self, jdbc_url: str, username: str, password: str, jar_path: str):
        """
        Initialize Impala JDBC connection

        Args:
            jdbc_url: JDBC connection URL for Impala
            username: Kerberos principal or username
            password: Password for authentication
            jar_path: Path to ImpalaJDBC jar file
        """
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.jar_path = jar_path
        self.connection = None
        self._connect()

    def _connect(self):
        """Establish JDBC connection"""
        try:
            import jaydebeapi
            import jpype

            resolved_jar = str(Path(self.jar_path).expanduser().resolve())
            if not Path(resolved_jar).exists():
                raise FileNotFoundError(f"Impala JDBC jar not found at: {resolved_jar}")

            # If JVM is already started, explicitly add classpath for this session.
            # jaydebeapi cannot restart JVM with new classpath once it's running.
            if jpype.isJVMStarted():
                jpype.addClassPath(resolved_jar)

            driver_candidates = [
                "com.cloudera.impala.jdbc.Driver",
                "com.cloudera.impala.jdbc42.Driver",
                "com.cloudera.impala.jdbc41.Driver",
            ]

            last_error = None
            for driver_class in driver_candidates:
                try:
                    self.connection = jaydebeapi.connect(
                        driver_class,
                        self.jdbc_url,
                        [self.username, self.password],
                        [resolved_jar],
                    )
                    logger.info(
                        "✓ Connected to Impala via JDBC using driver %s", driver_class
                    )
                    return
                except Exception as e:
                    last_error = e

            raise RuntimeError(
                "Unable to load any Impala JDBC driver class from jar "
                f"{resolved_jar}. Last error: {last_error}"
            )
        except ImportError:
            logger.error("jaydebeapi not installed. Run: pip install jaydebeapi JPype1")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Impala: {e}")
            raise

    def execute(self, sql: str) -> list[dict]:
        """
        Execute SQL query and return results as list of dicts

        Args:
            sql: SQL query string

        Returns:
            List of dictionaries with column names as keys
        """
        if not self.connection:
            raise ConnectionError("Not connected to Impala")

        cursor = self.connection.cursor()
        try:
            cursor.arraysize = 10000
        except Exception:
            pass
            
        try:
            cursor.execute(sql)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            
            all_rows = []
            chunk_size = getattr(cursor, "arraysize", 10000)
            
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                
                chunk_len = len(chunk)
                all_rows.extend([dict(zip(columns, row)) for row in chunk])
                logger.info(f"{chunk_len} readed with total {len(all_rows)}")
                
            return all_rows
        finally:
            cursor.close()

    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            logger.info("✓ Impala connection closed")


class ImpylaConnection:
    """Lightweight native Python connection for Impala using impyla (bypasses Java/JDBC)"""

    def __init__(self, host: str, port: int, username: str = None, password: str = None, 
                 use_ssl: bool = True, auth_mechanism: str = 'LDAP', kerberos_service_name: str = 'impala',
                 keytab_path: str = None, kerberos_principal: str = None):
        """
        Initialize native Impala connection
        
        Args:
            host: Impala daemon hostname
            port: Impala daemon port (usually 21050)
            username: LDAP username (not needed for Kerberos if ticket exists)
            password: Password for authentication
            use_ssl: Whether to use SSL/TLS
            auth_mechanism: 'LDAP', 'GSSAPI' (Kerberos), or 'PLAIN'
            kerberos_service_name: Service name for Kerberos (default 'impala')
            keytab_path: Path to keytab file for automatic kinit
            kerberos_principal: Principal name for automatic kinit (e.g. user@REALM.COM)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.auth_mechanism = auth_mechanism
        self.kerberos_service_name = kerberos_service_name
        self.keytab_path = keytab_path
        self.kerberos_principal = kerberos_principal
        self.connection = None
        self._connect()

    def _connect(self):
        """Establish native connection via Thrift"""
        try:
            import subprocess
            from impala.dbapi import connect
            
            # Auto-kinit if keytab provided
            if self.auth_mechanism == 'GSSAPI' and self.keytab_path and self.kerberos_principal:
                try:
                    subprocess.run(["kinit", "-kt", self.keytab_path, self.kerberos_principal], check=True)
                    logger.info(f"Successfully authenticated using kinit for {self.kerberos_principal}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to authenticate with kinit: {e}")
                    raise RuntimeError(f"Kerberos kinit failed: {e}")
            
            kwargs = {
                'host': self.host,
                'port': self.port,
                'auth_mechanism': self.auth_mechanism,
                'use_ssl': self.use_ssl
            }
            
            if self.auth_mechanism == 'LDAP':
                kwargs['user'] = self.username
                kwargs['password'] = self.password
            elif self.auth_mechanism == 'GSSAPI':
                kwargs['kerberos_service_name'] = self.kerberos_service_name
            
            self.connection = connect(**kwargs)
            logger.info(f"✓ Connected to Impala natively via impyla ({self.auth_mechanism})")
        except ImportError:
            logger.error("impyla not installed. Run: pip install impyla thrift_sasl")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Impala natively: {e}")
            raise

    def execute(self, sql: str) -> list[dict]:
        """
        Execute SQL query and return results as list of dicts.
        Uses fetchmany for progress logging.
        """
        if not self.connection:
            raise ConnectionError("Not connected to Impala")

        cursor = self.connection.cursor()
        try:
            # Impyla cursor usually defaults arraysize to 10000 or similar
            cursor.arraysize = 10000
            
            cursor.execute(sql)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            
            all_rows = []
            chunk_size = getattr(cursor, "arraysize", 10000)
            
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                
                chunk_len = len(chunk)
                all_rows.extend([dict(zip(columns, row)) for row in chunk])
                logger.info(f"{chunk_len} readed with total {len(all_rows)} via native impyla")
                
            return all_rows
        finally:
            cursor.close()

    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            logger.info("✓ Native Impala connection closed")


class AthenaConnection:
    """Lightweight connection for AWS Athena/Glue"""

    def __init__(
        self, region_name: str, s3_staging_dir: str, workgroup: Optional[str] = None
    ):
        """
        Initialize Athena connection

        Args:
            region_name: AWS region (e.g., 'us-east-1')
            s3_staging_dir: S3 path for query results
            workgroup: Athena workgroup name (optional)
        """
        self.region_name = region_name
        self.s3_staging_dir = s3_staging_dir
        self.workgroup = workgroup
        self.connection = None
        self._connect()

    def _connect(self):
        """Establish Athena connection"""
        try:
            from pyathena import connect
            from pyathena.arrow.cursor import ArrowCursor

            self.connection = connect(
                region_name=self.region_name,
                s3_staging_dir=self.s3_staging_dir,
                work_group=self.workgroup,
                cursor_class=ArrowCursor,
            )
            logger.info("✓ Connected to Athena/Glue using ArrowCursor")
        except ImportError:
            logger.error("pyathena not installed. Run: pip install pyathena")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Athena: {e}")
            raise

    def execute(self, sql: str) -> list[dict]:
        """
        Execute SQL query and return results as list of dicts

        Args:
            sql: SQL query string

        Returns:
            List of dictionaries with column names as keys
        """
        if not self.connection:
            raise ConnectionError("Not connected to Athena")

        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            
            if hasattr(cursor, "as_arrow"):
                arrow_table = cursor.as_arrow()
                if arrow_table is None:
                    return []
                return arrow_table.to_pylist()

            # Fallback if ArrowCursor isn't used
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            logger.info("✓ Athena connection closed")


class ConnectionManager:
    """Simple manager for source and target connections"""

    def __init__(self):
        self.source = None
        self.target = None

    def set_source(self, connection):
        """Set source database connection"""
        self.source = connection
        logger.info("Source connection registered")

    def set_target(self, connection):
        """Set target database connection"""
        self.target = connection
        logger.info("Target connection registered")

    def close_all(self):
        """Close all connections"""
        if self.source:
            self.source.close()
        if self.target:
            self.target.close()
        logger.info("All connections closed")
