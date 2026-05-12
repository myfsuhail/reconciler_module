"""
Lightweight database connection module for Impala (JDBC) and Athena/Glue
"""

import logging
import os
import socket
import ssl
from pathlib import Path
from typing import Optional

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
            cursor.arraysize = 25000
        except Exception:
            pass
            
        try:
            cursor.execute(sql)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            
            all_rows = []
            chunk_size = getattr(cursor, "arraysize", 25000)
            
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

    def __init__(
        self,
        host: str,
        port: int,
        username: str = None,
        password: str = None,
        use_ssl: bool = True,
        auth_mechanism: str = "LDAP",
        kerberos_service_name: str = "impala",
        keytab_path: str = None,
        kerberos_principal: str = None,
        ca_cert: str = None,
        ssl_ciphers: str = "DEFAULT:@SECLEVEL=1",
        patch_ssl_socket: bool = True,
        tls_preflight: bool = True,
        auth_mechanisms: list[str] = None,
        timeout: int = 30,
    ):
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
            ca_cert: Optional PEM file path for TLS validation
            ssl_ciphers: SSL cipher override for Java8-compatible runtimes
            patch_ssl_socket: Apply impyla thrift socket monkey patch for custom SSL context
            tls_preflight: Perform a TLS handshake probe before connecting
            auth_mechanisms: Optional auth fallback order (e.g. ['LDAP', 'PLAIN'])
            timeout: Connection timeout in seconds
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
        self.ca_cert = str(Path(ca_cert).expanduser().resolve()) if ca_cert else None
        self.ssl_ciphers = ssl_ciphers
        self.patch_ssl_socket = patch_ssl_socket
        self.tls_preflight = tls_preflight
        self.auth_mechanisms = auth_mechanisms or [auth_mechanism]
        self.timeout = timeout
        self.connection = None
        self._connect()

    def _patch_impyla_ssl_socket(self):
        """Monkey patch impyla thrift socket creation with custom SSL context."""
        from impala import _thrift_api
        from impala import hiveserver2 as _hs2
        from thrift.transport.TSocket import TSocket
        from thrift.transport.TSSLSocket import TSSLSocket

        ca_cert = self.ca_cert
        ssl_ciphers = self.ssl_ciphers

        class ImpalaCompatTSSLSocket(TSSLSocket):
            def isOpen(self):
                return self.handle is not None

        def compat_get_socket(host, port, use_ssl, cert_file):
            if not use_ssl:
                return TSocket(host, port)

            effective_ca = cert_file or ca_cert
            context = ssl.create_default_context(cafile=effective_ca)
            context.set_ciphers(ssl_ciphers)

            return ImpalaCompatTSSLSocket(
                host,
                port,
                ssl_context=context,
                server_hostname=host,
                validate_callback=lambda cert, hostname: None,
            )

        _thrift_api.get_socket = compat_get_socket
        _hs2.get_socket = compat_get_socket

    def _tls_preflight(self):
        """Verify TLS handshake independently before opening impyla connection."""
        if not self.ca_cert:
            logger.warning("Skipping TLS preflight because ca_cert is not configured")
            return

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=self.ca_cert)
        context.set_ciphers(self.ssl_ciphers)

        with socket.create_connection((self.host, self.port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=self.host) as tls_sock:
                logger.info("TLS preflight succeeded using %s", tls_sock.version())

    def _connect(self):
        """Establish native connection via Thrift"""
        try:
            import subprocess
            from impala.dbapi import connect
            
            # Auto-kinit if keytab provided
            if (
                "GSSAPI" in self.auth_mechanisms
                and self.keytab_path
                and self.kerberos_principal
            ):
                try:
                    subprocess.run(
                        ["kinit", "-kt", self.keytab_path, self.kerberos_principal],
                        check=True,
                    )
                    logger.info(
                        "Successfully authenticated using kinit for principal"
                    )
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to authenticate with kinit: {e}")
                    raise RuntimeError(f"Kerberos kinit failed: {e}")

            if self.use_ssl and self.tls_preflight:
                self._tls_preflight()

            if self.use_ssl and self.patch_ssl_socket:
                self._patch_impyla_ssl_socket()
            
            last_error = None
            for mechanism in self.auth_mechanisms:
                kwargs = {
                    "host": self.host,
                    "port": self.port,
                    "auth_mechanism": mechanism,
                    "use_ssl": self.use_ssl,
                    "timeout": self.timeout,
                }

                if self.use_ssl and self.ca_cert:
                    kwargs["ca_cert"] = self.ca_cert

                if mechanism in {"LDAP", "PLAIN"}:
                    kwargs["user"] = self.username
                    kwargs["password"] = self.password
                elif mechanism == "GSSAPI":
                    kwargs["kerberos_service_name"] = self.kerberos_service_name

                try:
                    self.connection = connect(**kwargs)
                    self.auth_mechanism = mechanism
                    logger.info(
                        "✓ Connected to Impala natively via impyla (%s)",
                        mechanism,
                    )
                    return
                except Exception as e:
                    last_error = e
                    logger.warning(
                        "Impyla connection failed with auth_mechanism=%s: %s",
                        mechanism,
                        e,
                    )

            raise RuntimeError(f"Failed to connect with impyla: {last_error}")
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
            # Impyla cursor usually defaults arraysize to 25000 or similar
            cursor.arraysize = 25000
            
            cursor.execute(sql)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            
            all_rows = []
            chunk_size = getattr(cursor, "arraysize", 25000)
            
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                
                chunk_len = len(chunk)
                all_rows.extend([dict(zip(columns, row)) for row in chunk])
                logger.info("%s rows read; total=%s via native impyla", chunk_len, len(all_rows))
                
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
