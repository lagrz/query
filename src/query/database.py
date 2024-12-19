import logging
import sqlite3
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import mysql.connector
from mysql.connector import pooling

from .dataclass import AdapterSettings

logger = logging.getLogger(__name__)


class DatabaseConnectionBase:
    def query(self, query: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class SqliteDatabaseConnection(DatabaseConnectionBase):
    def __init__(self, database: str):
        self.database = database
        self._connection = None

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.database)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def query(self, query: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                logger.debug(f"Executing query: {query}")
                cursor.execute(query)
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            except sqlite3.Error as e:
                logger.error(f"SQLite error executing query: {e}")
                raise
            finally:
                cursor.close()


class MysqlDatabaseConnection(DatabaseConnectionBase):
    def __init__(
            self,
            user: str,
            password: str,
            host: str,
            port: int,
            database: str,
            **kwargs: Any,
    ):
        self.config = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "database": database,
            "pool_reset_session": True,  # Ensure sessions are reset when returned to pool
            **kwargs,
        }
        self.pool = None
        self._active_connections = set()
        self._setup_connection_pool()

    def _setup_connection_pool(self):
        pool_name = "mypool"
        pool_size = 5
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **self.config
        )

    @contextmanager
    def get_connection(self):
        conn = self.pool.get_connection()
        self._active_connections.add(conn)
        try:
            yield conn
        finally:
            if conn in self._active_connections:
                self._active_connections.remove(conn)
            conn.close()

    def query(self, query: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            except mysql.connector.Error as e:
                logger.error(f"MySQL error executing query: {e}")
                raise
            finally:
                cursor.close()

    def close(self) -> None:
        if self.pool:
            # Close any remaining active connections
            for conn in list(self._active_connections):
                try:
                    conn.close()
                    self._active_connections.remove(conn)
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

            # Set pool to None to ensure no new connections are created
            self.pool = None


class DatabaseAdapter:
    def __init__(self, settings: AdapterSettings):
        self.settings = settings
        self._connection: Optional[DatabaseConnectionBase] = None

    def _get_connection(self) -> DatabaseConnectionBase:
        if self._connection:
            return self._connection

        adapter = self.settings.adapter.lower()
        if adapter == "sqlite3":
            if not self.settings.database:
                raise ValueError("Database name must be provided for sqlite3 adapter.")
            self._connection = SqliteDatabaseConnection(self.settings.database)
        elif adapter in {"mysql", "mysql2"}:
            required_attrs = ["user", "password", "host", "port", "database"]
            missing_attrs = [
                attr
                for attr in required_attrs
                if not getattr(self.settings, attr, None)
            ]
            if missing_attrs:
                raise ValueError(
                    f"Missing required settings for MySQL adapter: {', '.join(missing_attrs)}"
                )
            self._connection = MysqlDatabaseConnection(
                user=self.settings.user,
                password=self.settings.password,
                host=self.settings.host,
                port=self.settings.port,
                database=self.settings.database,
            )
        else:
            raise ValueError(f"Unsupported adapter: {self.settings.adapter}")

        return self._connection

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        connection = self._get_connection()
        return connection.query(query)

    def close(self) -> None:
        if self._connection:
            self._connection.close()