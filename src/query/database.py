import logging
import sqlite3
from typing import Any, Dict, List, Optional

import mysql.connector

from .dataclass import AdapterSettings  # Ensure this is the correct module path

logger = logging.getLogger(__name__)


class DatabaseConnectionBase:
    def query(self, query: str) -> List[Dict[str, Any]]:
        raise NotImplementedError


class SqliteDatabaseConnection(DatabaseConnectionBase):
    def __init__(self, database: str):
        self.database = database

    def query(self, query: str) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row  # Enable accessing columns by name
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                result = [dict(row) for row in rows]
                return result
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
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.extra_params = kwargs  # Allow passing additional parameters if needed

    def query(self, query: str) -> List[Dict[str, Any]]:
        with mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            **self.extra_params,
        ) as conn:
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
