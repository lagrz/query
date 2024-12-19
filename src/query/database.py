import csv
import json
import logging
import sqlite3
from contextlib import contextmanager
from io import StringIO
from typing import Any, Dict, List, Optional

import mysql.connector
import requests

from .dataclass import AdapterSettings

logger = logging.getLogger(__name__)


class DatabaseConnectionBase:
    def __init__(self, settings: AdapterSettings):
        self.settings = settings

    def query(self, query: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class SqliteDatabaseConnection(DatabaseConnectionBase):
    def __init__(self, settings: AdapterSettings):
        super().__init__(settings)
        if not self.settings.database:
            raise ValueError("Database name must be provided for sqlite3 adapter.")
        self._connection = None

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.settings.database)
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
    def __init__(self, settings: AdapterSettings):
        super().__init__(settings)
        required_attrs = ["user", "password", "host", "port", "database"]
        missing_attrs = [
            attr for attr in required_attrs if not getattr(self.settings, attr, None)
        ]
        if missing_attrs:
            raise ValueError(
                f"Missing required settings for MySQL adapter: {', '.join(missing_attrs)}"
            )

        self.config = {
            "user": self.settings.user,
            "password": self.settings.password,
            "host": self.settings.host,
            "port": self.settings.port,
            "database": self.settings.database,
            "pool_name": getattr(self.settings, "pool_name", "pool") or "pool",
            "pool_reset_session": True,  # Ensure sessions are reset when returned to pool
        }
        self.pool = None
        self._active_connections = set()
        self._setup_connection_pool()

    def _setup_connection_pool(self):
        pool_name = self.config.pop("pool_name")
        pool_size = 5
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name, pool_size=pool_size, **self.config
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


class HttpDatabaseConnection(DatabaseConnectionBase):
    def __init__(self, settings: AdapterSettings):
        super().__init__(settings)
        required_attrs = ["base_url", "base_path"]
        missing_attrs = [
            attr for attr in required_attrs if not getattr(self.settings, attr, None)
        ]
        if missing_attrs:
            raise ValueError(
                f"Missing required settings for HTTP adapter: {', '.join(missing_attrs)}"
            )

        self.base_url = self.settings.base_url.rstrip("/")
        self.base_path = self.settings.base_path.strip("/")
        self.http_method = getattr(self.settings, "http_method", "post").lower()
        self.base_payload = getattr(self.settings, "base_payload", {}) or {}
        self.base_headers = getattr(self.settings, "base_headers", {}) or {}
        self.session = requests.Session()

        # Set up the session with base headers
        self.session.headers.update(self.base_headers)

    def _make_request(
        self, endpoint: str, payload: Dict[str, Any]
    ) -> requests.Response:
        """Make HTTP request with the configured method."""
        url = f"{self.base_url}/{self.base_path}/{endpoint.lstrip('/')}"

        # Merge base payload with query-specific payload
        merged_payload = {**self.base_payload, **payload}

        logger.debug(f"Making {self.http_method} request to {url}")
        logger.debug(f"Payload: {merged_payload}")

        try:
            if self.http_method == "get":
                response = self.session.get(url, params=merged_payload)
            else:  # post is default
                response = self.session.post(url, json=merged_payload)

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            raise

    @staticmethod
    def _parse_csv_response(response_text: str) -> List[Dict[str, Any]]:
        """Parse CSV response into list of dictionaries.
        Assumes first line contains headers and following lines contain data."""
        try:
            result = []
            csv_file = StringIO(response_text)
            reader = csv.reader(csv_file)

            # Get headers from first row and convert to lowercase
            headers = [header.lower() for header in next(reader)]

            # Process each data row
            for row in reader:
                # Skip empty rows
                if not row:
                    continue
                # Create dict mapping lowercase headers to row values
                row_dict = {
                    headers[i]: val for i, val in enumerate(row) if i < len(headers)
                }
                result.append(row_dict)

            return result

        except Exception as e:
            logger.error(f"Failed to parse CSV response: {e}")
            raise

    @staticmethod
    def _parse_json_response(response_json: Any) -> List[Dict[str, Any]]:
        """Parse JSON response into list of dictionaries."""
        if isinstance(response_json, list):
            return response_json
        elif isinstance(response_json, dict):
            # If it's a single dict, wrap it in a list
            return [response_json]
        else:
            raise ValueError(f"Unexpected JSON response type: {type(response_json)}")

    def query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query against the HTTP endpoint.
        The query string is expected to be a JSON object containing:
        - endpoint: the API endpoint to call
        - payload: additional payload to merge with base_payload
        """
        try:
            # Parse the query string as JSON
            query_data = json.loads(query)

            if not isinstance(query_data, dict):
                raise ValueError("Query must be a JSON object")

            endpoint = query_data.get("endpoint", "")
            payload = query_data.get("payload", {})

            if not endpoint:
                raise ValueError("Query must contain 'endpoint' field")

            # Make the request
            response = self._make_request(endpoint, payload)

            # Check if we expect CSV response
            if self.base_payload.get("csv") == 1 or payload.get("csv") == 1:
                return self._parse_csv_response(response.text)
            else:
                return self._parse_json_response(response.json())

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON query: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing HTTP query: {e}")
            raise

    def close(self) -> None:
        """Close the HTTP session."""
        if self.session:
            self.session.close()


class DatabaseAdapter:
    def __init__(self, settings: AdapterSettings):
        self.settings = settings
        self._connection: Optional[DatabaseConnectionBase] = None

    def _get_connection(self) -> DatabaseConnectionBase:
        if self._connection:
            return self._connection

        adapter = self.settings.adapter.lower()

        connection_classes = {
            "sqlite3": SqliteDatabaseConnection,
            "mysql": MysqlDatabaseConnection,
            "mysql2": MysqlDatabaseConnection,
            "http": HttpDatabaseConnection,
        }

        connection_class = connection_classes.get(adapter)
        if not connection_class:
            raise ValueError(f"Unsupported adapter: {self.settings.adapter}")

        self._connection = connection_class(self.settings)
        return self._connection

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        connection = self._get_connection()
        return connection.query(query)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
