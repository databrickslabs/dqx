import asyncio
import logging

from databricks import sql as databricks_sql
from databricks.sql.client import Connection

logger = logging.getLogger(__name__)


class SQLConnector:
    """Execute queries against a Databricks SQL Warehouse."""

    def __init__(self, access_token: str, server_hostname: str, http_path: str) -> None:
        self._access_token = access_token
        self._server_hostname = server_hostname
        self._http_path = http_path

    def _connect(self) -> Connection:
        return databricks_sql.connect(
            server_hostname=self._server_hostname,
            http_path=self._http_path,
            access_token=self._access_token,
        )

    def run_sql_statement(self, sql: str) -> list[dict]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

    async def run_sql_statement_async(self, sql: str) -> list[dict]:
        return await asyncio.to_thread(self.run_sql_statement, sql)
