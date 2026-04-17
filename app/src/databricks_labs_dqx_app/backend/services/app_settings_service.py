import json
import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

logger = logging.getLogger(__name__)

_CONFIG_KEY = "workspace_config"


class AppSettingsService:
    """Manages app configuration in a Delta table using app (SP) credentials.

    Config is stored as a JSON blob in the dq_app_settings table keyed by a
    well-known key.  All operations use the app's service principal, not the
    calling user's OBO token.
    """

    def __init__(self, ws: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> None:
        self._ws = ws
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._table = f"{catalog}.{schema}.dq_app_settings"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_table(self) -> None:
        """Create the settings table if it doesn't exist."""
        sql = (
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  setting_key STRING NOT NULL,"
            "  setting_value STRING,"
            "  updated_at TIMESTAMP,"
            "  updated_by STRING"
            ")"
        )
        self._execute(sql)
        logger.info(f"Ensured settings table exists: {self._table}")

    def get_config(self) -> WorkspaceConfig:
        """Load the workspace config from the settings table."""
        sql = f"SELECT setting_value FROM {self._table} WHERE setting_key = '{_CONFIG_KEY}'"
        rows = self._query(sql)
        if not rows:
            logger.info("No config found in settings table, returning default")
            return WorkspaceConfig(run_configs=[])

        raw = rows[0][0]
        # Undo damage from earlier save_config that used \' instead of ''
        cleaned = raw.replace("\\'", "'") if "\\'" in raw else raw
        data = json.loads(cleaned, strict=False)
        result = Installation._unmarshal_type(data, "dq_app_settings", WorkspaceConfig)
        if not isinstance(result, WorkspaceConfig):
            logger.warning("Config unmarshal returned unexpected type, using default")
            return WorkspaceConfig(run_configs=[])
        return result

    def save_config(self, config: WorkspaceConfig) -> WorkspaceConfig:
        """Save the workspace config to the settings table."""
        config_dict = config.as_dict()
        value = json.dumps(config_dict)
        escaped = value.replace("'", "''")

        # Upsert via MERGE
        sql = (
            f"MERGE INTO {self._table} AS target "
            f"USING (SELECT '{_CONFIG_KEY}' AS setting_key) AS source "
            "ON target.setting_key = source.setting_key "
            "WHEN MATCHED THEN UPDATE SET "
            f"  setting_value = '{escaped}', "
            "  updated_at = current_timestamp() "
            "WHEN NOT MATCHED THEN INSERT (setting_key, setting_value, updated_at) "
            f"VALUES ('{_CONFIG_KEY}', '{escaped}', current_timestamp())"
        )
        self._execute(sql)
        logger.info("Saved workspace config to settings table")
        return config

    def get_setting(self, key: str) -> str | None:
        """Read a single setting value by key."""
        escaped_key = key.replace("'", "''")
        sql = f"SELECT setting_value FROM {self._table} WHERE setting_key = '{escaped_key}'"
        rows = self._query(sql)
        return rows[0][0] if rows else None

    def save_setting(self, key: str, value: str) -> None:
        """Upsert a single setting value."""
        escaped_key = key.replace("'", "''")
        escaped_val = value.replace("'", "''")
        sql = (
            f"MERGE INTO {self._table} AS target "
            f"USING (SELECT '{escaped_key}' AS setting_key) AS source "
            "ON target.setting_key = source.setting_key "
            "WHEN MATCHED THEN UPDATE SET "
            f"  setting_value = '{escaped_val}', "
            "  updated_at = current_timestamp() "
            "WHEN NOT MATCHED THEN INSERT (setting_key, setting_value, updated_at) "
            f"VALUES ('{escaped_key}', '{escaped_val}', current_timestamp())"
        )
        self._execute(sql)
        logger.info("Saved setting: %s", key)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str) -> None:
        """Execute a SQL statement that doesn't return rows."""
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}")

    def _query(self, sql: str) -> list[list[str]]:
        """Execute a SQL query and return rows as lists of strings."""
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}")

        if resp.result and resp.result.data_array:
            return resp.result.data_array
        return []
