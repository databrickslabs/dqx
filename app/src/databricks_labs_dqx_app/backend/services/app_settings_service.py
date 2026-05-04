import json
import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.config import WorkspaceConfig

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)

_CONFIG_KEY = "workspace_config"


class AppSettingsService:
    """Manages app configuration in a Delta table using app (SP) credentials.

    Config is stored as a JSON blob in the dq_app_settings table keyed by a
    well-known key.  All operations use the app's service principal, not the
    calling user's OBO token.
    """

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        self._table = f"{sql.catalog}.{sql.schema}.dq_app_settings"

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
        self._sql.execute(sql)
        logger.info(f"Ensured settings table exists: {self._table}")

    def get_config(self) -> WorkspaceConfig:
        """Load the workspace config from the settings table."""
        sql = f"SELECT setting_value FROM {self._table} WHERE setting_key = '{_CONFIG_KEY}'"
        rows = self._sql.query(sql)
        if not rows:
            logger.info("No config found in settings table, returning default")
            return WorkspaceConfig(run_configs=[])

        data = json.loads(rows[0][0])
        result = Installation._unmarshal_type(data, "dq_app_settings", WorkspaceConfig)
        if not isinstance(result, WorkspaceConfig):
            logger.warning("Config unmarshal returned unexpected type, using default")
            return WorkspaceConfig(run_configs=[])
        return result

    def save_config(self, config: WorkspaceConfig) -> WorkspaceConfig:
        """Save the workspace config to the settings table."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        config_dict = config.as_dict()
        value = json.dumps(config_dict)
        escaped = escape_sql_string(value)

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
        self._sql.execute(sql)
        logger.info("Saved workspace config to settings table")
        return config

    def get_setting(self, key: str) -> str | None:
        """Read a single setting value by key."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        escaped_key = escape_sql_string(key)
        sql = f"SELECT setting_value FROM {self._table} WHERE setting_key = '{escaped_key}'"
        rows = self._sql.query(sql)
        return rows[0][0] if rows else None

    def save_setting(self, key: str, value: str) -> None:
        """Upsert a single setting value."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        escaped_key = escape_sql_string(key)
        escaped_val = escape_sql_string(value)
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
        self._sql.execute(sql)
        logger.info("Saved setting: %s", key)
