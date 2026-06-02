import json
import logging

from databricks.labs.dqx.config import WorkspaceConfig
from pydantic import TypeAdapter, ValidationError

from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql

logger = logging.getLogger(__name__)

_CONFIG_KEY = "workspace_config"

# Module-level adapter so we pay the type-tree walk once at import time
# rather than on every ``get_config`` call. ``TypeAdapter`` is Pydantic's
# public v2 surface for validating non-BaseModel types against a target
# type (here, the DQX ``WorkspaceConfig`` dataclass). We use this
# instead of :func:`databricks.labs.blueprint.installation.Installation._unmarshal_type`
# — that one is a leading-underscore private API in blueprint and the
# next blueprint release can change its signature without warning.
#
# Pydantic round-trips ``WorkspaceConfig`` exactly the same way the
# blueprint helper does (verified against blueprint output during
# refactor). And it's the same path the project's ``ConfigIn`` /
# ``ConfigOut`` BaseModel response models already use to deserialize
# the user-POSTed config — so we're aligning the LOAD path with the
# trusted SAVE path rather than relying on a separate, private
# deserializer.
_WORKSPACE_CONFIG_ADAPTER: TypeAdapter[WorkspaceConfig] = TypeAdapter(WorkspaceConfig)


class AppSettingsService:
    """Manages app configuration backed by ``dq_app_settings``.

    Config is stored as a JSON blob keyed by a well-known key.  All
    operations use the app's service principal, not the calling
    user's OBO token.

    The ``dq_app_settings`` table is one of the OLTP tables that lives
    in Lakebase Postgres when ``conf.lakebase_enabled`` is true and in
    Delta otherwise. The injected executor decides which.
    """

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._table = sql.fqn("dq_app_settings")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_table(self) -> None:
        """No-op kept for backwards compatibility.

        The migration runner now owns table creation for both backends
        (see :mod:`backend.migrations` and
        :mod:`backend.migrations.postgres`); calling code that still
        invokes this method gets a quiet ``DEBUG`` log and we move on.
        """
        logger.debug("AppSettingsService.ensure_table() is a no-op; migrations handle DDL")

    def get_config(self) -> WorkspaceConfig:
        """Load the workspace config from the settings table.

        Deserialization goes through :data:`_WORKSPACE_CONFIG_ADAPTER`
        (a module-level Pydantic ``TypeAdapter[WorkspaceConfig]``) so
        we depend only on Pydantic — already a project dependency for
        the route models — and not on any private blueprint helper.

        Defensive fallback: malformed JSON or a payload that doesn't
        match the ``WorkspaceConfig`` shape returns an empty default
        rather than propagating. This keeps a corrupt settings row from
        bricking the admin UI; the bad row stays visible to operators
        via the WARNING log without short-circuiting the rest of the
        app.
        """
        sql = f"SELECT setting_value FROM {self._table} WHERE setting_key = '{_CONFIG_KEY}'"
        rows = self._sql.query(sql)
        if not rows:
            logger.info("No config found in settings table, returning default")
            return WorkspaceConfig(run_configs=[])

        raw = rows[0][0]
        try:
            data = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("workspace_config row is not valid JSON; returning default")
            return WorkspaceConfig(run_configs=[])

        try:
            return _WORKSPACE_CONFIG_ADAPTER.validate_python(data)
        except ValidationError:
            logger.warning("workspace_config row failed schema validation; returning default", exc_info=True)
            return WorkspaceConfig(run_configs=[])

    def save_config(self, config: WorkspaceConfig, user_email: str | None = None) -> WorkspaceConfig:
        """Save the workspace config to the settings table."""
        config_dict = config.as_dict()
        self.save_setting(_CONFIG_KEY, json.dumps(config_dict), user_email=user_email)
        logger.info("Saved workspace config to settings table")
        return config

    def get_setting(self, key: str) -> str | None:
        """Read a single setting value by key."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        escaped_key = escape_sql_string(key)
        sql = f"SELECT setting_value FROM {self._table} WHERE setting_key = '{escaped_key}'"
        rows = self._sql.query(sql)
        return rows[0][0] if rows else None

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        """Upsert a single setting value, recording who wrote it."""
        self._sql.upsert(
            self._table,
            key_cols={"setting_key": key},
            value_cols={
                "setting_value": value,
                "updated_at": RawSql("current_timestamp()"),
                "updated_by": user_email,
            },
        )
        logger.info("Saved setting: %s (by=%s)", key, user_email or "system")

    # ------------------------------------------------------------------
    # Custom metrics — global SQL-expression list passed to DQMetricsObserver.
    # Stored as a JSON array of strings under ``custom_metrics_v1``. Each
    # entry must be of the form ``<aggregate_expression> as <alias>`` and
    # be safe per ``is_sql_query_safe``.
    # ------------------------------------------------------------------

    def get_custom_metrics(self) -> list[str]:
        """Return the configured global custom-metric SQL expressions, or [] if unset."""
        raw = self.get_setting("custom_metrics_v1")
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("custom_metrics_v1 setting is not valid JSON; ignoring")
            return []
        if not isinstance(parsed, list):
            logger.warning("custom_metrics_v1 setting is not a list; ignoring")
            return []
        return [s for s in parsed if isinstance(s, str) and s.strip()]

    def save_custom_metrics(self, expressions: list[str], *, user_email: str | None = None) -> list[str]:
        """Persist the global custom-metric list. Returns the cleaned list."""
        cleaned = [s.strip() for s in expressions if isinstance(s, str) and s.strip()]
        self.save_setting("custom_metrics_v1", json.dumps(cleaned), user_email=user_email)
        return cleaned

    # ------------------------------------------------------------------
    # Retention — daily DELETE sweep window for analytical tables.
    # Two knobs:
    #   * ``retention_days``            — applied to dq_validation_runs,
    #                                     dq_profiling_results, dq_metrics
    #                                     and the OLTP history tables
    #                                     (default 90).
    #   * ``quarantine_retention_days`` — applied only to
    #                                     dq_quarantine_records, which
    #                                     stores the full source row
    #                                     payload (PII surface). Default
    #                                     30 so row-level data ages out
    #                                     faster than trend tables.
    # Both keys store a plain integer string. The scheduler reads them
    # via ``SchedulerService._resolve_setting_days`` which floors at 7
    # days so a misconfiguration cannot wipe data inside the safety
    # window. Returning ``None`` means the setting is unset and the
    # consumer should fall back to its compiled-in default.
    # ------------------------------------------------------------------

    _RETENTION_KEY = "retention_days"
    _QUARANTINE_RETENTION_KEY = "quarantine_retention_days"

    def get_retention_days(self) -> int | None:
        """Return the configured global retention window, or ``None`` if unset."""
        return self._get_int_setting(self._RETENTION_KEY)

    def get_quarantine_retention_days(self) -> int | None:
        """Return the configured quarantine retention window, or ``None`` if unset."""
        return self._get_int_setting(self._QUARANTINE_RETENTION_KEY)

    def save_retention_days(self, days: int, *, user_email: str | None = None) -> int:
        """Persist the global retention window. Returns the saved value."""
        self.save_setting(self._RETENTION_KEY, str(int(days)), user_email=user_email)
        return int(days)

    def save_quarantine_retention_days(self, days: int, *, user_email: str | None = None) -> int:
        """Persist the quarantine retention window. Returns the saved value."""
        self.save_setting(self._QUARANTINE_RETENTION_KEY, str(int(days)), user_email=user_email)
        return int(days)

    def _get_int_setting(self, key: str) -> int | None:
        raw = self.get_setting(key)
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            logger.warning("Setting %s is not parseable as int (%r); ignoring", key, raw)
            return None
