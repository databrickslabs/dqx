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

    # ------------------------------------------------------------------
    # Embedded dashboard — Insights page renders a Databricks AI/BI
    # dashboard inside an iframe. Admins set the dashboard ID + an
    # optional display title via the Configuration page; the GET
    # endpoint falls back to ``conf.default_dashboard_id`` (env) when
    # this setting is unset, so a bundle can ship a starter dashboard
    # ID without preventing customer overrides.
    # ------------------------------------------------------------------

    _EMBEDDED_DASHBOARD_KEY = "embedded_dashboard_v1"

    def get_embedded_dashboard(self) -> dict | None:
        """Return ``{"dashboard_id": str, "title": str | None}`` or ``None`` if unset."""
        raw = self.get_setting(self._EMBEDDED_DASHBOARD_KEY)
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("embedded_dashboard_v1 setting is not valid JSON; ignoring")
            return None
        if not isinstance(parsed, dict):
            logger.warning("embedded_dashboard_v1 setting is not a dict; ignoring")
            return None
        dashboard_id = parsed.get("dashboard_id")
        if not isinstance(dashboard_id, str) or not dashboard_id.strip():
            return None
        title = parsed.get("title")
        return {
            "dashboard_id": dashboard_id.strip(),
            "title": title.strip() if isinstance(title, str) and title.strip() else None,
        }

    def save_embedded_dashboard(
        self,
        dashboard_id: str,
        title: str | None = None,
        *,
        user_email: str | None = None,
    ) -> dict:
        """Persist the embedded dashboard ID + optional title. Returns the saved payload."""
        cleaned_id = (dashboard_id or "").strip()
        cleaned_title = (title or "").strip() or None
        payload = {"dashboard_id": cleaned_id, "title": cleaned_title}
        self.save_setting(self._EMBEDDED_DASHBOARD_KEY, json.dumps(payload), user_email=user_email)
        return payload

    def delete_embedded_dashboard(self, *, user_email: str | None = None) -> None:
        """Clear the embedded dashboard setting so the env default takes over again."""
        self.save_setting(self._EMBEDDED_DASHBOARD_KEY, "", user_email=user_email)

    # ------------------------------------------------------------------
    # Run review statuses — admin-managed list of labels surfaced on the
    # Runs detail page (next to comments) and as a Runs History filter.
    # Stored as a JSON array under ``run_review_statuses_v1``. One entry
    # MUST be flagged ``is_default``; that value is what
    # ``ReviewStatusService`` returns virtually for runs that have never
    # been explicitly reviewed.
    #
    # We seed a sensible default list on first read (rather than at
    # migration time) so the feature works out-of-the-box even on
    # already-deployed workspaces, and so the seed list can evolve in
    # code without needing a fresh migration.
    # ------------------------------------------------------------------

    _RUN_REVIEW_STATUSES_KEY = "run_review_statuses_v1"

    # Default catalogue shipped on first read. The colors are token
    # names the UI maps to its design-system palette so we can rebrand
    # without touching backend data.
    _RUN_REVIEW_STATUSES_SEED: list[dict] = [
        {
            "value": "Pending review",
            "description": "Awaiting business review",
            "color": "gray",
            "is_default": True,
        },
        {
            "value": "Acknowledged",
            "description": "Known issue, accepted by owners",
            "color": "amber",
            "is_default": False,
        },
        {
            "value": "Resolved",
            "description": "Fixed upstream",
            "color": "green",
            "is_default": False,
        },
        {
            "value": "False positive",
            "description": "Rule is wrong, not a real issue",
            "color": "blue",
            "is_default": False,
        },
    ]

    def get_run_review_statuses(self) -> list[dict]:
        """Return the admin-managed catalogue of run review status values.

        Falls back to (and persists) the seed list on first read so the
        Runs detail dropdown is never empty after a fresh deploy.
        Returned entries are always normalised — ``value`` trimmed,
        ``description`` defaulted to empty string, ``color`` defaulted
        to ``"gray"``, ``is_default`` coerced to bool — so call sites
        can index by field without defensive lookups.
        """
        raw = self.get_setting(self._RUN_REVIEW_STATUSES_KEY)
        if not raw:
            logger.info("Seeding default run_review_statuses on first read")
            self._persist_run_review_statuses(self._RUN_REVIEW_STATUSES_SEED, user_email=None)
            return [self._normalise_status_entry(e) for e in self._RUN_REVIEW_STATUSES_SEED]

        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("run_review_statuses_v1 is not valid JSON; falling back to seed")
            return [self._normalise_status_entry(e) for e in self._RUN_REVIEW_STATUSES_SEED]
        if not isinstance(parsed, list):
            logger.warning("run_review_statuses_v1 is not a list; falling back to seed")
            return [self._normalise_status_entry(e) for e in self._RUN_REVIEW_STATUSES_SEED]

        out: list[dict] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            normalised = self._normalise_status_entry(item)
            if normalised["value"]:
                out.append(normalised)
        # Defensive: if the persisted list is malformed and we end up
        # with nothing, surface the seed rather than an empty dropdown.
        return out or [self._normalise_status_entry(e) for e in self._RUN_REVIEW_STATUSES_SEED]

    def save_run_review_statuses(
        self,
        statuses: list[dict],
        *,
        user_email: str | None = None,
    ) -> list[dict]:
        """Replace the admin-managed catalogue, enforcing exactly one default.

        Validation rules (raise ``ValueError`` on violation so the route
        can turn them into a 400):
        - At least one entry is required (callers always need a default
          to surface for unreviewed runs).
        - ``value`` must be non-empty, trimmed, and unique within the list.
        - Exactly one entry must have ``is_default=True``.
        """
        cleaned: list[dict] = []
        seen: set[str] = set()
        for item in statuses or []:
            if not isinstance(item, dict):
                continue
            normalised = self._normalise_status_entry(item)
            value = normalised["value"]
            if not value:
                raise ValueError("Run review status 'value' must be non-empty.")
            if value in seen:
                raise ValueError(f"Duplicate run review status value: {value!r}.")
            seen.add(value)
            cleaned.append(normalised)

        if not cleaned:
            raise ValueError("At least one run review status is required.")

        defaults = [e for e in cleaned if e["is_default"]]
        if len(defaults) != 1:
            raise ValueError(f"Exactly one run review status must be marked as default; got {len(defaults)}.")

        self._persist_run_review_statuses(cleaned, user_email=user_email)
        return cleaned

    def get_default_run_review_status(self) -> str:
        """Return the ``value`` of the catalogue entry flagged ``is_default``.

        Called by ``ReviewStatusService`` to surface an effective status
        for runs that have no explicit row. Guaranteed non-empty because
        :meth:`save_run_review_statuses` enforces the invariant.
        """
        for entry in self.get_run_review_statuses():
            if entry["is_default"]:
                return entry["value"]
        # Should be unreachable thanks to the save-side invariant, but
        # we fall back to the seed default rather than raise so a buggy
        # write can't take down the whole listings endpoint.
        return self._RUN_REVIEW_STATUSES_SEED[0]["value"]

    def _persist_run_review_statuses(
        self,
        entries: list[dict],
        *,
        user_email: str | None,
    ) -> None:
        self.save_setting(
            self._RUN_REVIEW_STATUSES_KEY,
            json.dumps([self._normalise_status_entry(e) for e in entries]),
            user_email=user_email,
        )
        logger.info("Saved %d run review status(es) (by=%s)", len(entries), user_email or "system")

    @staticmethod
    def _normalise_status_entry(item: dict) -> dict:
        value = (item.get("value") or "").strip() if isinstance(item.get("value"), str) else ""
        description = item.get("description") or ""
        if not isinstance(description, str):
            description = ""
        color = item.get("color") or "gray"
        if not isinstance(color, str) or not color.strip():
            color = "gray"
        return {
            "value": value,
            "description": description.strip(),
            "color": color.strip(),
            "is_default": bool(item.get("is_default")),
        }
