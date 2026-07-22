import json
import logging
import re

from databricks.labs.dqx.config import WorkspaceConfig
from pydantic import TypeAdapter, ValidationError

from databricks_labs_dqx_app.backend.common.approvals import ApprovalMode, normalize_approvals_mode
from databricks_labs_dqx_app.backend.config import conf
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql

logger = logging.getLogger(__name__)

_CONFIG_KEY = "workspace_config"

# Compiled-in fallback for the ``draft_run_sample_limit`` setting — the
# row cap applied to DRAFT monitored-table runs when the admin has not
# configured one. 0 means unlimited. Shared by ``BindingRunService`` and
# the ``/config/draft-run-sample-limit`` admin endpoints.
DRAFT_RUN_SAMPLE_LIMIT_DEFAULT = 1000

# Compiled-in fallback for the ``default_pass_threshold`` setting — the
# org-wide minimum pass rate (%) below which a check warns. Shared by
# the breach evaluator (results service) and the admin settings endpoint.
DEFAULT_PASS_THRESHOLD_DEFAULT = 70

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

    # ------------------------------------------------------------------
    # Draft-run sampling — bounds the rows a DRAFT monitored-table run
    # reads (``BindingRunService.run_binding`` with ``source='draft'``).
    # Approved/published runs NEVER sample — they always scan the whole
    # table; this knob exists only so exploratory draft runs on large
    # tables stay cheap. Stored as a plain integer string:
    #   * unset / invalid → consumer falls back to
    #     ``DRAFT_RUN_SAMPLE_LIMIT_DEFAULT`` (1000)
    #   * 0               → unlimited (draft runs scan the whole table)
    #   * positive N      → draft runs read at most N rows
    # ------------------------------------------------------------------

    _DRAFT_RUN_SAMPLE_LIMIT_KEY = "draft_run_sample_limit"

    def get_draft_run_sample_limit(self) -> int | None:
        """Return the configured draft-run sample limit, or ``None`` if unset.

        0 means unlimited (whole table). Negative stored values are
        treated as unset so a corrupt row can never disable sampling by
        accident.
        """
        value = self._get_int_setting(self._DRAFT_RUN_SAMPLE_LIMIT_KEY)
        if value is not None and value < 0:
            logger.warning("Setting %s is negative (%d); treating as unset", self._DRAFT_RUN_SAMPLE_LIMIT_KEY, value)
            return None
        return value

    def save_draft_run_sample_limit(self, limit: int, *, user_email: str | None = None) -> int:
        """Persist the draft-run sample limit (0 = unlimited). Returns the saved value."""
        self.save_setting(self._DRAFT_RUN_SAMPLE_LIMIT_KEY, str(int(limit)), user_email=user_email)
        return int(limit)

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
    # Rules Registry — auto-upgrade-without-approval (design spec §5).
    #
    # Governs re-materialization behaviour when a FOLLOWING (i.e.
    # ``pinned_version IS NULL``) applied rule's registry rule is
    # republished and the newly rendered check differs from what's
    # currently stored:
    #   * ``False`` (default = "Behaviour B"): the materialized row is
    #     pushed back to ``pending_approval`` for per-table re-review.
    #   * ``True`` ("Behaviour A"): the materialized row silently
    #     re-approves — central registry approval is treated as
    #     sufficient. Pinned applications are never affected either way.
    # ------------------------------------------------------------------

    _AUTO_UPGRADE_WITHOUT_APPROVAL_KEY = "auto_upgrade_without_approval"

    def get_auto_upgrade_without_approval(self) -> bool:
        """Return the configured auto-upgrade behaviour; defaults to ``True`` (Behaviour A) when unset."""
        raw = self.get_setting(self._AUTO_UPGRADE_WITHOUT_APPROVAL_KEY)
        return raw is None or raw.strip().lower() == "true"

    def save_auto_upgrade_without_approval(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the auto-upgrade-without-approval setting. Returns the saved value."""
        self.save_setting(
            self._AUTO_UPGRADE_WITHOUT_APPROVAL_KEY, "true" if enabled else "false", user_email=user_email
        )
        return enabled

    # ------------------------------------------------------------------
    # Approvals mode — app-wide submit→approve gate (issue #94). A 3-value
    # enum string (see :class:`~backend.common.approvals.ApprovalMode`):
    #   * ``enabled`` (default) — authors submit, approvers/admins approve.
    #   * ``auto_bypass`` — gate stays on, but a submit auto-approves when the
    #     acting user could approve it themselves (admin, or approve_rules +
    #     edit rights on the object). Everyone else still lands in
    #     ``pending_approval``.
    #   * ``disabled`` — no approval step; every submit auto-approves.
    # An unset/corrupt row reads back as ``enabled`` so the gate can never be
    # silently disabled by a bad value (see ``normalize_approvals_mode``).
    # ------------------------------------------------------------------

    _APPROVALS_MODE_KEY = "approvals_mode"

    def get_approvals_mode(self) -> str:
        """Return the configured approvals mode; defaults to ``enabled`` when unset."""
        return normalize_approvals_mode(self.get_setting(self._APPROVALS_MODE_KEY))

    def save_approvals_mode(self, mode: str, *, user_email: str | None = None) -> str:
        """Persist the approvals mode. Returns the normalised (validated) value.

        Raises:
            ValueError: *mode* is not one of the accepted values (mapped to a
                400 by the route).
        """
        candidate = (mode or "").strip().lower()
        if candidate not in ApprovalMode.ALL:
            raise ValueError(f"Invalid approvals mode: {mode!r}. Must be one of {sorted(ApprovalMode.ALL)}.")
        self.save_setting(self._APPROVALS_MODE_KEY, candidate, user_email=user_email)
        return candidate

    # ------------------------------------------------------------------
    # Object permissions — default per-grant inheritance (P22-D item 10).
    #
    # Governs the DEFAULT state of the per-grant "inherit to child objects"
    # toggle in the Permissions tab ("Cascade permissions by default"). Defaults
    # to ``True`` (new grants cascade down the hierarchy — granting on a table
    # space or monitored table also grants SELECT on underlying tables/rules),
    # which is the common intent. An admin can flip this to ``False`` so each new
    # grant is scoped to just its object unless the granter opts in.
    # ------------------------------------------------------------------

    _PERMISSIONS_DEFAULT_INHERIT_KEY = "permissions_default_inherit"

    def get_permissions_default_inherit(self) -> bool:
        """Return the admin default for the per-grant inheritance toggle (default ``True``).

        When the setting has never been persisted (``raw is None``) we default to
        ``True`` — cascading is the expected behaviour for most deployments.
        """
        raw = self.get_setting(self._PERMISSIONS_DEFAULT_INHERIT_KEY)
        if raw is None:
            return True
        return raw.strip().lower() == "true"

    def save_permissions_default_inherit(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the default per-grant inheritance setting. Returns the saved value."""
        self.save_setting(self._PERMISSIONS_DEFAULT_INHERIT_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    # ------------------------------------------------------------------
    # Rules Registry — default-auto-upgrade (P21-G). Distinct from
    # ``auto_upgrade_without_approval`` above:
    #   * ``auto_upgrade_without_approval`` governs RE-APPROVAL — whether a
    #     re-rendered check on an EXISTING following (``pinned_version IS
    #     NULL``) application silently re-approves or falls back to
    #     ``pending_approval``.
    #   * ``default_auto_upgrade`` (this setting) governs the PIN CHOSEN AT
    #     ATTACH TIME for a brand-new rule application / data-product
    #     member, when the caller does not explicitly request a pin:
    #       - ``True`` (default): the new attachment follows latest
    #         (``pinned_version = None``), matching today's behaviour.
    #       - ``False``: the new attachment is pinned to the rule's (or
    #         binding's) CURRENT version at attach time, so it only moves
    #         forward when a steward explicitly re-pins/unpins it.
    #     This mirrors dqlake's ``default_auto_upgrade`` app-setting
    #     (``backend/routers/bindings.py:_resolve_pinned_version``).
    #     It is applied ONLY when a NEW row is inserted — never on an
    #     update of an existing application/member, where an explicit
    #     ``pinned_version=None`` from the caller already means "the
    #     steward explicitly chose to follow latest / clear the pin" and
    #     must be honoured as-is. See
    #     :meth:`resolve_pinned_version_for_new_attachment` and its call
    #     sites in ``ApplyRulesService.apply_rule`` / ``DataProductService.add_member``.
    # ------------------------------------------------------------------

    _DEFAULT_AUTO_UPGRADE_KEY = "default_auto_upgrade"

    def get_default_auto_upgrade(self) -> bool:
        """Return whether new attachments default to following latest; defaults to ``True`` when unset."""
        raw = self.get_setting(self._DEFAULT_AUTO_UPGRADE_KEY)
        if raw is None:
            return True
        return raw.strip().lower() == "true"

    def save_default_auto_upgrade(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the default-auto-upgrade setting. Returns the saved value."""
        self.save_setting(self._DEFAULT_AUTO_UPGRADE_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    def resolve_pinned_version_for_new_attachment(
        self, explicit_pinned_version: int | None, current_version: int
    ) -> int | None:
        """Resolve the pin to store for a BRAND-NEW rule application / data-product member.

        Call this ONLY from the insert path of a new attachment — never
        when updating an existing row (see the module-level comment on
        ``default_auto_upgrade`` above for why this is attach-time-only).

        Args:
            explicit_pinned_version: The pin the caller explicitly requested,
                or ``None`` if the caller left it unspecified (the common
                case — most attach flows have no pin control at all).
            current_version: The rule's (or binding's) current published/
                approved version, used as the pin when ``default_auto_upgrade``
                is off.

        Returns:
            ``explicit_pinned_version`` unchanged if the caller specified one;
            otherwise ``None`` (follow latest) when ``default_auto_upgrade`` is
            on, or ``current_version`` when it's off.
        """
        if explicit_pinned_version is not None:
            return explicit_pinned_version
        if self.get_default_auto_upgrade():
            return None
        return current_version

    # ------------------------------------------------------------------
    # Global Results tab (issue B2-20) — the app-wide, all-tables Results
    # surface (``routes/_sidebar/results.tsx`` + its sidebar entry). OFF by
    # default: it duplicates per-object results and confuses fresh deploys,
    # so an admin must explicitly opt in. When off, the global Results nav
    # item AND the homepage overall-score "?" explainer are hidden (the "?"
    # only explains a global-results-vs-home divergence that's moot with no
    # global results screen). Per-object MT/TS/RR results tabs are
    # unaffected — this gates only the GLOBAL surface. Only an explicit
    # ``"true"`` reads as on; an unset or any other value reads as off so a
    # fresh deploy or a corrupt row keeps the surface hidden.
    # ------------------------------------------------------------------

    _GLOBAL_RESULTS_ENABLED_KEY = "global_results_enabled"

    def get_global_results_enabled(self) -> bool:
        """Return whether the global Results tab is enabled; defaults to ``False`` (off) when unset."""
        raw = self.get_setting(self._GLOBAL_RESULTS_ENABLED_KEY)
        return raw is not None and raw.strip().lower() == "true"

    def save_global_results_enabled(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the global-Results-tab setting. Returns the saved value."""
        self.save_setting(self._GLOBAL_RESULTS_ENABLED_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    # ------------------------------------------------------------------
    # Rules Results tab (item 35) — whether the per-rule "Results" tab is
    # surfaced inside the Rules Registry rule dialog. Distinct from
    # ``global_results_enabled`` above (that gates the app-wide, all-tables
    # Results SURFACE + its sidebar entry); this gates only the Results TAB on
    # an individual rule. OFF by default — a fresh deploy hides the rule
    # Results tab until an admin explicitly opts in. Only an explicit
    # ``"true"`` reads as on; an unset or any other value reads as off.
    # ------------------------------------------------------------------

    _RULES_RESULTS_TAB_ENABLED_KEY = "rules_results_tab_enabled"

    def get_rules_results_tab_enabled(self) -> bool:
        """Return whether the per-rule Results tab is enabled; defaults to ``False`` (off) when unset."""
        raw = self.get_setting(self._RULES_RESULTS_TAB_ENABLED_KEY)
        return raw is not None and raw.strip().lower() == "true"

    def save_rules_results_tab_enabled(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the rules-Results-tab setting. Returns the saved value."""
        self.save_setting(self._RULES_RESULTS_TAB_ENABLED_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    # ------------------------------------------------------------------
    # Require-draft-run-before-submit (issue B2-12) — a governance gate that,
    # when ON, refuses to SUBMIT a monitored table / table space (or a
    # per-table applied rule) for review — and equally refuses the
    # auto-approve shortcut that the approvals-mode setting would otherwise
    # take — until a draft run has been recorded for the target table(s). This
    # forces authors to dry-run-test their checks before they enter review.
    #
    # Defaults to ``False`` (OFF) so existing deploys keep today's behaviour:
    # a submit never requires a prior run. Only an explicit ``"true"`` reads as
    # on; an unset or any other value reads as off. Registry rules are
    # table-agnostic (no single table to validate) and cross-table SQL checks
    # have no home table, so the gate does not apply to those submits — see
    # ``DraftRunGateService`` and the route call sites for the exact scoping.
    # ------------------------------------------------------------------

    _REQUIRE_DRAFT_RUN_BEFORE_SUBMIT_KEY = "require_draft_run_before_submit"

    def get_require_draft_run_before_submit(self) -> bool:
        """Return whether a draft run is required before submit; defaults to ``False`` (off) when unset."""
        raw = self.get_setting(self._REQUIRE_DRAFT_RUN_BEFORE_SUBMIT_KEY)
        return raw is not None and raw.strip().lower() == "true"

    def save_require_draft_run_before_submit(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the require-draft-run-before-submit setting. Returns the saved value."""
        self.save_setting(
            self._REQUIRE_DRAFT_RUN_BEFORE_SUBMIT_KEY, "true" if enabled else "false", user_email=user_email
        )
        return enabled

    # ------------------------------------------------------------------
    # Run review statuses — admin-managed list of labels surfaced on the
    # Runs detail page (next to comments) and as a Runs History filter.
    # Stored as a JSON array under ``run_review_statuses_v1``. One entry
    # MUST be flagged ``is_default``; that value is what
    # ``ReviewStatusService`` returns virtually for runs that have never
    # been explicitly reviewed.
    #
    # The seed catalogue below is persisted once at startup via
    # :meth:`seed_run_review_statuses_if_absent` (called from the app
    # lifespan after migrations). The read path is side-effect free: if
    # the setting is somehow still unset it returns the seed *virtually*
    # without writing, so a GET (e.g. the Runs listing) never turns into
    # a write and read-only health probes don't mutate state.
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
            "value": "False positive",
            "description": "Rule is wrong, not a real issue",
            "color": "amber",
            "is_default": False,
        },
        {
            "value": "Confirmed",
            "description": "Known issue, accepted by owners",
            "color": "red",
            "is_default": False,
        },
        {
            "value": "Resolved",
            "description": "Fixed upstream",
            "color": "green",
            "is_default": False,
        },
    ]

    def get_run_review_statuses(self) -> list[dict]:
        """Return the admin-managed catalogue of run review status values.

        Pure read — never writes. When the setting is unset (or
        malformed) this returns the normalised seed list *virtually*
        rather than persisting it; the seed is written once at startup by
        :meth:`seed_run_review_statuses_if_absent`. Keeping this read-only
        means the Runs listing GET (which calls this) and read-only health
        probes don't trigger a ``dq_app_settings`` write.

        Returned entries are always normalised — ``value`` trimmed,
        ``description`` defaulted to empty string, ``color`` defaulted
        to ``"gray"``, ``is_default`` coerced to bool — so call sites
        can index by field without defensive lookups.
        """
        raw = self.get_setting(self._RUN_REVIEW_STATUSES_KEY)
        if not raw:
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

    def seed_run_review_statuses_if_absent(self, *, user_email: str | None = None) -> bool:
        """Persist the seed catalogue iff no value exists yet.

        Called once at startup (after migrations) so the read path stays
        side-effect free. Idempotent: returns ``False`` (no write) when a
        value is already present, ``True`` when it seeded. ``save_setting``
        is an upsert keyed by ``setting_key``, so the rare case of two
        first-start workers racing converges instead of conflicting.
        """
        if self.get_setting(self._RUN_REVIEW_STATUSES_KEY):
            return False
        logger.info("Seeding default run_review_statuses at startup")
        self._persist_run_review_statuses(self._RUN_REVIEW_STATUSES_SEED, user_email=user_email)
        return True

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

    # ------------------------------------------------------------------
    # Reserved label definitions — Rules Registry Phase 1. Dimensions &
    # severity are TAGS, not new tables: they are pre-built entries in the
    # same ``label_definitions`` JSON blob (see ``routes.v1.config``),
    # flagged ``is_builtin`` so the save endpoint can refuse to delete or
    # rename them. Seeded once at startup (mirrors
    # ``seed_run_review_statuses_if_absent`` above); the read path
    # (``routes.v1.config.get_label_definitions``) stays side-effect free.
    #
    # Kept as raw dicts (not the ``LabelDefinition`` pydantic model) to
    # avoid a services -> routes import — ``routes.v1.config`` already
    # imports ``AppSettingsService``, so importing back would cycle.
    # ------------------------------------------------------------------

    _LABEL_DEFINITIONS_KEY = "label_definitions"

    _RESERVED_LABEL_DEFINITION_SEEDS: list[dict] = [
        {
            "key": "dimension",
            "description": "Data quality dimension the rule measures.",
            "values": ["Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"],
            # Fixed, admin-curated catalog — rule authors pick from this list,
            # they don't extend it inline. Enforced server-side regardless of
            # this seed value; see ``_NO_CUSTOM_VALUE_BUILTIN_KEYS`` in
            # ``routes.v1.config``.
            "allow_custom_values": False,
            "is_builtin": True,
            "value_colors": {
                "Validity": "#2563EB",
                "Completeness": "#16A34A",
                "Accuracy": "#D97706",
                "Consistency": "#7C3AED",
                "Uniqueness": "#0891B2",
                "Timeliness": "#DB2777",
            },
            # One-line explanations lifted from the DQ dimension glossary so
            # authors get the same definitions wherever the value is shown
            # (admin editor, label picker tooltip).
            "value_descriptions": {
                "Validity": "Whether values match the expected format or rules.",
                "Completeness": "Whether all required values are present (no missing data).",
                "Accuracy": "Whether values reflect the real-world truth they represent.",
                "Consistency": "Whether values agree across systems, tables, or time.",
                "Uniqueness": "Whether records that should be unique actually are.",
                "Timeliness": "Whether data is available within the expected time window.",
            },
        },
        {
            "key": "severity",
            "description": "Rule severity, independent of DQX criticality (warn/error).",
            "values": ["Low", "Medium", "High", "Critical"],
            "allow_custom_values": False,
            "is_builtin": True,
            "value_colors": {
                "Low": "#6B7280",
                "Medium": "#D97706",
                "High": "#EA580C",
                "Critical": "#DC2626",
            },
            # Admin-editable severity -> DQX criticality mapping consumed by
            # ``registry_models.resolve_criticality`` (materializer). Matches
            # the historical hardcoded defaults
            # (``registry_models.SEVERITY_TO_CRITICALITY``).
            "value_criticality": {
                "Low": "warn",
                "Medium": "warn",
                "High": "error",
                "Critical": "error",
            },
        },
    ]

    def get_label_definitions(self) -> list[dict]:
        """Return the stored ``label_definitions`` list as raw dicts.

        Defensive read shared by the seeding path below and
        ``registry_models.resolve_criticality``: malformed JSON, a
        non-list payload, or non-dict entries degrade to an empty /
        filtered list with a WARNING rather than propagating. Kept as
        raw dicts (not the ``LabelDefinition`` pydantic model) to avoid
        a services -> routes import cycle — see the class-level note.
        """
        raw = self.get_setting(self._LABEL_DEFINITIONS_KEY)
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("label_definitions setting is not valid JSON; treating as empty")
            return []
        if not isinstance(parsed, list):
            logger.warning("label_definitions setting is not a list; treating as empty")
            return []
        return [item for item in parsed if isinstance(item, dict)]

    def seed_reserved_label_definitions_if_absent(self, *, user_email: str | None = None) -> bool:
        """Ensure the reserved ``dimension``/``severity`` label keys exist.

        Idempotent and non-destructive: reads the current
        ``label_definitions`` list, adds only the reserved seed entries
        whose ``key`` is not already present, and leaves every existing
        entry (admin-edited or not) untouched. Returns ``True`` iff a
        write happened.
        """
        existing = self.get_label_definitions()
        existing_keys = {item.get("key") for item in existing}
        missing = [seed for seed in self._RESERVED_LABEL_DEFINITION_SEEDS if seed["key"] not in existing_keys]
        if not missing:
            return False

        updated = existing + [json.loads(json.dumps(seed)) for seed in missing]
        self.save_setting(self._LABEL_DEFINITIONS_KEY, json.dumps(updated), user_email=user_email)
        logger.info("Seeded reserved label definition(s): %s", [s["key"] for s in missing])
        return True

    # ------------------------------------------------------------------
    # AI Gateway settings — Rules Registry Phase 4A. Kill-switch, serving
    # endpoint name, and per-user hourly rate limit for AIGateway
    # (services/ai_gateway.py). AI is ON by default (per explicit product
    # request) so the AI-assisted authoring surfaces work out of the box on
    # a fresh deploy. The admin kill-switch remains: setting ``ai_enabled``
    # to ``false`` turns every AI affordance off app-wide. Note the cost
    # implication — AI serving-endpoint calls run On-Behalf-Of the caller
    # (see ``get_ai_gateway``), so an enabled default means those calls can
    # be incurred without an explicit opt-in.
    # ------------------------------------------------------------------

    _AI_ENABLED_KEY = "ai_enabled"
    _AI_ENDPOINT_NAME_KEY = "ai_endpoint_name"
    _AI_RATE_LIMIT_KEY = "ai_rate_limit_per_user_per_hour"

    AI_RATE_LIMIT_DEFAULT = 30

    # Seeded default AI serving endpoint — a reasonable out-of-the-box
    # selection for the admin dropdown (Rules Registry Phase 7F). AI is ON by
    # default (see :meth:`get_ai_enabled`) and this endpoint is what shows up
    # pre-selected. An admin who explicitly saves an empty value gets that
    # empty value back (see the ``raw is None`` check below) rather than being
    # forced back to the default on every read.
    AI_ENDPOINT_NAME_DEFAULT = "databricks-gpt-5-4-nano"

    def get_ai_enabled(self) -> bool:
        """Return whether the AI kill-switch is on; defaults to ``True`` (on) when unset.

        AI features are enabled by default so a fresh deploy is usable without
        an explicit opt-in. An admin can still turn everything off by saving
        ``ai_enabled = false`` (the kill-switch). Only an unset value (no row)
        or an explicit ``"true"`` reads as on; any other stored value — including
        ``"false"`` — is off.
        """
        raw = self.get_setting(self._AI_ENABLED_KEY)
        if raw is None:
            return True
        return raw.strip().lower() == "true"

    def save_ai_enabled(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the AI kill-switch setting. Returns the saved value."""
        self.save_setting(self._AI_ENABLED_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    def get_ai_endpoint_name(self) -> str:
        """Return the configured AI serving endpoint name.

        Defaults to :data:`AI_ENDPOINT_NAME_DEFAULT` when the setting has
        never been saved (no row). An admin who explicitly saves an empty
        value gets ``""`` back, not the default — the row exists, it's just
        empty.
        """
        raw = self.get_setting(self._AI_ENDPOINT_NAME_KEY)
        if raw is None:
            return self.AI_ENDPOINT_NAME_DEFAULT
        return raw.strip()

    def save_ai_endpoint_name(self, endpoint_name: str, *, user_email: str | None = None) -> str:
        """Persist the AI serving endpoint name. Returns the cleaned (trimmed) value."""
        cleaned = (endpoint_name or "").strip()
        self.save_setting(self._AI_ENDPOINT_NAME_KEY, cleaned, user_email=user_email)
        return cleaned

    def get_ai_rate_limit_per_user_per_hour(self) -> int:
        """Return the configured per-user hourly AI call cap; defaults to :data:`AI_RATE_LIMIT_DEFAULT`."""
        value = self._get_int_setting(self._AI_RATE_LIMIT_KEY)
        return value if value is not None else self.AI_RATE_LIMIT_DEFAULT

    def save_ai_rate_limit_per_user_per_hour(self, limit: int, *, user_email: str | None = None) -> int:
        """Persist the per-user hourly AI call cap. Returns the saved value."""
        self.save_setting(self._AI_RATE_LIMIT_KEY, str(int(limit)), user_email=user_email)
        return int(limit)

    # ------------------------------------------------------------------
    # Pass-threshold setting — org-wide default minimum pass rate (%).
    # Resolution order: per-column override → per-rule override →
    # registry-rule default → this admin default (compiled fallback 70).
    # ------------------------------------------------------------------

    _DEFAULT_PASS_THRESHOLD_KEY = "default_pass_threshold"

    def get_default_pass_threshold(self) -> int:
        """Org-wide default minimum pass rate (%) below which a check warns.

        Returns the compiled default (70) when unset or unparseable. Clamped to
        [0, 100] defensively so a hand-edited row can never escape the range.
        """
        value = self._get_int_setting(self._DEFAULT_PASS_THRESHOLD_KEY)
        if value is None:
            return DEFAULT_PASS_THRESHOLD_DEFAULT
        return max(0, min(100, value))

    def save_default_pass_threshold(self, value: int, *, user_email: str | None = None) -> int:
        """Persist the org-wide default pass threshold. Returns the clamped value."""
        clamped = max(0, min(100, int(value)))
        self.save_setting(self._DEFAULT_PASS_THRESHOLD_KEY, str(clamped), user_email=user_email)
        return clamped

    # ------------------------------------------------------------------
    # Pass-threshold feature toggle — master switch. When False, the UI
    # hides all threshold controls and the materializer emits no threshold
    # metadata; breach evaluation is also disabled server-side.
    # ------------------------------------------------------------------

    _PASS_THRESHOLD_ENABLED_KEY = "pass_threshold_enabled"

    def get_pass_threshold_enabled(self) -> bool:
        """Master switch for the pass-threshold feature (default ON).

        Returns *True* when the setting has never been persisted (``raw is
        None``) so the feature is enabled by default across all deployments.
        """
        raw = self.get_setting(self._PASS_THRESHOLD_ENABLED_KEY)
        return raw is None or raw.strip().lower() == "true"

    def save_pass_threshold_enabled(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the pass-threshold feature toggle. Returns the saved value."""
        self.save_setting(self._PASS_THRESHOLD_ENABLED_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    # ------------------------------------------------------------------
    # Vector Search / embeddings settings — Rules Registry Phase 4B/4C,
    # auto-derived since Phase 8B. The admin UI only exposes the AI
    # enable toggle + serving-endpoint dropdown; the rule-mapping
    # suggester's vector store is fully auto-provisioned from that alone
    # — these three settings are no longer surfaced as admin inputs.
    #
    #   * ``embedding_endpoint_name`` — Databricks serving endpoint that
    #     turns rule/query text into an embedding vector
    #     (``services/rule_embeddings.py``). Defaults to
    #     :data:`EMBEDDING_ENDPOINT_NAME_DEFAULT`, a Foundation Model API
    #     embedding endpoint available out-of-the-box in most workspaces.
    #   * ``vs_endpoint_name`` / ``vs_index_name`` — the Databricks Vector
    #     Search endpoint + index that stores rule embeddings for
    #     nearest-neighbour retrieval. Auto-derived from this app's own
    #     catalog/schema (see :meth:`_default_vs_endpoint_name` /
    #     :meth:`_default_vs_index_name`) so multiple ``dqx_studio``
    #     deployments on the same metastore never collide.
    #
    # The setter methods are kept (and the settings remain independently
    # overridable via direct API calls) purely for backwards
    # compatibility/testing — nothing in the UI writes to them anymore.
    # ``VectorStoreProvisioner.ensure_vector_store`` / the suggester
    # degrade gracefully (best-effort, never raise) if the auto-created
    # infra isn't ready yet — see ``services/vector_store.py``.
    # ------------------------------------------------------------------

    _EMBEDDING_ENDPOINT_NAME_KEY = "embedding_endpoint_name"
    _VS_ENDPOINT_NAME_KEY = "vs_endpoint_name"
    _VS_INDEX_NAME_KEY = "vs_index_name"

    # A widely-available Foundation Model API embedding endpoint — a
    # reasonable out-of-the-box default so the rule-mapping suggester
    # works the moment an admin flips "Enable AI" on, without a separate
    # embedding-endpoint field to fill in.
    EMBEDDING_ENDPOINT_NAME_DEFAULT = "databricks-gte-large-en"

    # Fixed prefix/suffix for the auto-derived Vector Search endpoint and
    # index names (see ``_default_vs_endpoint_name``/``_default_vs_index_name``).
    _VS_ENDPOINT_NAME_PREFIX = "dqx_studio_rule_suggester"
    _VS_INDEX_NAME_SUFFIX = "dq_rule_embeddings_index"

    def get_embedding_endpoint_name(self) -> str:
        """Return the embedding serving endpoint name.

        Defaults to :data:`EMBEDDING_ENDPOINT_NAME_DEFAULT` when the
        setting is unset — either no row at all, or a row holding an
        empty/whitespace-only value. Vector Search cannot provision
        without a real embedding endpoint, so an empty stored value is
        treated as "unset" and falls back to the default rather than
        silently disabling provisioning.
        """
        raw = self.get_setting(self._EMBEDDING_ENDPOINT_NAME_KEY)
        if raw is None or not raw.strip():
            return self.EMBEDDING_ENDPOINT_NAME_DEFAULT
        return raw.strip()

    def save_embedding_endpoint_name(self, endpoint_name: str, *, user_email: str | None = None) -> str:
        """Persist the embedding serving endpoint name. Returns the cleaned (trimmed) value."""
        cleaned = (endpoint_name or "").strip()
        self.save_setting(self._EMBEDDING_ENDPOINT_NAME_KEY, cleaned, user_email=user_email)
        return cleaned

    def _default_vs_endpoint_name(self) -> str:
        """Auto-derive a Vector Search endpoint name scoped to this app's catalog."""
        safe_catalog = re.sub(r"[^A-Za-z0-9_]", "_", conf.catalog)
        return f"{self._VS_ENDPOINT_NAME_PREFIX}_{safe_catalog}"

    def get_vs_endpoint_name(self) -> str:
        """Return the Vector Search endpoint name.

        Defaults to an auto-derived, catalog-scoped name (see
        :meth:`_default_vs_endpoint_name`) when the setting is unset —
        either no row at all, or a row holding an empty/whitespace-only
        value (an empty stored value would otherwise silently disable
        Vector Search provisioning).
        """
        raw = self.get_setting(self._VS_ENDPOINT_NAME_KEY)
        if raw is None or not raw.strip():
            return self._default_vs_endpoint_name()
        return raw.strip()

    def save_vs_endpoint_name(self, endpoint_name: str, *, user_email: str | None = None) -> str:
        """Persist the Vector Search endpoint name. Returns the cleaned (trimmed) value."""
        cleaned = (endpoint_name or "").strip()
        self.save_setting(self._VS_ENDPOINT_NAME_KEY, cleaned, user_email=user_email)
        return cleaned

    def _default_vs_index_name(self) -> str:
        """Auto-derive a fully-qualified (UC three-level) Vector Search index name."""
        return f"{conf.catalog}.{conf.schema_name}.{self._VS_INDEX_NAME_SUFFIX}"

    def get_vs_index_name(self) -> str:
        """Return the fully-qualified Vector Search index name.

        Defaults to an auto-derived name under this app's own UC
        catalog/schema (see :meth:`_default_vs_index_name`) when the
        setting is unset — either no row at all, or a row holding an
        empty/whitespace-only value (an empty stored value would
        otherwise silently disable Vector Search provisioning).
        """
        raw = self.get_setting(self._VS_INDEX_NAME_KEY)
        if raw is None or not raw.strip():
            return self._default_vs_index_name()
        return raw.strip()

    def save_vs_index_name(self, index_name: str, *, user_email: str | None = None) -> str:
        """Persist the Vector Search index name. Returns the cleaned (trimmed) value."""
        cleaned = (index_name or "").strip()
        self.save_setting(self._VS_INDEX_NAME_KEY, cleaned, user_email=user_email)
        return cleaned

    # ------------------------------------------------------------------
    # Tag auto-apply (apply-on-tag feature) — when ON, tag-mapped rules
    # are eagerly auto-attached to tables that receive a matching UC tag,
    # rather than only feeding them as suggestions for a steward to review.
    # Defaults to ``False`` (suggestion-only) so a fresh deploy or an unset
    # row never silently auto-applies rules; an admin must explicitly opt in.
    # Only an explicit ``"true"`` reads as on; any other value reads as off.
    # ------------------------------------------------------------------

    _TAG_AUTO_APPLY_KEY = "tag_auto_apply"

    def get_tag_auto_apply(self) -> bool:
        """Whether tag-mapped rules eagerly auto-attach (True) vs. only feed suggestions (False, default)."""
        raw = self.get_setting(self._TAG_AUTO_APPLY_KEY)
        return raw is not None and raw.strip().lower() == "true"

    def save_tag_auto_apply(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the tag-auto-apply setting. Returns the saved value."""
        self.save_setting(self._TAG_AUTO_APPLY_KEY, "true" if enabled else "false", user_email=user_email)
        return enabled

    # ------------------------------------------------------------------
    # Compute settings (P22-B) — the SQL warehouse used for app-side
    # ad-hoc SQL (View Data preview, discovery-style reads) and the jobs
    # compute used for the task-runner submission path. Both mirror
    # dqlake's Settings "jobs" section.
    #
    #   * ``sql_warehouse_id`` — a bare warehouse id. When unset (no row,
    #     or an empty value) callers fall back to the bundle-bound
    #     ``DATABRICKS_WAREHOUSE_ID`` env var, i.e. today's behaviour.
    #     :meth:`get_sql_warehouse_id` returns ``None`` in that case so
    #     the resolver (:func:`resolve_warehouse_id`) can apply the env
    #     fallback in one place.
    #   * ``jobs_compute_v1`` — a small JSON object describing the compute
    #     the task-runner job should use. ``{"kind": "serverless"}``
    #     (default) or ``{"kind": "existing_cluster", "cluster_id": "..."}``.
    #     Persisted + surfaced now; the submission-side wiring is a
    #     documented follow-up because ``job_service.py`` is frozen (see the
    #     route docstring in ``routes/v1/compute.py``).
    # ------------------------------------------------------------------

    _SQL_WAREHOUSE_ID_KEY = "sql_warehouse_id"
    _JOBS_COMPUTE_KEY = "jobs_compute_v1"

    JOBS_COMPUTE_SERVERLESS = "serverless"
    JOBS_COMPUTE_EXISTING_CLUSTER = "existing_cluster"

    def get_sql_warehouse_id(self) -> str | None:
        """Return the configured SQL warehouse id, or ``None`` when unset.

        ``None`` means "no admin override" — the caller should fall back
        to the ``DATABRICKS_WAREHOUSE_ID`` env var. An empty/whitespace
        stored value is treated the same as unset.
        """
        raw = self.get_setting(self._SQL_WAREHOUSE_ID_KEY)
        if raw is None or not raw.strip():
            return None
        return raw.strip()

    def save_sql_warehouse_id(self, warehouse_id: str, *, user_email: str | None = None) -> str:
        """Persist the SQL warehouse id (trimmed). An empty value clears the override."""
        cleaned = (warehouse_id or "").strip()
        self.save_setting(self._SQL_WAREHOUSE_ID_KEY, cleaned, user_email=user_email)
        return cleaned

    def get_jobs_compute(self) -> dict:
        """Return the configured jobs-compute selection.

        Defaults to ``{"kind": "serverless"}`` when unset or malformed so
        callers always get a well-formed object. An ``existing_cluster``
        selection carries a non-empty ``cluster_id``; anything else
        collapses back to serverless.
        """
        raw = self.get_setting(self._JOBS_COMPUTE_KEY)
        if not raw:
            return {"kind": self.JOBS_COMPUTE_SERVERLESS}
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning("jobs_compute_v1 setting is not valid JSON; defaulting to serverless")
            return {"kind": self.JOBS_COMPUTE_SERVERLESS}
        if not isinstance(parsed, dict):
            return {"kind": self.JOBS_COMPUTE_SERVERLESS}
        return self._normalise_jobs_compute(parsed)

    def save_jobs_compute(self, compute: dict, *, user_email: str | None = None) -> dict:
        """Persist the jobs-compute selection. Returns the normalised value."""
        normalised = self._normalise_jobs_compute(compute if isinstance(compute, dict) else {})
        self.save_setting(self._JOBS_COMPUTE_KEY, json.dumps(normalised), user_email=user_email)
        return normalised

    @classmethod
    def _normalise_jobs_compute(cls, compute: dict) -> dict:
        kind = compute.get("kind")
        if kind == cls.JOBS_COMPUTE_EXISTING_CLUSTER:
            cluster_id = compute.get("cluster_id")
            if isinstance(cluster_id, str) and cluster_id.strip():
                return {"kind": cls.JOBS_COMPUTE_EXISTING_CLUSTER, "cluster_id": cluster_id.strip()}
        return {"kind": cls.JOBS_COMPUTE_SERVERLESS}

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
