"""Central orchestrator that seeds the DQX Studio e-commerce demo.

:class:`DemoSeedService` ties the pure demo modules (:mod:`.manifest`,
:mod:`.datagen`, :mod:`.redate`) to the app's real governance services so a
single :meth:`DemoSeedService.run` call produces a fully governed, realistic
demo: deterministic source tables, an approved reusable rule set, approved
table bindings, approved data products, and a multi-week quality trend built
from genuine engine runs that are then re-dated into the past.

**The approval invariant.** The app runs with approvals ENABLED. This service
is an in-process orchestrator that bypasses the HTTP submit routes, so it can
NOT rely on any approvals-mode auto-publish (that only fires on the submit
route). Every governed object is therefore driven to ``approved`` by this
service's own explicit calls, replicating the exact per-object approval
sequence a human hand-approval performs (so the audit trail is identical):

* **Rules** — :meth:`RegistryService.match_or_create_approved_rule`, which
  lands the rule at ``status="approved"``, ``version==1``.
* **Bindings** — after :meth:`ApplyRulesService.save_applied_rules`, the same
  sequence the approve route runs: materialize the binding, transition its
  materialized checks ``draft -> pending_approval -> approved`` (reusing the
  module-level route helper :func:`_transition_binding_checks`), set the
  binding status ``approved``, then freeze a new version. A missed approval is
  caught downstream: :meth:`BindingRunService.run_binding` with
  ``source="approved"`` raises ``NeverApprovedError`` on a version-0 binding.
* **Data products** — :meth:`DataProductService.create` ->
  :meth:`~DataProductService.add_member` (requires each binding already
  approved) -> :meth:`~DataProductService.submit` ->
  :meth:`~DataProductService.approve`.

**Testability.** ``run(weeks=0)`` builds rules + bindings + products (all
approved) and writes a terminal status, but SKIPS the validation-gate real-run
and the weekly re-date loop, so the orchestration is unit-testable without a
warehouse. The real ``weeks>0`` path (validation gate + weekly mutate / run /
re-date) is behind that branch and exercised in the sandbox deploy.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from databricks_labs_dqx_app.backend.demo import datagen, manifest, redate
from databricks_labs_dqx_app.backend.demo.manifest import (
    BindingSpec,
    RuleSpec,
    WEEKS_DEFAULT,
    active_mapping,
)
from databricks_labs_dqx_app.backend.demo.status import DemoStatus, DemoStatusStore
from databricks_labs_dqx_app.backend.registry_models import (
    RESERVED_DESCRIPTION_KEY,
    RESERVED_DIMENSION_KEY,
    RESERVED_NAME_KEY,
    RESERVED_SEVERITY_KEY,
    RuleDefinition,
    RuleSlot,
    SlotFamily,
    set_reserved_tag,
    set_slot_tags,
)
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import _transition_binding_checks
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService, DesiredAppliedRule
from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.database_reset_service import DatabaseResetService
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

# A run is considered catastrophically misfiring when nearly every row fails a
# check — a symptom of a mis-bound rule (e.g. a predicate matching all rows).
# The demo's seeded fail rates are all comfortably below this, so any check at
# or above it in the validation gate indicates a broken binding, not a story.
_MISFIRE_RATE = 0.985
# How long to wait for a submitted binding run to reach a terminal state.
_RUN_TIMEOUT_SECONDS = 900
_RUN_POLL_SECONDS = 10


@dataclass
class DemoSeedResult:
    """Summary of a completed :meth:`DemoSeedService.run`.

    Args:
        rules: Number of registry rules created or reused.
        tables: Number of monitored-table bindings created or reused.
        products: Number of data products created or reused.
        weeks: Number of weeks of quality trend generated (0 for a build-only run).
        trend_points: Number of re-dated score-history points written.
    """

    rules: int
    tables: int
    products: int
    weeks: int
    trend_points: int


class DemoSeedService:
    """Orchestrates end-to-end seeding of the DQX Studio demo content."""

    def __init__(
        self,
        *,
        demo_sql: SqlExecutor,
        app_sql: SqlExecutor,
        oltp: OltpExecutorProtocol,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        apply_rules: ApplyRulesService,
        materializer: Materializer,
        rules_catalog: RulesCatalogService,
        version_service: MonitoredTableVersionService,
        data_products: DataProductService,
        binding_run: BindingRunService,
        score_cache: ScoreCacheService,
        status: DemoStatusStore,
        reset_service: DatabaseResetService | None = None,
        catalog: str = "dqx",
    ) -> None:
        self._demo_sql = demo_sql
        self._app_sql = app_sql
        self._oltp = oltp
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._apply_rules = apply_rules
        self._materializer = materializer
        self._rules_catalog = rules_catalog
        self._version_service = version_service
        self._data_products = data_products
        self._binding_run = binding_run
        self._score_cache = score_cache
        self._status = status
        self._reset_service = reset_service
        self._catalog = catalog
        self._schema = manifest.SOURCE_SCHEMA
        self._started_at = ""

    # ------------------------------------------------------------------
    # Public entrypoint
    # ------------------------------------------------------------------

    def run(self, *, user_email: str, wipe_first: bool, weeks: int = WEEKS_DEFAULT) -> DemoSeedResult:
        """Seed the full demo, driving every governed object to ``approved``.

        Writes a ``running`` status at start, phase updates as it proceeds, and
        a terminal ``succeeded`` status at the end. On ANY exception the status
        is set to ``failed`` (with a newline-stripped message) and the error is
        re-raised.

        Args:
            user_email: The admin triggering the seed; attributed on every
                created object and recorded in status updates.
            wipe_first: When True, clear all app-owned data via the reset
                service before seeding (a fresh, reproducible run).
            weeks: Number of weeks of quality story to generate. ``0`` builds
                the governed objects only and skips the validation gate and the
                weekly re-date loop (the unit-testable build-only path).

        Returns:
            A :class:`DemoSeedResult` summarising what was created.
        """
        self._started_at = self._now_iso()
        try:
            self._set_status("running", "starting", "Preparing demo seed", user_email)

            if wipe_first and self._reset_service is not None:
                self._set_status("running", "wipe", "Clearing existing app data", user_email)
                self._reset_service.reset_all_data(performed_by=user_email)

            self._set_status("running", "datagen", "Generating source tables", user_email)
            self._build_source_data()

            self._set_status("running", "rules", "Publishing reusable rule set", user_email)
            rule_map = self._build_rules(user_email)

            self._set_status("running", "bindings", "Applying and approving bindings", user_email)
            binding_map = self._build_bindings(rule_map, user_email)

            self._set_status("running", "products", "Creating and approving data products", user_email)
            product_ids = self._build_products(binding_map, user_email)

            trend_points = 0
            if weeks > 0:
                self._set_status("running", "validate", "Running validation gate", user_email)
                self._validation_gate(binding_map, user_email)
                self._set_status("running", "trend", f"Building {weeks}-week quality trend", user_email)
                trend_points = self._build_weekly_trend(binding_map, product_ids, weeks, user_email)
            else:
                # Build-only: still refresh caches so the app shows a truthful
                # (current) score for the freshly governed tables.
                self._score_cache.refresh_all_for_tables(sorted(self._table_fqns()))

            result = DemoSeedResult(
                rules=len(rule_map),
                tables=len(binding_map),
                products=len(product_ids),
                weeks=weeks,
                trend_points=trend_points,
            )
            self._set_status(
                "succeeded",
                "done",
                f"Seeded {result.rules} rules, {result.tables} tables, {result.products} products",
                user_email,
            )
            return result
        except Exception as exc:
            message = self._sanitize(str(exc))
            logger.exception("Demo seed failed")
            self._set_status("failed", "error", message, user_email)
            raise

    # ------------------------------------------------------------------
    # Phase: source data
    # ------------------------------------------------------------------

    def _build_source_data(self) -> None:
        """Create the demo schema, the source tables, and the governed column tags."""
        self._demo_sql.execute(datagen.create_schema_sql(self._catalog, self._schema))
        for table in manifest.TABLES:
            self._demo_sql.execute(datagen.build_create_table_sql(table.name, self._catalog, self._schema))
        for tag in manifest.COLUMN_TAGS:
            self._demo_sql.execute(datagen.build_set_column_tag_sql(tag, self._catalog, self._schema))

    # ------------------------------------------------------------------
    # Phase: rules
    # ------------------------------------------------------------------

    def _build_rules(self, user_email: str) -> dict[str, str]:
        """Match-or-create + approve every manifest rule; return ``rule_key -> rule_id``."""
        rule_map: dict[str, str] = {}
        for spec in manifest.RULES:
            definition = self._definition_for(spec)
            metadata = self._metadata_for(spec)
            rule, created = self._registry.match_or_create_approved_rule(definition, metadata, user_email)
            if rule is None:
                # A same-fingerprint, non-approved rule authored elsewhere blocks
                # auto-approval — extremely unlikely for demo content, but never
                # silently proceed without an approved rule to bind.
                raise RuntimeError(f"Could not obtain an approved rule for demo rule key '{spec.key}'")
            if getattr(rule, "status", "approved") != "approved":
                logger.warning("Demo rule '%s' resolved to status '%s' (expected approved)", spec.key, rule.status)
            rule_map[spec.key] = rule.rule_id
            logger.info("Demo rule '%s' -> %s (created=%s)", spec.key, rule.rule_id, created)
        return rule_map

    @staticmethod
    def _definition_for(spec: RuleSpec) -> RuleDefinition:
        """Build a :class:`RuleDefinition` from a manifest :class:`RuleSpec`."""
        slots = [
            RuleSlot(name=slot.name, family=cast(SlotFamily, slot.family), arg_key=slot.arg_key, position=index)
            for index, slot in enumerate(spec.slots)
        ]
        return RuleDefinition(body=dict(spec.body), slots=slots)

    @staticmethod
    def _metadata_for(spec: RuleSpec) -> dict[str, Any]:
        """Build a rule's reserved-tag ``user_metadata`` from a manifest :class:`RuleSpec`."""
        metadata: dict[str, Any] = {}
        metadata = set_reserved_tag(metadata, RESERVED_NAME_KEY, spec.name)
        metadata = set_reserved_tag(metadata, RESERVED_DESCRIPTION_KEY, spec.description)
        metadata = set_reserved_tag(metadata, RESERVED_DIMENSION_KEY, spec.dimension)
        metadata = set_reserved_tag(metadata, RESERVED_SEVERITY_KEY, spec.severity)
        if spec.slot_tags:
            metadata = set_slot_tags(metadata, {slot: list(tags) for slot, tags in spec.slot_tags.items()})
        return metadata

    # ------------------------------------------------------------------
    # Phase: bindings
    # ------------------------------------------------------------------

    def _build_bindings(self, rule_map: dict[str, str], user_email: str) -> dict[str, str]:
        """Register + apply + approve every binding; return ``table_name -> binding_id``."""
        binding_map: dict[str, str] = {}
        for binding in manifest.BINDINGS:
            binding_id = self._register_binding(binding.table, user_email)
            binding_map[binding.table] = binding_id
            desired = self._desired_rules(binding, rule_map, week=0)
            self._apply_rules.save_applied_rules(binding_id, desired, user_email)
            self._approve_binding(binding_id, user_email)
        return binding_map

    def _register_binding(self, table: str, user_email: str) -> str:
        """Register a monitored table, reusing an existing binding on duplicate."""
        table_fqn = self._table_fqn(table)
        try:
            registered = self._monitored_tables.register(table_fqn, user_email)
            return registered.binding_id
        except DuplicateMonitoredTableError:
            existing = self._monitored_tables.get_by_table_fqn(table_fqn)
            if existing is None:
                raise
            return existing.table.binding_id

    def _desired_rules(
        self, binding: BindingSpec, rule_map: dict[str, str], week: int
    ) -> list[DesiredAppliedRule]:
        """Build the desired applied-rule set for a binding at *week* (one entry per rule key)."""
        desired: list[DesiredAppliedRule] = []
        for rule_key, groups in active_mapping(binding, week).items():
            rule_id = rule_map.get(rule_key)
            if rule_id is None:
                continue
            column_mapping = [dict(group) for group in groups]
            desired.append(DesiredAppliedRule(rule_id=rule_id, column_mapping=column_mapping))
        return desired

    def _approve_binding(self, binding_id: str, user_email: str) -> None:
        """Drive a binding's materialized checks to ``approved`` and freeze a version.

        Replicates the exact approve-route sequence (materialize, transition
        ``draft -> pending_approval -> approved``, set binding status, freeze)
        so the audit trail is identical to a hand-approval and the binding is
        genuinely approved — not reliant on any approvals-mode auto-publish.
        """
        self._materializer.materialize_binding(binding_id)
        _transition_binding_checks(
            self._monitored_tables,
            self._rules_catalog,
            binding_id,
            from_status="draft",
            to_status="pending_approval",
            user_email=user_email,
        )
        _transition_binding_checks(
            self._monitored_tables,
            self._rules_catalog,
            binding_id,
            from_status="pending_approval",
            to_status="approved",
            user_email=user_email,
        )
        self._monitored_tables.set_status(binding_id, "approved", user_email)
        self._version_service.freeze_new_version(binding_id, user_email)

    # ------------------------------------------------------------------
    # Phase: data products
    # ------------------------------------------------------------------

    def _build_products(self, binding_map: dict[str, str], user_email: str) -> list[str]:
        """Create, populate, submit and approve every data product; return product ids."""
        product_ids: list[str] = []
        for spec in manifest.DATA_PRODUCTS:
            product = self._data_products.create(spec.name, spec.description, None, user_email)
            for member in spec.members:
                binding_id = binding_map.get(member)
                if binding_id is None:
                    continue
                self._data_products.add_member(product.product_id, binding_id, None, user_email)
            self._data_products.submit(product.product_id, user_email)
            self._data_products.approve(product.product_id, user_email)
            product_ids.append(product.product_id)
        return product_ids

    # ------------------------------------------------------------------
    # Phase: validation gate (weeks > 0)
    # ------------------------------------------------------------------

    def _validation_gate(self, binding_map: dict[str, str], user_email: str) -> None:
        """Reset to baseline, run every binding once, and hard-fail a catastrophic misfire."""
        for stmt in datagen.build_baseline_reset_sql(self._catalog, self._schema):
            self._demo_sql.execute(stmt)
        for table, binding_id in binding_map.items():
            run = self._binding_run.run_binding(binding_id, "approved", None, user_email)
            self._wait_for_run(run.run_id)
            self._assert_no_misfire(table, run.run_id)

    def _assert_no_misfire(self, table: str, run_id: str) -> None:
        """Raise when a run's check failed nearly every row (a mis-bound rule)."""
        input_rows, failures = self._read_run_check_failures(run_id)
        if input_rows <= 0:
            return
        for check_name, failed in failures.items():
            rate = failed / input_rows
            if rate >= _MISFIRE_RATE:
                raise RuntimeError(
                    f"Validation gate: check '{self._sanitize(check_name)}' on table "
                    f"'{self._sanitize(table)}' failed {rate:.3f} of rows — likely a mis-bound rule."
                )

    def _read_run_check_failures(self, run_id: str) -> tuple[int, dict[str, int]]:
        """Return ``(input_rows, {check_name: failed_rows})`` for a run from ``dq_metrics``."""
        metrics_fqn = self._app_sql.fqn("dq_metrics")
        rows = self._app_sql.query_dicts(
            f"SELECT metric_name, metric_value FROM {metrics_fqn} "  # noqa: S608
            f"WHERE run_id = '{escape_sql_string(run_id)}'"
        )
        input_rows = 0
        failures: dict[str, int] = {}
        for row in rows:
            name = row.get("metric_name")
            value = row.get("metric_value")
            if name == "input_row_count" and value:
                input_rows = self._safe_int(value)
            elif name == "check_metrics" and value:
                failures = self._parse_check_failures(value)
        return input_rows, failures

    @staticmethod
    def _parse_check_failures(check_metrics_json: str) -> dict[str, int]:
        """Sum error + warning counts per check from a ``check_metrics`` JSON array."""
        import json

        try:
            parsed = json.loads(check_metrics_json)
        except (ValueError, TypeError):
            return {}
        failures: dict[str, int] = {}
        if not isinstance(parsed, list):
            return {}
        for entry in parsed:
            if not isinstance(entry, dict):
                continue
            name = entry.get("check_name")
            if not isinstance(name, str):
                continue
            errors = DemoSeedService._safe_int(entry.get("error_count"))
            warnings = DemoSeedService._safe_int(entry.get("warning_count"))
            failures[name] = errors + warnings
        return failures

    # ------------------------------------------------------------------
    # Phase: weekly quality trend (weeks > 0)
    # ------------------------------------------------------------------

    def _build_weekly_trend(
        self,
        binding_map: dict[str, str],
        product_ids: list[str],
        weeks: int,
        user_email: str,
    ) -> int:
        """Generate a *weeks*-long trend from real runs, re-dated into the past.

        Each week: apply the week's rule lifecycle to changed bindings, mutate
        the source data to the week's fail levels, run every binding, wait for
        completion, then re-date the run's metrics / validation-run / score
        rows to the week's instant. Product and global scores are refreshed and
        re-dated once per week. Returns the number of score-history points
        re-dated (tables + products + global across all weeks).
        """
        now = datetime.now(timezone.utc)
        binding_specs = {b.table: b for b in manifest.BINDINGS}
        rule_map = {spec.key: rid for spec, rid in self._current_rule_ids().items()}
        last_active: dict[str, tuple[str, ...]] = {}
        trend_points = 0

        for week in range(weeks):
            instant = self._week_instant(now, week, weeks)
            target_iso = redate.iso(instant)

            for table, binding in binding_specs.items():
                binding_id = binding_map.get(table)
                if binding_id is None:
                    continue
                active_keys = tuple(sorted(active_mapping(binding, week).keys()))
                if last_active.get(table) != active_keys:
                    desired = self._desired_rules(binding, rule_map, week)
                    self._apply_rules.save_applied_rules(binding_id, desired, user_email)
                    self._approve_binding(binding_id, user_email)
                    last_active[table] = active_keys

            for stmt in self._week_mutations(week, weeks):
                self._demo_sql.execute(stmt)

            for table, binding_id in binding_map.items():
                run = self._binding_run.run_binding(binding_id, "approved", None, user_email)
                self._wait_for_run(run.run_id)
                self._redate_run(run.run_id, target_iso)
                self._score_cache.refresh_for_tables([self._table_fqn(table)])
                self._redate_history("table", self._table_fqn(table), target_iso)
                trend_points += 1

            for product_id in product_ids:
                self._score_cache.refresh_product(product_id)
                self._redate_history("product", product_id, target_iso)
                trend_points += 1
            self._score_cache.refresh_global()
            self._redate_history("global", "global", target_iso)
            trend_points += 1

        # Final truthful "now" refresh so the app's cached scores are current.
        self._score_cache.refresh_all_for_tables(sorted(self._table_fqns()))
        return trend_points

    def _current_rule_ids(self) -> dict[RuleSpec, str]:
        """Resolve each manifest rule spec to its approved registry rule id (by fingerprint)."""
        resolved: dict[RuleSpec, str] = {}
        for spec in manifest.RULES:
            existing = self._registry.find_approved_rule_for_definition(self._definition_for(spec))
            if existing is not None:
                resolved[spec] = existing.rule_id
        return resolved

    def _week_mutations(self, week: int, weeks: int) -> list[str]:
        """Flatten every table's per-week mutation statements for *week*."""
        stmts: list[str] = []
        for table in manifest.TABLES:
            stmts.extend(datagen.build_mutation_sql(table.name, week, weeks, self._catalog, self._schema))
        return stmts

    def _redate_run(self, run_id: str, target_iso: str) -> None:
        """Shift a run's ``dq_metrics`` and ``dq_validation_runs`` timestamps to *target_iso*."""
        metrics_fqn = self._app_sql.fqn("dq_metrics")
        runs_fqn = self._app_sql.fqn("dq_validation_runs")
        self._app_sql.execute(redate.build_redate_metrics_sql(metrics_fqn, run_id, target_iso))
        self._app_sql.execute(redate.build_redate_runs_sql(runs_fqn, run_id, target_iso))

    def _redate_history(self, scope_type: str, scope_key: str, target_iso: str) -> None:
        """Re-date the most recently appended ``dq_score_history`` row of a scope to *target_iso*."""
        history_fqn = self._oltp.fqn("dq_score_history")
        self._oltp.execute(redate.build_redate_latest_history_sql(history_fqn, scope_type, scope_key, target_iso))

    @staticmethod
    def _week_instant(now: datetime, week: int, weeks: int) -> datetime:
        """Return the (irregularly spaced) instant for *week*; the final week shares *now*."""
        if week >= weeks - 1:
            return now
        days_back = (weeks - 1 - week) * 7
        jitter_hours = ((week * 13 + 5) % 11) - 5
        return now - timedelta(days=days_back, hours=jitter_hours)

    def _wait_for_run(self, run_id: str) -> str:
        """Poll ``dq_validation_runs`` until the run is terminal; return its final status."""
        runs_fqn = self._app_sql.fqn("dq_validation_runs")
        deadline = time.monotonic() + _RUN_TIMEOUT_SECONDS
        while True:
            rows = self._app_sql.query_dicts(
                f"SELECT status FROM {runs_fqn} "  # noqa: S608
                f"WHERE run_id = '{escape_sql_string(run_id)}'"
            )
            status = rows[0].get("status") if rows else None
            if status in ("SUCCESS", "FAILED", "CANCELED"):
                return status or "FAILED"
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Timed out waiting for run {self._sanitize(run_id)} to finish")
            time.sleep(_RUN_POLL_SECONDS)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _table_fqn(self, table: str) -> str:
        return f"{self._catalog}.{self._schema}.{table}"

    def _table_fqns(self) -> list[str]:
        return [self._table_fqn(t.name) for t in manifest.TABLES]

    def _set_status(self, state: str, phase: str, message: str, user_email: str) -> None:
        self._status.set(
            DemoStatus(
                state=state,
                phase=phase,
                message=self._sanitize(message),
                started_at=self._started_at,
                updated_at=self._now_iso(),
            ),
            user_email=user_email,
        )

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _sanitize(text: str) -> str:
        """Strip newlines/carriage returns to prevent log/status injection (CWE-117)."""
        return text.replace("\n", " ").replace("\r", " ").strip()

    @staticmethod
    def _safe_int(value: object) -> int:
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0
