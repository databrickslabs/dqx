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

* **Rules** — the genuine ``RegistryService.create_rule -> submit -> approve``
  publish path (respecting each rule's real mode/polarity/author_kind), which
  lands the rule at ``status="approved"``, ``version==1``. An already-approved
  rule with the same fingerprint is reused for idempotency.
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

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EntityTagAssignment

from databricks_labs_dqx_app.backend.demo import datagen, manifest, redate
from databricks_labs_dqx_app.backend.demo.manifest import (
    BindingSpec,
    RuleSpec,
    UNIQUE_EXPECT_ROWS,
    WEEKS_DEFAULT,
    active_mapping,
)
from databricks_labs_dqx_app.backend.lowcode_compile import compile_lowcode_body
from databricks_labs_dqx_app.backend.demo.status import DemoStatus, DemoStatusStore
from databricks_labs_dqx_app.backend.registry_models import (
    RESERVED_DESCRIPTION_KEY,
    RESERVED_DIMENSION_KEY,
    RESERVED_NAME_KEY,
    RESERVED_SEVERITY_KEY,
    AuthorKind,
    ParamType,
    Polarity,
    RuleDefinition,
    RuleMode,
    RuleParameter,
    RuleParamValue,
    RuleSlot,
    SlotFamily,
    set_reserved_tag,
    set_slot_tags,
)
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
# The weekly trend fans out one serverless Job per table, so up to a handful of
# runs contend for cold-start capacity at once. A legitimate run can therefore
# exceed 15 minutes under contention, so this is a generous 30-minute ceiling
# (defense-in-depth): the real safeguard against false timeouts is that
# :meth:`_wait_for_run` reads the terminal row across ALL rows for a run_id,
# never latching onto a stale RUNNING placeholder.
_RUN_TIMEOUT_SECONDS = 1800
_RUN_POLL_SECONDS = 10
# Terminal run states as written to ``dq_validation_runs.status`` by the runner.
_TERMINAL_RUN_STATES = ("SUCCESS", "FAILED", "CANCELED")
# The runner appends the terminal ``dq_validation_runs`` row BEFORE it writes the
# run's ``dq_metrics`` rows, so a run can be "terminal" (per :meth:`_wait_for_run`)
# a beat before its metrics exist. Before re-dating ``dq_metrics`` we poll for
# those rows so the re-date UPDATE never matches zero rows and strands a week at
# wall-clock now. Bounded and short — the metrics write trails the terminal row
# by seconds, not minutes.
_METRICS_TIMEOUT_SECONDS = 300
_METRICS_POLL_SECONDS = 5


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
        sp_ws: WorkspaceClient,
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
        self._sp_ws = sp_ws
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
                # Capture a single "now" so the validation gate's cleanup and the
                # final week's shared instant use one consistent wall-clock, with
                # no drift between the gate run and the final-week re-date.
                now = datetime.now(timezone.utc)
                self._set_status("running", "validate", "Running validation gate", user_email)
                self._validation_gate(binding_map, user_email)
                self._set_status("running", "trend", f"Building {weeks}-week quality trend", user_email)
                trend_points = self._build_weekly_trend(
                    binding_map, rule_map, product_ids, weeks, user_email, now
                )
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
        # Governed column tags are a best-effort showcase, not core demo state.
        # Assigning a governed tag needs the ASSIGN privilege on that tag AND a
        # metastore that defines it; either can be absent in a given workspace.
        # A tag that can't be applied must NOT abort the ~1h build — log the
        # governed-tag name (a manifest constant, not user input) and continue.
        for tag in manifest.COLUMN_TAGS:
            try:
                self._assign_column_tag(tag)
            except Exception as exc:
                # Broad except by design (see the BLE001 policy block in
                # pyproject.toml): tag assignment is a best-effort showcase, so
                # ANY failure — a missing ASSIGN privilege, an undefined tag, a
                # transient API error — is logged and skipped rather than
                # aborting the ~1h seed.
                logger.warning(
                    "Skipped governed tag %s on %s.%s: %s",
                    tag.tag,
                    tag.table,
                    tag.column,
                    self._sanitize(str(exc)),
                )

    def _assign_column_tag(self, tag: manifest.ColumnTagSpec) -> None:
        """Assign one governed ``class.*`` column tag via the Entity Tag Assignments API.

        Governed tags have dotted keys (e.g. ``class.location``) that NO
        ``ALTER TABLE ... SET TAGS`` SQL can assign — the ``.`` is reserved in
        the tag-key grammar. The Unity Catalog Entity Tag Assignments API is the
        correct mechanism, so this calls
        :meth:`sp_ws.entity_tag_assignments.create` with the full dotted key
        against the fully-qualified column entity. Classification (``class.*``)
        tags carry an empty value; their allowed-values set is what constrains
        them.

        Args:
            tag: the governed column tag specification to assign.

        Raises:
            ValueError: if the tag key is not in the ``class.*`` namespace.
        """
        if not tag.tag.startswith("class."):
            raise ValueError(f"governed demo tags must be class.*: {tag.tag}")
        entity_name = f"{self._catalog}.{self._schema}.{tag.table}.{tag.column}"
        self._sp_ws.entity_tag_assignments.create(
            EntityTagAssignment(
                entity_type="columns",
                entity_name=entity_name,
                tag_key=tag.tag,
                tag_value="",
            )
        )

    # ------------------------------------------------------------------
    # Phase: rules
    # ------------------------------------------------------------------

    def _build_rules(self, user_email: str) -> dict[str, str]:
        """Create + submit + approve every manifest rule; return ``rule_key -> rule_id``.

        Uses the genuine ``create_rule -> submit -> approve`` publish path so
        each rule keeps its REAL mode, polarity and author_kind. (The former
        :meth:`RegistryService.match_or_create_approved_rule` shortcut is the
        profiler-suggestion primitive: it hardcodes ``mode="dqx_native"`` and
        ``author_kind="ai_assisted"``, so a ``sql``-mode demo rule would be
        stored as ``dqx_native`` and later materialize to ``function: ''`` — a
        runtime "function '' is not defined" failure.)

        Idempotent by structural fingerprint: an already-approved rule with the
        same definition is reused rather than re-created (the seed re-runs, and
        ``wipe_first`` resets, but this stays safe either way).
        """
        rule_map: dict[str, str] = {}
        for spec in manifest.RULES:
            definition = self._definition_for(spec)
            existing = self._registry.find_approved_rule_for_definition(definition)
            if existing is not None:
                rule_map[spec.key] = existing.rule_id
                logger.info("Demo rule '%s' -> %s (reused approved)", spec.key, existing.rule_id)
                continue
            rule, _warning = self._registry.create_rule(
                mode=cast(RuleMode, spec.mode),
                definition=definition,
                user_email=user_email,
                polarity=cast(Polarity, spec.polarity) if spec.polarity is not None else None,
                author_kind=cast(AuthorKind, spec.author_kind),
                user_metadata=self._metadata_for(spec),
                source="demo",
            )
            self._registry.submit(rule.rule_id, user_email)
            approved = self._registry.approve(rule.rule_id, user_email)
            rule_map[spec.key] = approved.rule_id
            logger.info("Demo rule '%s' -> %s (created)", spec.key, approved.rule_id)
        return rule_map

    @staticmethod
    def _definition_for(spec: RuleSpec) -> RuleDefinition:
        """Build a :class:`RuleDefinition` from a manifest :class:`RuleSpec`.

        Slots come straight from the spec. Scalar (non-column) arguments are
        declared as typed :class:`RuleParameter` entries (so the authoring UI
        reads them from ``definition.parameters``, not from ``body.arguments``).
        A ``lowcode`` spec carries only the re-editable ``lowcode_ast`` (+
        optional ``group_by``); its body is compiled here into the final stored
        payload (``predicate`` / ``sql_query`` / ``merge_columns``) via
        :func:`lowcode_compile.compile_lowcode_body`, exactly as the AI
        "build with AI" path (``ai_rules_service._build_lowcode_body``) does.
        """
        slots = [
            RuleSlot(
                name=slot.name,
                family=cast(SlotFamily, slot.family),
                arg_key=slot.arg_key,
                position=index,
                cardinality="many" if slot.arg_key == "columns" else "one",
            )
            for index, slot in enumerate(spec.slots)
        ]
        parameters = [
            RuleParameter(name=param.name, type=cast(ParamType, param.type), value=cast(RuleParamValue, param.value))
            for param in spec.parameters
        ]
        body = DemoSeedService._compiled_body(spec) if spec.mode == "lowcode" else dict(spec.body)
        return RuleDefinition(body=body, slots=slots, parameters=parameters)

    @staticmethod
    def _compiled_body(spec: RuleSpec) -> dict[str, Any]:
        """Compile a ``lowcode`` spec's AST into the stored ``definition.body``.

        Byte-for-byte the shape ``ai_rules_service._build_lowcode_body`` and
        ``RegistryRuleFormDialog.buildDefinition`` write: the re-editable
        ``lowcode_ast`` (so the visual builder rehydrates exactly), the raw
        ``group_by`` string when present, and the compiled ``predicate`` OR
        ``sql_query`` + ``merge_columns`` that actually materializes and runs.
        """
        ast = cast(dict[str, Any], spec.body.get("lowcode_ast", {}))
        group_by = cast(str, spec.body.get("group_by", "") or "")
        compiled = compile_lowcode_body(ast, group_by)
        body: dict[str, Any] = {"lowcode_ast": ast}
        if group_by:
            body["group_by"] = group_by
        if compiled.predicate is not None:
            body["predicate"] = compiled.predicate
        if compiled.sql_query is not None:
            body["sql_query"] = compiled.sql_query
        if compiled.merge_columns is not None:
            body["merge_columns"] = compiled.merge_columns
        return body

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

    def _desired_rules(self, binding: BindingSpec, rule_map: dict[str, str], week: int) -> list[DesiredAppliedRule]:
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
        # Imported lazily: ``routes.v1`` imports this module (via ``admin.py``),
        # so a module-level import here forms a circular import when this module
        # is loaded first.
        from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import _transition_binding_checks

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
        """Reset to baseline, run every binding once, hard-fail a misfire, then discard the gate runs.

        The gate runs execute at real wall-clock "now" and are ``published``,
        but are throwaway — they exist only to check the seeded fail rates. They
        are NEVER re-dated, so if left in place a gate run's ``run_time`` would
        beat every re-dated (past) weekly run in the score cache's
        latest-published-run selection, showing a stale headline score. Once the
        misfire assertions pass, delete each gate run's rows from ``dq_metrics``
        and ``dq_validation_runs`` so the weekly-loop runs alone define the trend.
        """
        for stmt in datagen.build_baseline_reset_sql(self._catalog, self._schema):
            self._demo_sql.execute(stmt)
        # Fan out the gate runs: submit EVERY binding's run first (each
        # ``run_binding`` only submits an async Job and returns immediately),
        # then wait for all, then assert no misfire — so the tables' Jobs run
        # concurrently rather than one at a time.
        gate_runs: dict[str, str] = {}
        for table, binding_id in binding_map.items():
            run = self._binding_run.run_binding(binding_id, "approved", None, user_email)
            gate_runs[table] = run.run_id
        for run_id in gate_runs.values():
            self._wait_for_run(run_id)
        for table, run_id in gate_runs.items():
            self._assert_no_misfire(table, run_id)
        for run_id in gate_runs.values():
            self._delete_run(run_id)

    def _delete_run(self, run_id: str) -> None:
        """Delete a throwaway run's ``dq_metrics`` and ``dq_validation_runs`` rows."""
        metrics_fqn = self._app_sql.fqn("dq_metrics")
        runs_fqn = self._app_sql.fqn("dq_validation_runs")
        self._app_sql.execute(redate.build_delete_metrics_sql(metrics_fqn, run_id))
        self._app_sql.execute(redate.build_delete_runs_sql(runs_fqn, run_id))

    def _assert_no_misfire(self, table: str, run_id: str) -> None:
        """Raise when a run's check misfired — catastrophic rate, or a uniqueness band breach.

        Two independent gates:

        * **Catastrophic rate** — any check failing at or above
          :data:`_MISFIRE_RATE` of rows is a mis-bound predicate.
        * **Uniqueness band** — the ``unique`` rule's check must land inside
          :data:`~.manifest.UNIQUE_EXPECT_ROWS`. A mis-bound uniqueness rule
          (e.g. keyed on the wrong column) flags every row, so a failed-row
          count outside the expected ``(low, high)`` band is a misfire even
          when it stays below the catastrophic rate.
        """
        input_rows, failures = self._read_run_check_failures(run_id)
        if input_rows <= 0:
            return
        unique_check_name = manifest.RULES_BY_KEY["unique"].name
        low, high = UNIQUE_EXPECT_ROWS
        unique_check_seen = False
        for check_name, failed in failures.items():
            rate = failed / input_rows
            if rate >= _MISFIRE_RATE:
                raise RuntimeError(
                    f"Validation gate: check '{self._sanitize(check_name)}' on table "
                    f"'{self._sanitize(table)}' failed {rate:.3f} of rows — likely a mis-bound rule."
                )
            if check_name == unique_check_name:
                unique_check_seen = True
                if not (low <= failed <= high):
                    raise RuntimeError(
                        f"Validation gate: uniqueness check '{self._sanitize(check_name)}' on table "
                        f"'{self._sanitize(table)}' failed {failed} rows, outside the expected "
                        f"band [{low}, {high}] — likely a mis-bound uniqueness rule."
                    )
        # The uniqueness band gate matches by check_name == the unique rule's
        # reserved name. If the binding has the unique rule applied at gate time
        # yet no metrics check carries that name (e.g. metrics fell back to
        # rule_id), the band silently never fires. The catastrophic-rate gate
        # still guards correctness, so don't hard-fail — but make the skip
        # observable rather than passing mutely.
        if not unique_check_seen and self._unique_rule_active_at_gate(table):
            logger.warning(
                f"Validation gate: table '{self._sanitize(table)}' has the uniqueness rule applied "
                f"but no check named '{self._sanitize(unique_check_name)}' appeared in its metrics "
                f"— the uniqueness band was skipped."
            )

    @staticmethod
    def _unique_rule_active_at_gate(table: str) -> bool:
        """Whether *table*'s manifest binding has the ``unique`` rule applied at gate time.

        The validation gate runs against the week-0 binding state, so a binding
        counts as having the uniqueness rule when it is in its week-0 active
        mapping. Used only to decide whether a missing uniqueness check is a
        loggable skip.
        """
        for binding in manifest.BINDINGS:
            if binding.table == table:
                return "unique" in active_mapping(binding, 0)
        return False

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
        rule_map: dict[str, str],
        product_ids: list[str],
        weeks: int,
        user_email: str,
        now: datetime,
    ) -> int:
        """Generate a *weeks*-long trend from real runs, re-dated into the past.

        Each week: apply the week's rule lifecycle to changed bindings, mutate
        the source data to the week's fail levels, run every binding, wait for
        completion, then re-date the run's metrics / validation-run / score
        rows to the week's instant. Product and global scores are refreshed and
        re-dated once per week. Returns the number of score-history points
        re-dated (tables + products + global across all weeks).

        The *authoritative* ``rule_map`` built by :meth:`_build_rules` (and used
        by :meth:`_build_bindings`) is threaded in and used verbatim for every
        weekly re-apply. It is NEVER re-derived by structural fingerprint here:
        re-resolving each rule id per week could hand back a different or missing
        id than :meth:`_build_rules` created (e.g. after ``_tighten_card_rule``
        bumps ``card_format``'s version, or for a fingerprint near-collision), so
        the weekly ``save_applied_rules`` would rewrite a binding with an
        incomplete or mismatched rule set — dropping rules and desynchronising
        the applied ``rule_id`` from the registry id the Results tab queries. Using
        the one authoritative map keeps the SAME rule ids applied at week 0 and
        every week after, so counts stay stable and Results stays unlocked.

        Args:
            binding_map: table name -> approved binding id.
            rule_map: the authoritative rule_key -> rule_id map from
                :meth:`_build_rules` (same map :meth:`_build_bindings` used).
            product_ids: the approved data-product ids to refresh per week.
            weeks: number of weeks of trend to generate.
            user_email: the admin attributed on each run.
            now: the single "now" instant captured before the validation gate;
                the final week shares it so there is no drift between the gate
                wall-clock and the final-week re-date.
        """
        binding_specs = {b.table: b for b in manifest.BINDINGS}
        last_active: dict[str, tuple[str, ...]] = {}
        trend_points = 0

        for week in range(weeks):
            instant = self._week_instant(now, week, weeks)
            target_iso = redate.iso(instant)

            if week == manifest.TIGHTEN_WEEK:
                self._tighten_card_rule(rule_map, user_email)

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

            # Fan out the week's runs: ``run_binding`` only SUBMITS an async
            # serverless Job (it returns the run_id immediately); the wait is
            # what serializes. So submit EVERY binding's run first, collecting
            # ``{table: run_id}``, then wait for all of them, then re-date +
            # refresh each. This runs the tables' Jobs concurrently on
            # Databricks instead of one table at a time (mirrors
            # ``DataProductService.run``'s submit-all-then-collect fan-out).
            week_runs: dict[str, str] = {}
            for table, binding_id in binding_map.items():
                run = self._binding_run.run_binding(binding_id, "approved", None, user_email)
                week_runs[table] = run.run_id
            for run_id in week_runs.values():
                self._wait_for_run(run_id)
            for table, run_id in week_runs.items():
                self._redate_run(run_id, target_iso)
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

        # Final truthful refresh so the app's cached scores are current. This
        # re-runs tables -> products -> global to update ``dq_score_cache``, but
        # ScoreCacheService ALSO appends one ``dq_score_history`` row per scope at
        # real wall-clock ``now()`` — and those appends are never re-dated, so
        # they would leave stuck real-now points on the back-dated trend. Every
        # genuine weekly point was already re-dated to at-or-before the final
        # week's instant, so we snapshot a cutoff the instant BEFORE the refresh
        # and delete every history row appended at/after it. Using a cutoff taken
        # here (not the seed-start ``now``, which precedes this refresh by the
        # whole run) guarantees the refresh's own appends are stripped regardless
        # of how long the run took, while the re-dated weekly points survive.
        cutoff = redate.iso(datetime.now(timezone.utc) - timedelta(seconds=1))
        self._score_cache.refresh_all_for_tables(sorted(self._table_fqns()))
        self._delete_history_after(cutoff)
        return trend_points

    def _tighten_card_rule(self, rule_map: dict[str, str], user_email: str) -> None:
        """Edit + re-approve the card-validation rule to a new version at TIGHTEN_WEEK.

        The "tightened card validation" story beat should be visible as a REAL
        registry rule version increment, not only a data-mutation swing. This
        runs the genuine revision path on the ``card_format`` rule — edit the
        approved rule in place (:meth:`RegistryService.update_draft`), submit
        the revision, then re-approve it (which bumps ``version`` N -> N+1 and
        freezes a new ``dq_rule_versions`` snapshot).

        The edit is metadata-only (a tightened description): the fingerprint
        excludes descriptive tags, so the rule's fingerprint — and therefore
        every binding's column materialization — is unchanged and stays valid.

        Best-effort: a failure here is logged and swallowed so it can never
        abort the ~1h seed, but under normal operation it succeeds.
        """
        rule_id = rule_map.get("card_format")
        if rule_id is None:
            logger.warning("Card rule not found in rule map; skipping the TIGHTEN_WEEK version bump")
            return
        spec = manifest.RULES_BY_KEY["card_format"]
        try:
            metadata = self._metadata_for(spec)
            metadata = set_reserved_tag(metadata, RESERVED_DESCRIPTION_KEY, manifest.CARD_RULE_TIGHTENED_DESCRIPTION)
            self._registry.update_draft(rule_id, user_email, user_metadata=metadata)
            self._registry.submit(rule_id, user_email)
            approved = self._registry.approve(rule_id, user_email)
            logger.info("Tightened card rule %s -> v%s", rule_id, getattr(approved, "version", "?"))
        except Exception as exc:
            # Broad except by design (see the BLE001 policy block in
            # pyproject.toml): the mid-history version bump is a best-effort
            # story beat, so any failure is logged and skipped rather than
            # aborting the seed's trend build.
            logger.warning("Skipped card-rule version bump on %s: %s", rule_id, self._sanitize(str(exc)))

    def _delete_history_after(self, cutoff_iso: str) -> None:
        """Delete ``dq_score_history`` rows appended after *cutoff_iso* (the polluting real-now appends)."""
        history_fqn = self._oltp.fqn("dq_score_history")
        self._oltp.execute(redate.build_delete_history_after_sql(history_fqn, cutoff_iso))

    def _week_mutations(self, week: int, weeks: int) -> list[str]:
        """Flatten every table's per-week mutation statements for *week*."""
        stmts: list[str] = []
        for table in manifest.TABLES:
            stmts.extend(datagen.build_mutation_sql(table.name, week, weeks, self._catalog, self._schema))
        return stmts

    def _redate_run(self, run_id: str, target_iso: str) -> None:
        """Shift a run's ``dq_metrics`` and ``dq_validation_runs`` timestamps to *target_iso*.

        The runner writes the terminal ``dq_validation_runs`` (SUCCESS) row
        BEFORE it writes the run's ``dq_metrics`` rows (see the task runner:
        ``result_row.writeTo(...).append()`` precedes ``_persist_observed_metrics``).
        :meth:`_wait_for_run` polls only ``dq_validation_runs``, so it can return
        the instant the terminal row lands — while the metrics rows for this
        run_id do not yet exist. Re-dating ``dq_metrics`` at that moment would
        match ZERO rows, leaving the week's metrics stuck at real wall-clock and
        producing an isolated disconnected 'now' point in the Score-by-Severity
        chart (the symptom this fixes). So before re-dating we WAIT for the
        metrics rows to exist, and if they never appear we log a loud demo-layer
        warning rather than silently skipping the re-date.
        """
        metrics_fqn = self._app_sql.fqn("dq_metrics")
        runs_fqn = self._app_sql.fqn("dq_validation_runs")
        # A run writes SEVERAL ``dq_metrics`` rows that can trickle in over a few
        # seconds AFTER the terminal ``dq_validation_runs`` row that
        # :meth:`_wait_for_run` gates on. So re-date, then re-check for any row of
        # this run_id still off the target instant, and re-date again until none
        # remain (or a bounded deadline). This catches metric rows that landed
        # after the first UPDATE — otherwise they stay at real wall-clock and
        # orphan the week's dimension/severity point (those charts read
        # ``dq_metrics.run_time`` directly). Matches dqlake, where one UPDATE on a
        # single fact table suffices.
        deadline = time.monotonic() + _METRICS_TIMEOUT_SECONDS
        while True:
            self._wait_for_metrics(run_id)
            self._app_sql.execute(redate.build_redate_metrics_sql(metrics_fqn, run_id, target_iso))
            self._app_sql.execute(redate.build_redate_runs_sql(runs_fqn, run_id, target_iso))
            if not self._metrics_off_target(run_id, target_iso):
                return
            if time.monotonic() >= deadline:
                logger.warning(
                    "Demo re-date: run %s still has dq_metrics rows off the target instant after %ss; "
                    "a stray trend point may remain.",
                    self._sanitize(run_id),
                    _METRICS_TIMEOUT_SECONDS,
                )
                return
            time.sleep(_METRICS_POLL_SECONDS)

    def _metrics_off_target(self, run_id: str, target_iso: str) -> bool:
        """Return True if any ``dq_metrics`` row for *run_id* is not at *target_iso*.

        Used by :meth:`_redate_run` to detect metric rows that arrived after the
        re-date UPDATE (so they are still at real wall-clock and would orphan the
        week's trend point). ``iso`` emits whole-second instants, which cast back
        to string exactly, so the string comparison is precise.
        """
        metrics_fqn = self._app_sql.fqn("dq_metrics")
        rows = self._app_sql.query_dicts(
            f"SELECT 1 FROM {metrics_fqn} "  # noqa: S608
            f"WHERE run_id = '{escape_sql_string(run_id)}' "
            f"AND CAST(run_time AS STRING) <> '{escape_sql_string(target_iso)}' LIMIT 1"
        )
        return bool(rows)

    def _wait_for_metrics(self, run_id: str) -> bool:
        """Poll ``dq_metrics`` until at least one row exists for *run_id*; return whether it appeared.

        Closes the write-order gap between the terminal ``dq_validation_runs``
        row (which :meth:`_wait_for_run` gates on) and the later ``dq_metrics``
        write, guaranteeing every week's run rows are present before the re-date
        UPDATE runs. Returns ``True`` as soon as a row is seen, ``False`` if the
        bounded deadline elapses first (the caller logs that loudly).
        """
        metrics_fqn = self._app_sql.fqn("dq_metrics")
        deadline = time.monotonic() + _METRICS_TIMEOUT_SECONDS
        while True:
            rows = self._app_sql.query_dicts(
                f"SELECT run_id FROM {metrics_fqn} "  # noqa: S608
                f"WHERE run_id = '{escape_sql_string(run_id)}' LIMIT 1"
            )
            if rows:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(_METRICS_POLL_SECONDS)

    def _redate_history(self, scope_type: str, scope_key: str, target_iso: str) -> None:
        """Re-date the most recently appended ``dq_score_history`` row of a scope to *target_iso*."""
        history_fqn = self._oltp.fqn("dq_score_history")
        self._oltp.execute(redate.build_redate_latest_history_sql(history_fqn, scope_type, scope_key, target_iso))

    @staticmethod
    def _week_instant(now: datetime, week: int, weeks: int) -> datetime:
        """Return the (irregularly spaced) instant for *week*; the final week is ``now - 30min``.

        Reproduces dqlake's ``seed_demo.py`` weekly cadence EXACTLY (the
        ``GAP_DAYS`` / ``HOURS`` / ``week_ts`` block): irregular per-step day
        gaps, a varied hour-of-day, and a per-week minute offset, laid oldest-
        first from *now* going back. The final week (``week == weeks - 1``) is
        dqlake's ``final_instant`` — ``now - 30 minutes`` — not exactly *now*, so
        the newest point reads as a real recent run rather than a wall-clock
        artifact. Deterministic: every component is read from fixed lists (wrapped
        by modulo for any week count), never randomised.

        The cumulative back-step for *week* is ``sum(GAP_DAYS[j] for j in
        range(weeks - 1 - week))`` — the same ``days_ago`` dqlake builds by
        accumulating gaps oldest-first — so the points monotonically approach
        ``now``.

        Args:
            now: the shared "now" instant; the final week resolves to ``now - 30min``.
            week: the zero-based week index.
            weeks: the total number of weeks in the trend.

        Returns:
            The instant for *week*, strictly at or before *now*.
        """
        # dqlake's exact fixed cadence lists (seed_demo.py:1264-1265).
        gap_days = (11, 4, 9, 14, 6, 3, 12, 8, 5, 13, 7, 10)
        hours = (9, 14, 6, 19, 11, 2, 16, 22, 8, 13, 4, 20)
        if week >= weeks - 1:
            # dqlake's final_instant: the newest run is a single recent instant.
            return now - timedelta(minutes=30)
        # days_ago[week] = sum of the gaps between this week and the final week,
        # matching dqlake's oldest-first accumulation.
        days_ago = sum(gap_days[j % len(gap_days)] for j in range(weeks - 1 - week))
        return now - timedelta(days=days_ago, hours=hours[week % len(hours)], minutes=(week * 17) % 60)

    def _wait_for_run(self, run_id: str) -> str:
        """Poll ``dq_validation_runs`` until the run is terminal; return its final status.

        ``dq_validation_runs`` holds TWO rows per run_id: an app-written RUNNING
        placeholder inserted at submit time, and a runner-appended terminal row
        (``SUCCESS`` / ``FAILED`` / ``CANCELED``) once the Job finishes. The poll
        therefore asks directly for a TERMINAL row for this run_id — scanning ALL
        rows via a ``status IN (...)`` filter — rather than reading ``rows[0]`` of
        an unordered result set. Reading ``rows[0]`` (as an earlier version did)
        could latch onto the stale RUNNING placeholder even after the terminal
        row existed, timing out a run that had actually completed.

        A returned terminal row's status is returned (so a FAILED/CANCELED run is
        surfaced to the caller); an empty result means still-running and the loop
        keeps polling until the deadline.
        """
        runs_fqn = self._app_sql.fqn("dq_validation_runs")
        in_list = ", ".join(f"'{escape_sql_string(state)}'" for state in _TERMINAL_RUN_STATES)
        deadline = time.monotonic() + _RUN_TIMEOUT_SECONDS
        while True:
            rows = self._app_sql.query_dicts(
                f"SELECT status FROM {runs_fqn} "  # noqa: S608
                f"WHERE run_id = '{escape_sql_string(run_id)}' AND status IN ({in_list}) LIMIT 1"
            )
            status = rows[0].get("status") if rows else None
            if status in _TERMINAL_RUN_STATES:
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
