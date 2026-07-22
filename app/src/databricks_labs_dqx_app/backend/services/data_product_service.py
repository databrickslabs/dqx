"""Data Products service (Data Products Task 4).

Owns the ``dq_data_products`` / ``dq_data_product_members`` tables (design
spec §3.3/§3.4) and the review lifecycle on top of them (a Table Space
carries the SAME draft -> pending_approval -> approved/rejected lifecycle as
registry rules and monitored tables — P21 item 30):

- Any metadata edit or member add/remove flips the space back to ``draft``
  ("Modified since approval" display state) WITHOUT bumping ``version``.
- :meth:`submit` moves ``draft``/``rejected`` -> ``pending_approval``; an
  ``approved`` space (necessarily unchanged, since any edit above already
  flips it to ``draft``) is rejected with ``InvalidStatusTransitionError``
  (409) — mirrors the P20 registry-rule "no changes to submit" guard.
- :meth:`approve` is the ONLY operation that bumps ``version`` (``v+1``):
  ``pending_approval`` -> ``approved`` (409 otherwise).
- :meth:`reject` moves ``pending_approval`` -> ``rejected`` (409 otherwise).
- ``display_status``: ``approved`` -> ``"approved"``;
  ``pending_approval`` -> ``"pending_approval"``; ``rejected`` ->
  ``"rejected"``; ``draft`` with ``version > 0`` -> ``"modified"`` (has been
  approved before, edited since); otherwise -> ``"draft"``.
- Member upsert is by ``binding_id`` (a pin change on an existing member
  updates in place rather than duplicating a row).
- Name uniqueness is enforced app-side (ahead of the DB
  ``UNIQUE(name)`` constraint) so callers get a clean
  :class:`DuplicateDataProductNameError` instead of a raw SQL error.

Run fan-out (design spec §4.2) resolves every member's checks the same
way :class:`~.binding_run_service.BindingRunService` resolves a single
table, then submits each through it while sharing one minted
:class:`~.run_sets.RunSetService` run set — exactly the "one run set per
trigger" invariant Task 3 established. Per-member resolution/submission
failures are collected into ``skipped`` (collect-and-continue, mirroring
``routes/v1/dryrun.py:batch_run_from_catalog``'s per-table try/except)
rather than aborting the whole product run. This includes the case where
``BindingRunService.run_binding`` submits the job successfully but its own
run-set bookkeeping (``add_member`` against the run set we passed in)
raises — see the docstring on ``BindingRunService.run_binding`` — that
member is treated as failed here too, even though its job is live.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, cast, get_args
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import (
    SCHEDULE_KIND_DEFAULT,
    DataProduct,
    DataProductMember,
    MonitoredTable,
    RunSetSource,
    RunSetTrigger,
    ScheduleKind,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.services.score_cache_service import CachedScore, parse_cached_score
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

_UPDATABLE_FIELDS = ("name", "description", "steward", "schedule_cron", "schedule_tz")


class DuplicateDataProductNameError(ValueError):
    """Raised by :meth:`DataProductService.create`/:meth:`update` for a name already in use."""


class NoRunnableMembersError(ValueError):
    """Raised by :meth:`DataProductService.run` when zero members resolve to a runnable check set."""


class BindingNotApprovedError(ValueError):
    """Raised by :meth:`DataProductService.add_member` for a binding that is not approved.

    Only bindings satisfying :func:`_is_runnable` (status ``approved`` AND
    ``version > 0``) may JOIN a table space. Maps to HTTP 400 at the route.
    """


class InvalidStatusTransitionError(ValueError):
    """Raised by :meth:`DataProductService.approve`/:meth:`reject` when the space is not ``pending_approval``.

    Maps to HTTP 409 at the route — the same non-pending guard the monitored-table
    approve/reject routes enforce (the 557a486 lesson).
    """


@dataclass
class DataProductMemberDetail:
    """A ``dq_data_product_members`` row joined with its binding's live state.

    The ``score*`` fields carry the binding's cached table-scope DQ score
    (P5.3) — sourced from the monitored-table summaries the member build
    already fetches (which LEFT JOIN ``dq_score_cache`` in their own
    round-trip), so the Tables tab's score column costs no extra query.
    All ``None`` when the table has never been scored.
    """

    id: str
    binding_id: str
    table_fqn: str
    binding_status: str
    binding_version: int
    pinned_version: int | None
    rules_count: int
    checks_count: int
    runnable: bool
    score: float | None = None
    failed_tests: int | None = None
    total_tests: int | None = None
    score_computed_at: str | None = None


@dataclass
class DataProductDetail:
    """A ``dq_data_products`` row plus resolved members and list-view counters.

    The ``score*`` fields carry the cached DQ score LEFT-JOINed from
    ``dq_score_cache`` (P3.4) — all ``None`` when the product has never
    been scored. ``score_computed_at`` is the executor's ``ts_text`` string.
    """

    product: DataProduct
    members: list[DataProductMemberDetail] = field(default_factory=list)
    member_count: int = 0
    runnable_count: int = 0
    last_run_at: datetime | None = None
    score: float | None = None
    failed_tests: int | None = None
    total_tests: int | None = None
    score_computed_at: str | None = None


@dataclass
class DataProductRunSubmission:
    """One successfully submitted member run inside a product run fan-out."""

    binding_id: str
    table_fqn: str
    run_id: str
    job_run_id: int
    view_fqn: str
    binding_version: int | None


@dataclass
class DataProductRunResult:
    """Outcome of :meth:`DataProductService.run`."""

    run_set_id: str
    submitted: list[DataProductRunSubmission] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


@dataclass
class _MemberRow:
    id: str
    binding_id: str
    pinned_version: int | None


def display_status(product: DataProduct) -> str:
    """Compute the display status for *product*.

    ``approved`` -> ``"approved"``; ``pending_approval`` ->
    ``"pending_approval"``; ``rejected`` -> ``"rejected"``; ``draft`` with
    ``version > 0`` (has been approved before, edited since) ->
    ``"modified"``; otherwise (never approved) -> ``"draft"``.
    """
    if product.status in ("approved", "pending_approval", "rejected"):
        return product.status
    if product.version > 0:
        return "modified"
    return "draft"


def _is_runnable(binding_status: str, binding_version: int) -> bool:
    """Design spec Task 4 interface: runnable = binding approved AND version > 0."""
    return binding_status == "approved" and binding_version > 0


class DataProductService:
    """CRUD + publish + run fan-out for ``dq_data_products``."""

    def __init__(
        self,
        sql: OltpExecutorProtocol,
        monitored_tables: MonitoredTableService,
        run_set_service: RunSetService,
        binding_run_service: BindingRunService,
        version_service: MonitoredTableVersionService,
        app_settings: AppSettingsService,
        materializer: Materializer,
    ) -> None:
        self._sql = sql
        self._monitored_tables = monitored_tables
        self._run_set_service = run_set_service
        self._binding_run_service = binding_run_service
        self._version_service = version_service
        self._app_settings = app_settings
        self._materializer = materializer
        self._products_table = sql.fqn("dq_data_products")
        self._members_table = sql.fqn("dq_data_product_members")
        self._score_cache_table = sql.fqn("dq_score_cache")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def count(self) -> int:
        """Total table spaces (data products), any status (homepage stat card)."""
        rows = self._sql.query(f"SELECT COUNT(*) FROM {self._products_table}")  # noqa: S608
        return int(rows[0][0]) if rows and rows[0] and rows[0][0] is not None else 0

    def list_products(self) -> list[DataProductDetail]:
        """List every data product, newest-updated first, with resolved members.

        The cached DQ score columns are LEFT-JOINed from ``dq_score_cache``
        in the same round-trip (P3.4) — never recomputed here. Everything
        else the per-product detail needs is fetched in a BOUNDED number of
        batched queries, independent of product count: one for all products'
        members and (only when pins exist) one for the pinned frozen-snapshot
        counts. Never one-query-per-product. The per-product "last run" is
        derived from the already-fetched member ``last_run_at`` columns — no
        extra query.
        """
        scored = self._fetch_products_with_scores()
        if not scored:
            return []
        table_map = self._table_summary_map()
        product_ids = [product.product_id for product, _ in scored]
        members_by_product = self._fetch_members_by_product(product_ids)
        all_members = [m for members in members_by_product.values() for m in members]
        pinned_counts = self._pinned_snapshot_counts(all_members)
        live_check_counts = self._live_check_counts(all_members, table_map, pinned_counts)
        return [
            self._build_detail(
                product,
                table_map,
                cached,
                member_rows=members_by_product.get(product.product_id, []),
                pinned_counts=pinned_counts,
                live_check_counts=live_check_counts,
            )
            for product, cached in scored
        ]

    def get(self, product_id: str) -> DataProductDetail | None:
        """Get a single data product with resolved members, or None if it doesn't exist."""
        scored = self._fetch_product_with_score(product_id)
        if scored is None:
            return None
        product, cached = scored
        member_rows = self._fetch_members(product_id)
        table_map = self._table_summary_map()
        pinned_counts = self._pinned_snapshot_counts(member_rows)
        return self._build_detail(
            product,
            table_map,
            cached,
            member_rows=member_rows,
            pinned_counts=pinned_counts,
            live_check_counts=self._live_check_counts(member_rows, table_map, pinned_counts),
        )

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create(self, name: str, description: str | None, steward: str | None, created_by: str) -> DataProduct:
        """Create a new data product in ``draft`` status (no approver gate — design spec §3.3).

        Raises:
            ValueError: *name* is empty.
            DuplicateDataProductNameError: *name* is already in use.
        """
        if not name or not name.strip():
            raise ValueError("Data product name must not be empty.")
        self._assert_name_available(name, exclude_product_id=None)
        now = datetime.now(timezone.utc)
        product = DataProduct(
            product_id=uuid4().hex,
            name=name,
            description=description,
            steward=steward or created_by,
            schedule_cron=None,
            schedule_tz=None,
            status="draft",
            version=0,
            created_by=created_by,
            created_at=now,
            updated_by=created_by,
            updated_at=now,
        )
        self._sql.execute(
            f"INSERT INTO {self._products_table} "
            "(product_id, name, description, steward, schedule_cron, schedule_tz, schedule_kind, status, version, "
            "created_by, created_at, updated_by, updated_at) VALUES ("
            f"'{escape_sql_string(product.product_id)}', '{escape_sql_string(product.name)}', "
            f"{self._opt_str(product.description)}, {self._opt_str(product.steward)}, NULL, NULL, "
            f"{self._opt_str(product.schedule_kind)}, "
            f"'{product.status}', 0, {self._opt_str(created_by)}, now(), {self._opt_str(created_by)}, now())"
        )
        logger.info("Created data product %s (product_id=%s)", name, product.product_id)
        return product

    def update(self, product_id: str, updates: dict[str, Any], updated_by: str) -> DataProduct:
        """Apply a partial update to a data product.

        *updates* should only contain keys the caller explicitly supplied
        (e.g. via ``UpdateDataProductIn.model_dump(exclude_unset=True)``) so
        an omitted field is left untouched while an explicit ``None`` (e.g.
        clearing a schedule) is honored. ANY call flips the space back to
        ``draft`` without touching ``version`` (P21 item 30) — even a
        no-op save, matching the "editing = modified" semantics.

        Raises:
            LookupError: *product_id* does not exist.
            ValueError: an explicit ``name`` update is empty.
            DuplicateDataProductNameError: *name* is changed to one already in use.
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")

        if "name" in updates:
            new_name = updates["name"]
            if not new_name or not str(new_name).strip():
                raise ValueError("Data product name must not be empty.")
            if new_name != product.name:
                self._assert_name_available(new_name, exclude_product_id=product_id)

        set_clauses = ["status = 'draft'", f"updated_by = {self._opt_str(updated_by)}", "updated_at = now()"]
        for col in _UPDATABLE_FIELDS:
            if col in updates:
                set_clauses.append(f"{col} = {self._opt_str(updates[col])}")
        # schedule_kind (B2-52) is handled separately from _UPDATABLE_FIELDS
        # because its column is NOT NULL on Postgres: only a concrete, valid
        # kind is ever written, so an omitted/None value leaves the stored
        # value untouched rather than violating the constraint.
        apply_kind = updates.get("schedule_kind") in get_args(ScheduleKind)
        if apply_kind:
            set_clauses.append(f"schedule_kind = {self._opt_str(updates['schedule_kind'])}")
        e = escape_sql_string(product_id)
        self._sql.execute(f"UPDATE {self._products_table} SET {', '.join(set_clauses)} WHERE product_id = '{e}'")

        applied = {k: v for k, v in updates.items() if k in _UPDATABLE_FIELDS}
        if apply_kind:
            applied["schedule_kind"] = updates["schedule_kind"]
        return product.model_copy(
            update={**applied, "status": "draft", "updated_by": updated_by, "updated_at": datetime.now(timezone.utc)}
        )

    def delete(self, product_id: str) -> None:
        """Delete a data product and its members.

        Raises:
            LookupError: *product_id* does not exist.
        """
        e = escape_sql_string(product_id)
        rows = self._sql.query(f"SELECT product_id FROM {self._products_table} WHERE product_id = '{e}'")  # noqa: S608
        if not rows:
            raise LookupError(f"Data product not found: {product_id}")
        self._sql.execute(f"DELETE FROM {self._members_table} WHERE product_id = '{e}'")  # noqa: S608
        self._sql.execute(f"DELETE FROM {self._products_table} WHERE product_id = '{e}'")  # noqa: S608
        logger.info("Deleted data product %s", product_id)

    # ------------------------------------------------------------------
    # Members
    # ------------------------------------------------------------------

    def add_member(
        self, product_id: str, binding_id: str, pinned_version: int | None, updated_by: str
    ) -> DataProductMember:
        """Upsert a member by *binding_id* (a pin change updates the existing row in place).

        Flips the space back to ``draft`` (P21 item 30).

        On a brand-new member (no existing row for *binding_id*), the binding
        must be APPROVED — :func:`_is_runnable`'s predicate (status
        ``approved`` AND ``version > 0``), the same definition
        ``DataProductMemberDetail.runnable`` exposes. A binding whose approved
        rules carry unapproved edits ("modified" in the UI) still has
        ``status == "approved"`` underneath plus a frozen approved snapshot,
        so it stays eligible; draft / pending_approval / rejected /
        never-approved (``version == 0``) bindings are rejected. This is
        attach-time-only enforcement: the ``UPDATE`` branch (pin change on an
        EXISTING member) deliberately skips the check, so members whose
        binding later leaves ``approved`` are never retroactively evicted —
        the ``run()`` fan-out already skips them under ``source="approved"``.

        On a brand-new member, an unspecified *pinned_version* (``None``) is
        resolved against the ``default_auto_upgrade`` app-setting — see
        :meth:`~databricks_labs_dqx_app.backend.services.app_settings_service.AppSettingsService.resolve_pinned_version_for_new_attachment`.
        This is attach-time-only: re-adding an existing member (the
        ``UPDATE`` branch) always honours the caller's ``pinned_version``
        as-is, including an explicit ``None`` meaning "unpin / follow
        latest".

        Raises:
            LookupError: *product_id* does not exist.
            RuntimeError: *binding_id* does not exist (when adding a new member).
            BindingNotApprovedError: the binding is not approved (when adding
                a new member) — maps to HTTP 400 at the route.
        """
        if self._fetch_product(product_id) is None:
            raise LookupError(f"Data product not found: {product_id}")
        e_pid = escape_sql_string(product_id)
        e_bid = escape_sql_string(binding_id)
        rows = self._sql.query(
            f"SELECT id FROM {self._members_table} "  # noqa: S608
            f"WHERE product_id = '{e_pid}' AND binding_id = '{e_bid}'"
        )
        if rows:
            member_id = rows[0][0]
            self._sql.execute(
                f"UPDATE {self._members_table} SET pinned_version = {self._opt_int(pinned_version)} "
                f"WHERE id = '{escape_sql_string(member_id)}'"
            )
        else:
            # New member: the binding must exist AND be approved before it can
            # join the space (P3.2) — see the docstring for the exact predicate
            # and the deliberate no-retroactive-eviction asymmetry with the
            # UPDATE branch above.
            table = self._require_approved_binding(binding_id)
            member_id = uuid4().hex
            if pinned_version is None:
                pinned_version = self._app_settings.resolve_pinned_version_for_new_attachment(None, table.version)
            self._sql.execute(
                f"INSERT INTO {self._members_table} (id, product_id, binding_id, pinned_version) VALUES "
                f"('{escape_sql_string(member_id)}', '{e_pid}', '{e_bid}', {self._opt_int(pinned_version)})"
            )
        self._flip_to_draft(product_id, updated_by)
        return DataProductMember(
            id=member_id, product_id=product_id, binding_id=binding_id, pinned_version=pinned_version
        )

    def remove_member(self, product_id: str, member_id: str, updated_by: str) -> None:
        """Remove a member. Flips the space back to ``draft``.

        Raises:
            LookupError: *product_id* or *member_id* does not exist (or the
                member belongs to a different product).
        """
        if self._fetch_product(product_id) is None:
            raise LookupError(f"Data product not found: {product_id}")
        e_pid = escape_sql_string(product_id)
        e_mid = escape_sql_string(member_id)
        rows = self._sql.query(
            f"SELECT id FROM {self._members_table} WHERE id = '{e_mid}' AND product_id = '{e_pid}'"  # noqa: S608
        )
        if not rows:
            raise LookupError(f"Data product member not found: {member_id}")
        self._sql.execute(f"DELETE FROM {self._members_table} WHERE id = '{e_mid}'")  # noqa: S608
        self._flip_to_draft(product_id, updated_by)

    # ------------------------------------------------------------------
    # Review lifecycle (submit / approve / reject) — P21 item 30
    # ------------------------------------------------------------------

    def submit(self, product_id: str, updated_by: str) -> DataProduct:
        """Submit a Table Space for review: ``draft``/``rejected`` -> ``pending_approval``.

        Idempotent for an already-``pending_approval`` space (no-op re-submit).
        Does NOT bump ``version`` — only :meth:`approve` does.

        Rejects submitting an ``approved`` space (mirrors the P20 registry-rule
        guard in :meth:`RegistryService.submit`): :meth:`update`,
        :meth:`add_member`, and :meth:`remove_member` ALL flip the space to
        ``draft`` on ANY call — even a no-op save — so a space still sitting at
        ``approved`` has, by construction, zero unpublished changes. Without
        this guard a direct API call could submit an untouched approved space
        straight to ``pending_approval``, which is not itself runnable
        (:func:`_is_runnable` requires ``binding_status == "approved"``) —
        silently pausing that space's scheduled runs for no reason.

        Raises:
            LookupError: *product_id* does not exist.
            InvalidStatusTransitionError: the space is ``approved`` with no
                changes to submit (HTTP 409).
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")
        if product.status == "approved":
            raise InvalidStatusTransitionError(
                f"Cannot submit Table Space {product_id}: already approved with no changes to submit"
            )
        return self._set_status(product_id, "pending_approval", updated_by, _prefetched=product)

    def approve(self, product_id: str, updated_by: str) -> DataProduct:
        """Approve a Table Space: ``pending_approval`` -> ``approved``, bumping ``version`` by 1.

        The ONLY operation that bumps a space's version (mirrors monitored-table
        and registry-rule approval). Guards against approving a non-pending
        space out of band (the 557a486 lesson).

        Raises:
            LookupError: *product_id* does not exist.
            InvalidStatusTransitionError: the space is not ``pending_approval`` (HTTP 409).
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")
        if product.status != "pending_approval":
            raise InvalidStatusTransitionError(
                f"Cannot approve Table Space {product_id}: status is '{product.status}', expected 'pending_approval'"
            )
        new_version = product.version + 1
        e = escape_sql_string(product_id)
        self._sql.execute(
            f"UPDATE {self._products_table} SET status = 'approved', version = {new_version}, "
            f"updated_by = {self._opt_str(updated_by)}, updated_at = now() WHERE product_id = '{e}'"
        )
        logger.info("Approved data product %s at version %d", product_id, new_version)
        return product.model_copy(
            update={
                "status": "approved",
                "version": new_version,
                "updated_by": updated_by,
                "updated_at": datetime.now(timezone.utc),
            }
        )

    def reject(self, product_id: str, updated_by: str) -> DataProduct:
        """Reject a Table Space: ``pending_approval`` -> ``rejected``.

        Guards against rejecting a non-pending space out of band.

        Raises:
            LookupError: *product_id* does not exist.
            InvalidStatusTransitionError: the space is not ``pending_approval`` (HTTP 409).
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")
        if product.status != "pending_approval":
            raise InvalidStatusTransitionError(
                f"Cannot reject Table Space {product_id}: status is '{product.status}', expected 'pending_approval'"
            )
        return self._set_status(product_id, "rejected", updated_by, _prefetched=product)

    def revert(self, product_id: str, updated_by: str) -> DataProduct:
        """Withdraw a pending submission: ``pending_approval`` -> ``draft``.

        The counterpart to :meth:`submit` — an author pulls their own pending
        space back to keep editing, without a reject (the approver's decision,
        which leaves a ``rejected`` trail). Guards against reverting a
        non-pending space out of band.

        Raises:
            LookupError: *product_id* does not exist.
            InvalidStatusTransitionError: the space is not ``pending_approval`` (HTTP 409).
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")
        if product.status != "pending_approval":
            raise InvalidStatusTransitionError(
                f"Cannot revert Table Space {product_id}: status is '{product.status}', expected 'pending_approval'"
            )
        return self._set_status(product_id, "draft", updated_by, _prefetched=product)

    def _set_status(
        self, product_id: str, status: str, updated_by: str, _prefetched: DataProduct | None = None
    ) -> DataProduct:
        product = _prefetched or self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")
        e = escape_sql_string(product_id)
        self._sql.execute(
            f"UPDATE {self._products_table} SET status = '{status}', "
            f"updated_by = {self._opt_str(updated_by)}, updated_at = now() WHERE product_id = '{e}'"
        )
        logger.info("Set data product %s status to %s", product_id, status)
        return product.model_copy(
            update={"status": status, "updated_by": updated_by, "updated_at": datetime.now(timezone.utc)}
        )

    # ------------------------------------------------------------------
    # Run fan-out (design spec §4.2)
    # ------------------------------------------------------------------

    def run(
        self,
        product_id: str,
        source: RunSetSource,
        user_email: str,
        trigger: RunSetTrigger = "manual",
    ) -> DataProductRunResult:
        """Resolve every member's checks and submit through a shared run set.

        Resolution per member (design spec §4.2):
        - ``source == "draft"``: every member is submitted (draft render),
          including tables that have never been approved.
        - ``source == "approved"``: a pinned member resolves its pinned
          frozen snapshot; an unpinned member resolves its latest approved
          snapshot (requires ``binding.version > 0``); a member with no pin
          and no approved version is SKIPPED (never abort the whole run).

        Per-member submission failures (a pinned snapshot that no longer
        exists, a missing binding, a job-submission error, or the run-set
        bookkeeping failure documented on
        ``BindingRunService.run_binding``) are collected into ``skipped``
        rather than aborting the fan-out — mirroring
        ``routes/v1/dryrun.py:batch_run_from_catalog``'s per-table
        try/except.

        Raises:
            LookupError: *product_id* does not exist.
            NoRunnableMembersError: zero members resolve to a runnable
                check set (maps to 409 at the route).
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")

        member_rows = self._fetch_members(product_id)
        table_map = self._table_summary_map()

        to_run: list[tuple[_MemberRow, RunSetSource, int | None]] = []
        skipped: list[str] = []
        for row in member_rows:
            summary = table_map.get(row.binding_id)
            if summary is None:
                skipped.append(f"{row.binding_id}: monitored table binding no longer exists")
                continue
            table = summary.table
            if source == "draft":
                to_run.append((row, "draft", None))
            elif row.pinned_version is not None:
                to_run.append((row, "approved", row.pinned_version))
            elif table.version > 0:
                to_run.append((row, "approved", None))
            else:
                skipped.append(f"{table.table_fqn}: never approved")

        if not to_run:
            raise NoRunnableMembersError(f"Data product {product_id} has zero runnable members for source={source}")

        run_set_id = self._run_set_service.create(
            product_id=product_id,
            product_version=product.version,
            source=source,
            trigger=trigger,
            created_by=user_email,
        )

        submitted: list[DataProductRunSubmission] = []
        for row, resolved_source, resolved_version in to_run:
            table = table_map[row.binding_id].table
            try:
                result = self._binding_run_service.run_binding(
                    row.binding_id,
                    source=resolved_source,
                    version=resolved_version,
                    user_email=user_email,
                    trigger=trigger,
                    run_set_id=run_set_id,
                )
            except Exception as exc:  # collect-and-continue: one member's failure must not abort the fan-out
                logger.error(
                    "Failed to submit product %s member %s (%s): %s", product_id, row.binding_id, table.table_fqn, exc
                )
                skipped.append(f"{table.table_fqn}: {exc}")
                continue
            submitted.append(
                DataProductRunSubmission(
                    binding_id=row.binding_id,
                    table_fqn=table.table_fqn,
                    run_id=result.run_id,
                    job_run_id=result.job_run_id,
                    view_fqn=result.view_fqn,
                    binding_version=resolved_version if resolved_source == "approved" else None,
                )
            )

        if not submitted:
            # Every resolved member failed at submission time — the run set
            # was minted but never got a member. Best-effort cleanup
            # mirrors BindingRunService's own empty-run-set rollback.
            try:
                self._run_set_service.delete_empty(run_set_id)
            except Exception as cleanup_err:  # best-effort rollback; a stray empty run set is a lesser-severity gap
                logger.warning(
                    "Failed to roll back empty run set %s for product %s: %s", run_set_id, product_id, cleanup_err
                )

        return DataProductRunResult(run_set_id=run_set_id, submitted=submitted, skipped=skipped)

    def member_table_fqns(self, product_id: str) -> list[str]:
        """Return the distinct real source table FQNs of a product's members.

        Used by the scheduler's profiling fan-out (B2-52): a Table Space
        profiling run profiles each member table. Cross-table synthetic keys
        (``__sql_check__/<name>``) and members whose binding no longer exists
        are skipped — there is no physical table to profile. Order follows
        the members list; duplicates are collapsed.
        """
        member_rows = self._fetch_members(product_id)
        table_map = self._table_summary_map()
        seen: set[str] = set()
        fqns: list[str] = []
        for row in member_rows:
            summary = table_map.get(row.binding_id)
            if summary is None:
                continue
            fqn = summary.table.table_fqn
            if fqn and fqn not in seen and not fqn.startswith("__sql_check__/"):
                seen.add(fqn)
                fqns.append(fqn)
        return fqns

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_detail(
        self,
        product: DataProduct,
        table_map: dict[str, MonitoredTableSummary],
        cached_score: CachedScore | None,
        *,
        member_rows: list[_MemberRow],
        pinned_counts: dict[tuple[str, int], tuple[int, int]],
        live_check_counts: dict[str, int],
    ) -> DataProductDetail:
        """Assemble one product's detail from PRE-FETCHED batch data.

        Takes the member rows, pinned-snapshot-count map, and live check-count
        map the caller already fetched (batched across all products on the list
        path) so building N details issues zero additional queries. The
        product's ``last_run_at`` is derived here from the members' denormalized
        ``last_run_at`` — no separate query.
        """
        members: list[DataProductMemberDetail] = []
        member_last_runs: list[datetime] = []
        for row in member_rows:
            summary = table_map.get(row.binding_id)
            if summary is None:
                logger.warning(
                    "Data product %s member %s references missing binding %s",
                    product.product_id,
                    row.id,
                    row.binding_id,
                )
                continue
            table = summary.table
            if table.last_run_at is not None:
                member_last_runs.append(table.last_run_at)
            rules_count, checks_count = self._member_counts(
                summary, row.pinned_version, pinned_counts, live_check_counts
            )
            members.append(
                DataProductMemberDetail(
                    id=row.id,
                    binding_id=row.binding_id,
                    table_fqn=table.table_fqn,
                    binding_status=table.status,
                    binding_version=table.version,
                    pinned_version=row.pinned_version,
                    rules_count=rules_count,
                    checks_count=checks_count,
                    runnable=_is_runnable(table.status, table.version),
                    score=summary.score,
                    failed_tests=summary.failed_tests,
                    total_tests=summary.total_tests,
                    score_computed_at=summary.score_computed_at,
                )
            )
        cached = cached_score or CachedScore()
        return DataProductDetail(
            product=product,
            members=members,
            member_count=len(members),
            runnable_count=sum(1 for m in members if m.runnable),
            # Table space "last run" = the newest run across its member tables
            # (B2-15). Derived from the members' denormalized ``last_run_at``
            # (written on completion by MonitoredTableService.refresh_run_timestamps)
            # so a member run via EITHER trigger surface — MT-direct or this
            # space's fan-out — bumps it. Replaces the old product-id-grouped
            # run-set MAX, which missed MT-surface runs (product_id=None).
            last_run_at=max(member_last_runs) if member_last_runs else None,
            score=cached.score,
            failed_tests=cached.failed_tests,
            total_tests=cached.total_tests,
            score_computed_at=cached.computed_at,
        )

    @staticmethod
    def _member_counts(
        summary: MonitoredTableSummary,
        pinned_version: int | None,
        pinned_counts: dict[tuple[str, int], tuple[int, int]],
        live_check_counts: dict[str, int],
    ) -> tuple[int, int]:
        """Return ``(rules_count, checks_count)`` for a member.

        An UNPINNED member tracks the binding's latest approved state, so it
        reports the binding's applied-rule count and — for ``# Checks`` — the
        number of checks its applied rules actually expand to. That "live" check
        count comes from the SAME source as the monitored-tables overview
        (:meth:`_live_check_counts`): a rule expands to one check per mapping
        group (e.g. per column for a for-each-column rule), so a saved space
        shows the real non-zero count instead of ``summary.check_count`` — which
        counts ``dq_quality_rules`` rows that only exist after
        approval/materialization and is therefore ``0`` for a freshly-saved
        draft (P-item 44).

        A member PINNED to a specific version enforces that version's FROZEN
        snapshot, so it must report the snapshot's counts (its
        ``dq_monitored_table_versions.state_json`` reference count + cached
        ``check_count``) — resolved from the pre-fetched *pinned_counts* map (see
        :meth:`_pinned_snapshot_counts`), not the binding's current (possibly
        newer or emptied) live count, which would otherwise mislead the owner
        about what the pin actually enforces. Falls back to the live counts if
        the pinned snapshot can't be resolved (defensive: a pin should always
        have a matching frozen row).
        """
        if pinned_version is not None:
            snapshot = pinned_counts.get((summary.table.binding_id, pinned_version))
            if snapshot is not None:
                return snapshot
        rendered = live_check_counts.get(summary.table.binding_id)
        if rendered is not None:
            checks_count = rendered
        elif summary.applied_check_count is not None:
            checks_count = summary.applied_check_count
        else:
            checks_count = summary.check_count
        return summary.applied_rule_count, checks_count

    def _pinned_snapshot_counts(self, member_rows: list[_MemberRow]) -> dict[tuple[str, int], tuple[int, int]]:
        """Resolve every pinned member's frozen-snapshot counts in one batched query."""
        pins = [(row.binding_id, row.pinned_version) for row in member_rows if row.pinned_version is not None]
        if not pins:
            return {}
        return self._version_service.snapshot_counts_many(pins)

    def _live_check_counts(
        self,
        member_rows: list[_MemberRow],
        table_map: dict[str, MonitoredTableSummary],
        pinned_counts: dict[tuple[str, int], tuple[int, int]],
    ) -> dict[str, int]:
        """Batched ``# Checks`` for every UNPINNED member, from its applied rules' expansion.

        ``# Checks`` must be the number of DQ checks a member's applied rules
        actually produce — one per mapping group, so a for-each-column rule
        expands to several checks. ``MonitoredTableSummary.check_count`` counts
        materialized ``dq_quality_rules`` rows, which only exist after
        approval/run, so a freshly-saved DRAFT space reported ``0`` (P-item 44).

        Mirrors the monitored-tables overview's
        ``routes/v1/monitored_tables.py:_apply_snapshot_check_counts`` live-render
        branch: resolves the render count for ALL relevant bindings in ONE
        batched :meth:`Materializer.render_binding_checks_counts_many` call
        (query-bounded regardless of member count). Only UNPINNED members need
        it — a pinned member reports its frozen snapshot's cached count via
        :meth:`_pinned_snapshot_counts`. A binding whose applied rules all fail
        to resolve counts ``0``; on any materialization error every member
        falls back to the live summary count.
        """
        live_bindings: list[tuple[str, str]] = []
        seen: set[str] = set()
        for row in member_rows:
            if row.pinned_version is not None and pinned_counts.get((row.binding_id, row.pinned_version)) is not None:
                continue
            if row.binding_id in seen:
                continue
            summary = table_map.get(row.binding_id)
            if summary is None:
                continue
            # Approved bindings (version > 0) read their frozen snapshot count from
            # the summary (applied_check_count) — no render. Only never-approved
            # drafts (version == 0) need a live render (item 44). Mirrors the Tables
            # overview's _apply_snapshot_check_counts split.
            if summary.table.version > 0:
                continue
            seen.add(row.binding_id)
            live_bindings.append((row.binding_id, summary.table.table_fqn))
        if not live_bindings:
            return {}
        try:
            return self._materializer.render_binding_checks_counts_many(live_bindings)
        except MaterializationError:
            return {}

    def _table_summary_map(self) -> dict[str, MonitoredTableSummary]:
        summaries = self._monitored_tables.list_monitored_tables()
        return {s.table.binding_id: s for s in summaries}

    def _assert_name_available(self, name: str, exclude_product_id: str | None) -> None:
        e = escape_sql_string(name)
        rows = self._sql.query(f"SELECT product_id FROM {self._products_table} WHERE name = '{e}'")  # noqa: S608
        for row in rows:
            if exclude_product_id is None or row[0] != exclude_product_id:
                raise DuplicateDataProductNameError(f"A data product named '{name}' already exists.")

    def _flip_to_draft(self, product_id: str, updated_by: str) -> None:
        e = escape_sql_string(product_id)
        self._sql.execute(
            f"UPDATE {self._products_table} SET status = 'draft', "
            f"updated_by = {self._opt_str(updated_by)}, updated_at = now() WHERE product_id = '{e}'"
        )

    def _fetch_members(self, product_id: str) -> list[_MemberRow]:
        e = escape_sql_string(product_id)
        rows = self._sql.query(
            f"SELECT id, binding_id, pinned_version FROM {self._members_table} WHERE product_id = '{e}'"  # noqa: S608
        )
        return [_MemberRow(id=row[0], binding_id=row[1], pinned_version=self._parse_int(row[2])) for row in rows]

    def _fetch_members_by_product(self, product_ids: list[str]) -> dict[str, list[_MemberRow]]:
        """Fetch ALL listed products' members in ONE query, grouped app-side.

        Batched counterpart of :meth:`_fetch_members` for the list path — one
        ``IN (...)`` round-trip instead of one query per product.
        """
        if not product_ids:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(p)}'" for p in product_ids)
        rows = self._sql.query(
            f"SELECT id, product_id, binding_id, pinned_version FROM {self._members_table} "  # noqa: S608
            f"WHERE product_id IN ({in_list})"
        )
        result: dict[str, list[_MemberRow]] = {}
        for row in rows:
            result.setdefault(row[1], []).append(
                _MemberRow(id=row[0], binding_id=row[2], pinned_version=self._parse_int(row[3]))
            )
        return result

    def _select_cols(self, prefix: str = "") -> str:
        created_at = self._sql.ts_text(f"{prefix}created_at")
        updated_at = self._sql.ts_text(f"{prefix}updated_at")
        return (
            f"{prefix}product_id, {prefix}name, {prefix}description, {prefix}steward, "
            f"{prefix}schedule_cron, {prefix}schedule_tz, {prefix}status, {prefix}version, "
            f"{prefix}created_by, {created_at} AS created_at, "
            f"{prefix}updated_by, {updated_at} AS updated_at, "
            # schedule_kind (B2-52) appended last so the score-join columns keep
            # their relative offset (score cols follow at +1..+4).
            f"{prefix}schedule_kind"
        )

    def _require_approved_binding(self, binding_id: str) -> MonitoredTable:
        """Validate that a monitored table binding exists and is approved.

        "Approved" is :func:`_is_runnable`'s predicate — status ``approved``
        AND ``version > 0`` — NOT the UI display status: a binding shown as
        "modified" (approved with unapproved edits) is still ``approved``
        underneath and passes.

        Returns:
            The binding's :class:`MonitoredTable` row.

        Raises:
            RuntimeError: *binding_id* does not exist.
            BindingNotApprovedError: the binding is not approved.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise RuntimeError(f"Monitored table not found: {binding_id}")
        table = detail.table
        if not _is_runnable(table.status, table.version):
            reason = (
                "it has never been approved"
                if table.version == 0
                else f"its status is '{table.status}', expected 'approved'"
            )
            raise BindingNotApprovedError(
                f"Cannot add table '{table.table_fqn}' to this table space: {reason}. "
                "Only approved tables can join a table space."
            )
        return table

    def _fetch_product(self, product_id: str) -> DataProduct | None:
        e = escape_sql_string(product_id)
        sql = f"SELECT {self._select_cols()} FROM {self._products_table} WHERE product_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_product(rows[0])

    def _score_joined_select(self) -> str:
        """SELECT + FROM + LEFT JOIN fragment for the score-carrying read paths."""
        score_computed_at = self._sql.ts_text("sc.computed_at")
        return (
            f"SELECT {self._select_cols('p.')}, "
            f"sc.score, sc.failed_tests, sc.total_tests, {score_computed_at} AS score_computed_at "
            f"FROM {self._products_table} p "
            f"LEFT JOIN {self._score_cache_table} sc "
            f"ON sc.scope_type = 'product' AND sc.scope_key = p.product_id"
        )

    def _fetch_product_with_score(self, product_id: str) -> tuple[DataProduct, CachedScore] | None:
        e = escape_sql_string(product_id)
        sql = f"{self._score_joined_select()} WHERE p.product_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        row = rows[0]
        return self._row_to_product(row), parse_cached_score(row[13], row[14], row[15], row[16])

    def _fetch_products_with_scores(self) -> list[tuple[DataProduct, CachedScore]]:
        sql = f"{self._score_joined_select()} ORDER BY p.updated_at DESC"  # noqa: S608
        rows = self._sql.query(sql)
        return [(self._row_to_product(row), parse_cached_score(row[13], row[14], row[15], row[16])) for row in rows]

    def _row_to_product(self, row: list[str]) -> DataProduct:
        return DataProduct(
            product_id=row[0],
            name=row[1],
            description=row[2],
            steward=row[3],
            schedule_cron=row[4],
            schedule_tz=row[5],
            status=row[6] if row[6] in ("pending_approval", "approved", "rejected") else "draft",
            version=self._parse_int(row[7]) or 0,
            created_by=row[8],
            created_at=self._parse_timestamp(row[9]),
            updated_by=row[10],
            updated_at=self._parse_timestamp(row[11]),
            schedule_kind=(
                cast(ScheduleKind, row[12])
                if len(row) > 12 and row[12] in get_args(ScheduleKind)
                else SCHEDULE_KIND_DEFAULT
            ),
        )

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

    @staticmethod
    def _opt_int(value: int | None) -> str:
        return str(int(value)) if value is not None else "NULL"

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        return int(value) if value not in (None, "") else None

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace(" ", "T"))
        except ValueError:
            return None
