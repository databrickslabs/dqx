"""Data Products service (Data Products Task 4).

Owns the ``dq_data_products`` / ``dq_data_product_members`` tables (design
spec §3.3/§3.4) and the dqlake-exact lifecycle semantics on top of them:

- Any metadata edit or member add/remove flips ``published`` -> ``draft``
  ("Modified since publish" display state) WITHOUT bumping ``version``.
- :meth:`publish` is the ONLY operation that bumps ``version`` (``v+1``)
  and sets ``status='published'``.
- ``display_status`` (dqlake logic): ``published`` -> ``"published"``;
  ``draft`` with ``version > 0`` -> ``"modified"`` (has been published
  before, edited since); otherwise -> ``"draft"``.
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
from typing import Any
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import (
    DataProduct,
    DataProductMember,
    RunSetSource,
    RunSetTrigger,
)
from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService, MonitoredTableSummary
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

_UPDATABLE_FIELDS = ("name", "description", "steward", "schedule_cron", "schedule_tz")


class DuplicateDataProductNameError(ValueError):
    """Raised by :meth:`DataProductService.create`/:meth:`update` for a name already in use."""


class NoRunnableMembersError(ValueError):
    """Raised by :meth:`DataProductService.run` when zero members resolve to a runnable check set."""


@dataclass
class DataProductMemberDetail:
    """A ``dq_data_product_members`` row joined with its binding's live state."""

    id: str
    binding_id: str
    table_fqn: str
    binding_status: str
    binding_version: int
    pinned_version: int | None
    rules_count: int
    checks_count: int
    runnable: bool


@dataclass
class DataProductDetail:
    """A ``dq_data_products`` row plus resolved members and list-view counters."""

    product: DataProduct
    members: list[DataProductMemberDetail] = field(default_factory=list)
    member_count: int = 0
    runnable_count: int = 0
    last_run_at: datetime | None = None


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
    """Compute the dqlake-style display status for *product*.

    ``published`` -> ``"published"``; ``draft`` with ``version > 0``
    (has been published before, edited since) -> ``"modified"``;
    otherwise (never published) -> ``"draft"``.
    """
    if product.status == "published":
        return "published"
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
    ) -> None:
        self._sql = sql
        self._monitored_tables = monitored_tables
        self._run_set_service = run_set_service
        self._binding_run_service = binding_run_service
        self._version_service = version_service
        self._products_table = sql.fqn("dq_data_products")
        self._members_table = sql.fqn("dq_data_product_members")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_products(self) -> list[DataProductDetail]:
        """List every data product, newest-updated first, with resolved members."""
        products = self._fetch_products()
        if not products:
            return []
        table_map = self._table_summary_map()
        return [self._build_detail(p, table_map) for p in products]

    def get(self, product_id: str) -> DataProductDetail | None:
        """Get a single data product with resolved members, or None if it doesn't exist."""
        product = self._fetch_product(product_id)
        if product is None:
            return None
        return self._build_detail(product, self._table_summary_map())

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
            "(product_id, name, description, steward, schedule_cron, schedule_tz, status, version, "
            "created_by, created_at, updated_by, updated_at) VALUES ("
            f"'{escape_sql_string(product.product_id)}', '{escape_sql_string(product.name)}', "
            f"{self._opt_str(product.description)}, {self._opt_str(product.steward)}, NULL, NULL, "
            f"'{product.status}', 0, {self._opt_str(created_by)}, now(), {self._opt_str(created_by)}, now())"
        )
        logger.info("Created data product %s (product_id=%s)", name, product.product_id)
        return product

    def update(self, product_id: str, updates: dict[str, Any], updated_by: str) -> DataProduct:
        """Apply a partial update to a data product.

        *updates* should only contain keys the caller explicitly supplied
        (e.g. via ``UpdateDataProductIn.model_dump(exclude_unset=True)``) so
        an omitted field is left untouched while an explicit ``None`` (e.g.
        clearing a schedule) is honored. ANY call flips ``published`` ->
        ``draft`` without touching ``version`` (design spec §3.3) — even a
        no-op save, matching dqlake's "editing = modified" semantics.

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
        e = escape_sql_string(product_id)
        self._sql.execute(f"UPDATE {self._products_table} SET {', '.join(set_clauses)} WHERE product_id = '{e}'")

        applied = {k: v for k, v in updates.items() if k in _UPDATABLE_FIELDS}
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

    def add_member(self, product_id: str, binding_id: str, pinned_version: int | None, updated_by: str) -> DataProductMember:
        """Upsert a member by *binding_id* (a pin change updates the existing row in place).

        Flips the product ``published`` -> ``draft`` (design spec §3.3).

        Raises:
            LookupError: *product_id* does not exist.
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
            member_id = uuid4().hex
            self._sql.execute(
                f"INSERT INTO {self._members_table} (id, product_id, binding_id, pinned_version) VALUES "
                f"('{escape_sql_string(member_id)}', '{e_pid}', '{e_bid}', {self._opt_int(pinned_version)})"
            )
        self._flip_to_draft(product_id, updated_by)
        return DataProductMember(id=member_id, product_id=product_id, binding_id=binding_id, pinned_version=pinned_version)

    def remove_member(self, product_id: str, member_id: str, updated_by: str) -> None:
        """Remove a member. Flips the product ``published`` -> ``draft``.

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
    # Publish
    # ------------------------------------------------------------------

    def publish(self, product_id: str, updated_by: str) -> DataProduct:
        """Bump ``version`` by 1 and set ``status='published'`` (design spec §3.3).

        The ONLY operation that bumps a product's version.

        Raises:
            LookupError: *product_id* does not exist.
        """
        product = self._fetch_product(product_id)
        if product is None:
            raise LookupError(f"Data product not found: {product_id}")
        new_version = product.version + 1
        e = escape_sql_string(product_id)
        self._sql.execute(
            f"UPDATE {self._products_table} SET status = 'published', version = {new_version}, "
            f"updated_by = {self._opt_str(updated_by)}, updated_at = now() WHERE product_id = '{e}'"
        )
        logger.info("Published data product %s at version %d", product_id, new_version)
        return product.model_copy(
            update={
                "status": "published",
                "version": new_version,
                "updated_by": updated_by,
                "updated_at": datetime.now(timezone.utc),
            }
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
            raise NoRunnableMembersError(
                f"Data product {product_id} has zero runnable members for source={source}"
            )

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
                logger.warning("Failed to roll back empty run set %s for product %s: %s", run_set_id, product_id, cleanup_err)

        return DataProductRunResult(run_set_id=run_set_id, submitted=submitted, skipped=skipped)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_detail(self, product: DataProduct, table_map: dict[str, MonitoredTableSummary]) -> DataProductDetail:
        member_rows = self._fetch_members(product.product_id)
        members: list[DataProductMemberDetail] = []
        for row in member_rows:
            summary = table_map.get(row.binding_id)
            if summary is None:
                logger.warning(
                    "Data product %s member %s references missing binding %s", product.product_id, row.id, row.binding_id
                )
                continue
            table = summary.table
            rules_count, checks_count = self._member_counts(summary, row.pinned_version)
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
                )
            )
        last_run_at = self._last_run_at(product.product_id)
        return DataProductDetail(
            product=product,
            members=members,
            member_count=len(members),
            runnable_count=sum(1 for m in members if m.runnable),
            last_run_at=last_run_at,
        )

    def _member_counts(self, summary: MonitoredTableSummary, pinned_version: int | None) -> tuple[int, int]:
        """Return ``(rules_count, checks_count)`` for a member.

        An UNPINNED member tracks the binding's latest approved state, so it
        reports the live summary counts. A member PINNED to a specific version
        enforces that version's FROZEN snapshot, so it must report the
        snapshot's counts (``dq_monitored_table_versions.state_json`` /
        ``checks_json`` lengths) — not the binding's current (possibly newer or
        emptied) live count, which would otherwise mislead the owner about what
        the pin actually enforces. Falls back to the live counts if the pinned
        snapshot can't be resolved (defensive: a pin should always have a
        matching frozen row).
        """
        if pinned_version is not None:
            snapshot = self._version_service.snapshot_counts(summary.table.binding_id, pinned_version)
            if snapshot is not None:
                return snapshot
        return summary.applied_rule_count, summary.check_count

    def _last_run_at(self, product_id: str) -> datetime | None:
        run_sets = self._run_set_service.list_for_product(product_id, limit=1)
        return run_sets[0].created_at if run_sets else None

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
        return [
            _MemberRow(id=row[0], binding_id=row[1], pinned_version=self._parse_int(row[2])) for row in rows
        ]

    def _select_cols(self) -> str:
        created_at = self._sql.ts_text("created_at")
        updated_at = self._sql.ts_text("updated_at")
        return (
            "product_id, name, description, steward, schedule_cron, schedule_tz, status, version, "
            f"created_by, {created_at} AS created_at, updated_by, {updated_at} AS updated_at"
        )

    def _fetch_product(self, product_id: str) -> DataProduct | None:
        e = escape_sql_string(product_id)
        sql = f"SELECT {self._select_cols()} FROM {self._products_table} WHERE product_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_product(rows[0])

    def _fetch_products(self) -> list[DataProduct]:
        sql = f"SELECT {self._select_cols()} FROM {self._products_table} ORDER BY updated_at DESC"  # noqa: S608
        rows = self._sql.query(sql)
        return [self._row_to_product(row) for row in rows]

    def _row_to_product(self, row: list[str]) -> DataProduct:
        return DataProduct(
            product_id=row[0],
            name=row[1],
            description=row[2],
            steward=row[3],
            schedule_cron=row[4],
            schedule_tz=row[5],
            status="published" if row[6] == "published" else "draft",
            version=self._parse_int(row[7]) or 0,
            created_by=row[8],
            created_at=self._parse_timestamp(row[9]),
            updated_by=row[10],
            updated_at=self._parse_timestamp(row[11]),
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
