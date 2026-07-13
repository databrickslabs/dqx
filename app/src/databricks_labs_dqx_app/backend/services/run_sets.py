"""Run-set tracking service (Data Products Task 3).

Every run submission (single monitored table, data product, scheduled
product) mints a ``dq_run_sets`` row plus one ``dq_run_set_members`` row
per submitted table (design spec §3.5). This service owns the read/write
surface over those two OLTP tables and joins them (in Python — the two
halves of a run set span the OLTP backend and ``dq_validation_runs``,
which always lives in Delta, so a cross-backend SQL JOIN isn't possible)
against ``dq_validation_runs`` to derive per-member and aggregated run
status. It never writes to ``dq_validation_runs`` — that table stays
owned by :class:`~databricks_labs_dqx_app.backend.services.job_service.JobService`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast, get_args
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import RunSetSource, RunSetTrigger
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

# Aggregation precedence (design spec §4.2 / plan Task 3): running beats
# failed beats canceled beats success. Statuses are matched case-insensitively
# against ``dq_validation_runs.status`` (RUNNING/FAILED/CANCELED/SUCCESS).
_RUNNING = "RUNNING"
_FAILED = "FAILED"
_CANCELED = "CANCELED"
_SUCCESS = "success"


@dataclass
class RunSetMemberDetail:
    """A run-set member joined with its ``dq_validation_runs`` row (if any)."""

    run_id: str
    binding_id: str
    binding_version: int | None = None
    table_fqn: str | None = None
    status: str | None = None
    total_rows: int | None = None
    valid_rows: int | None = None
    invalid_rows: int | None = None
    error_rows: int | None = None
    warning_rows: int | None = None


@dataclass
class RunSetSummary:
    """A ``dq_run_sets`` row plus member count and aggregated status."""

    run_set_id: str
    source: RunSetSource
    trigger: RunSetTrigger
    member_count: int
    status: str
    product_id: str | None = None
    product_version: int | None = None
    created_by: str | None = None
    created_at: datetime | None = None


@dataclass
class RunSetDetail:
    """A ``dq_run_sets`` row plus its resolved members."""

    run_set_id: str
    source: RunSetSource
    trigger: RunSetTrigger
    status: str
    product_id: str | None = None
    product_version: int | None = None
    created_by: str | None = None
    created_at: datetime | None = None
    members: list[RunSetMemberDetail] = field(default_factory=list)


@dataclass
class _MemberRow:
    run_id: str
    binding_id: str
    binding_version: int | None


_RUN_SET_SOURCES: frozenset[str] = frozenset(get_args(RunSetSource))
_RUN_SET_TRIGGERS: frozenset[str] = frozenset(get_args(RunSetTrigger))


class RunSetService:
    """Mints and reads run sets (``dq_run_sets`` / ``dq_run_set_members``).

    ``oltp_sql`` is the OLTP executor (Lakebase or Delta-OLTP-fallback)
    that owns the two run-set tables; ``validation_sql`` is the Delta
    executor for ``dq_validation_runs`` (always Delta, regardless of
    whether Lakebase is enabled — see ``app/CLAUDE.md``).
    """

    def __init__(self, oltp_sql: OltpExecutorProtocol, validation_sql: SqlExecutor) -> None:
        self._sql = oltp_sql
        self._validation_sql = validation_sql
        self._run_sets_table = oltp_sql.fqn("dq_run_sets")
        self._members_table = oltp_sql.fqn("dq_run_set_members")
        self._validation_runs_table = validation_sql.fqn("dq_validation_runs")

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def create(
        self,
        product_id: str | None,
        product_version: int | None,
        source: RunSetSource,
        trigger: RunSetTrigger,
        created_by: str | None,
    ) -> str:
        """Mint a new run set and return its id."""
        run_set_id = uuid4().hex
        trigger_col = self._sql.q("trigger")
        self._sql.execute(
            f"INSERT INTO {self._run_sets_table} "
            f"(run_set_id, product_id, product_version, source, {trigger_col}, created_by, created_at) VALUES "
            f"('{escape_sql_string(run_set_id)}', {self._opt_str(product_id)}, "
            f"{self._opt_int(product_version)}, '{escape_sql_string(source)}', "
            f"'{escape_sql_string(trigger)}', {self._opt_str(created_by)}, now())"
        )
        logger.info("Created run set %s (product_id=%s, source=%s, trigger=%s)", run_set_id, product_id, source, trigger)
        return run_set_id

    def add_member(self, run_set_id: str, run_id: str, binding_id: str, binding_version: int | None) -> None:
        """Record a submitted table run as a member of *run_set_id*."""
        member_id = uuid4().hex
        self._sql.execute(
            f"INSERT INTO {self._members_table} (id, run_set_id, run_id, binding_id, binding_version) VALUES "
            f"('{escape_sql_string(member_id)}', '{escape_sql_string(run_set_id)}', "
            f"'{escape_sql_string(run_id)}', '{escape_sql_string(binding_id)}', {self._opt_int(binding_version)})"
        )

    def delete_empty(self, run_set_id: str) -> None:
        """Best-effort rollback of a just-minted run set that never got a member.

        Callers use this when :meth:`create` succeeded but the subsequent
        :meth:`add_member` failed, to avoid leaving a dangling ``dq_run_sets``
        row with zero members (which would otherwise aggregate as a
        vacuous "success" with no members — see ``BindingRunService``).
        Deliberately scoped to *run_set_id* only (no member-count guard):
        callers must only invoke this for a run set they just minted and
        know has no members.
        """
        e = escape_sql_string(run_set_id)
        self._sql.execute(f"DELETE FROM {self._run_sets_table} WHERE run_set_id = '{e}'")  # noqa: S608

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_for_product(self, product_id: str, limit: int = 50) -> list[RunSetSummary]:
        """Return the run sets triggered for *product_id*, newest first."""
        e = escape_sql_string(product_id)
        created_at = self._sql.ts_text("created_at")
        trigger_col = self._sql.q("trigger")
        sql = (
            f"SELECT run_set_id, product_id, product_version, source, {trigger_col} AS trigger_value, "  # noqa: S608
            f"created_by, {created_at} AS created_at "
            f"FROM {self._run_sets_table} WHERE product_id = '{e}' ORDER BY created_at DESC LIMIT {int(limit)}"
        )
        rows = self._sql.query(sql)
        run_set_ids = [row[0] for row in rows]
        members_by_set = self._fetch_members(run_set_ids)
        all_run_ids = [m.run_id for members in members_by_set.values() for m in members]
        validation_map = self._fetch_validation_rows(all_run_ids)

        summaries: list[RunSetSummary] = []
        for row in rows:
            run_set_id = row[0]
            members = members_by_set.get(run_set_id, [])
            statuses = [validation_map.get(m.run_id, {}).get("status") for m in members]
            summaries.append(
                RunSetSummary(
                    run_set_id=run_set_id,
                    product_id=row[1],
                    product_version=self._parse_int(row[2]),
                    source=self._parse_source(row[3]),
                    trigger=self._parse_trigger(row[4]),
                    created_by=row[5],
                    created_at=self._parse_timestamp(row[6]),
                    member_count=len(members),
                    status=self._aggregate_status(statuses),
                )
            )
        return summaries

    def get(self, run_set_id: str) -> RunSetDetail:
        """Return a run set plus its resolved members.

        Raises:
            LookupError: no ``dq_run_sets`` row exists for *run_set_id*.
        """
        e = escape_sql_string(run_set_id)
        created_at = self._sql.ts_text("created_at")
        trigger_col = self._sql.q("trigger")
        sql = (
            f"SELECT run_set_id, product_id, product_version, source, {trigger_col} AS trigger_value, "  # noqa: S608
            f"created_by, {created_at} AS created_at "
            f"FROM {self._run_sets_table} WHERE run_set_id = '{e}'"
        )
        rows = self._sql.query(sql)
        if not rows:
            raise LookupError(f"Run set not found: {run_set_id}")
        row = rows[0]

        members = self._fetch_members([run_set_id]).get(run_set_id, [])
        validation_map = self._fetch_validation_rows([m.run_id for m in members])

        member_details: list[RunSetMemberDetail] = []
        for m in members:
            vrow = validation_map.get(m.run_id, {})
            member_details.append(
                RunSetMemberDetail(
                    run_id=m.run_id,
                    binding_id=m.binding_id,
                    binding_version=m.binding_version,
                    table_fqn=vrow.get("source_table_fqn"),
                    status=vrow.get("status"),
                    total_rows=self._parse_int(vrow.get("total_rows")),
                    valid_rows=self._parse_int(vrow.get("valid_rows")),
                    invalid_rows=self._parse_int(vrow.get("invalid_rows")),
                    error_rows=self._parse_int(vrow.get("error_rows")),
                    warning_rows=self._parse_int(vrow.get("warning_rows")),
                )
            )

        return RunSetDetail(
            run_set_id=row[0],
            product_id=row[1],
            product_version=self._parse_int(row[2]),
            source=self._parse_source(row[3]),
            trigger=self._parse_trigger(row[4]),
            created_by=row[5],
            created_at=self._parse_timestamp(row[6]),
            status=self._aggregate_status([m.status for m in member_details]),
            members=member_details,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_members(self, run_set_ids: list[str]) -> dict[str, list[_MemberRow]]:
        if not run_set_ids:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(r)}'" for r in run_set_ids)
        sql = (
            f"SELECT run_set_id, run_id, binding_id, binding_version FROM {self._members_table} "  # noqa: S608
            f"WHERE run_set_id IN ({in_list})"
        )
        rows = self._sql.query(sql)
        result: dict[str, list[_MemberRow]] = {}
        for row in rows:
            result.setdefault(row[0], []).append(
                _MemberRow(run_id=row[1], binding_id=row[2], binding_version=self._parse_int(row[3]))
            )
        return result

    def _fetch_validation_rows(self, run_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Return the latest ``dq_validation_runs`` row per *run_ids*, keyed by run_id.

        Deduplicates the same way :meth:`JobService._list_deduplicated_rows`
        does: a RUNNING placeholder and a later terminal row can coexist for
        one ``run_id`` while a job is in flight, so we prefer the terminal
        row when both are present.
        """
        if not run_ids:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(r)}'" for r in run_ids)
        sql = (
            "SELECT run_id, source_table_fqn, status, total_rows, valid_rows, "  # noqa: S608
            "invalid_rows, error_rows, warning_rows FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY run_id "
            "    ORDER BY CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END ASC, created_at DESC"
            "  ) AS rn "
            f"  FROM {self._validation_runs_table} WHERE run_id IN ({in_list})"
            ") WHERE rn = 1"
        )
        rows = self._validation_sql.query_dicts(sql)
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            run_id = row.get("run_id")
            if run_id:
                result[run_id] = cast(dict[str, Any], row)
        return result

    @staticmethod
    def _aggregate_status(statuses: list[str | None]) -> str:
        upper = {s.upper() for s in statuses if s}
        if _RUNNING in upper:
            return "running"
        if _FAILED in upper:
            return "failed"
        if _CANCELED in upper:
            return "canceled"
        return _SUCCESS

    @staticmethod
    def _parse_source(value: str) -> RunSetSource:
        if value not in _RUN_SET_SOURCES:
            raise ValueError(f"Invalid run set source {value!r}; expected one of {sorted(_RUN_SET_SOURCES)}")
        return cast(RunSetSource, value)

    @staticmethod
    def _parse_trigger(value: str) -> RunSetTrigger:
        if value not in _RUN_SET_TRIGGERS:
            raise ValueError(f"Invalid run set trigger {value!r}; expected one of {sorted(_RUN_SET_TRIGGERS)}")
        return cast(RunSetTrigger, value)

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
