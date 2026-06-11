"""Per-run review-status tracking for DQX validation runs.

A reviewer (any authenticated user) tags a validation run with one of the
admin-configured values from ``dq_app_settings.run_review_statuses_v1`` —
e.g. ``Pending review`` (auto-default) → ``Acknowledged`` once business
owners have triaged the failures. The Runs detail page renders the
current status next to the comments thread, and the Runs History page
exposes a multi-select filter on it.

Storage layout
--------------
- ``dq_run_review_status`` — one mutable row per run that has been
  explicitly reviewed. Runs without a row are *not* unreviewed — they
  surface the configured default ``value`` from ``AppSettingsService``
  virtually, so dashboards and filters never see a NULL state.
- ``dq_run_review_status_history`` — append-only audit log. One row per
  explicit change (including the very first time someone moves a run
  off the virtual default). ``previous_status`` carries the *effective*
  value before the change, which is what reviewers care about reading
  even though it may have been virtual.

Both tables live in the OLTP executor — Lakebase Postgres when enabled,
Delta fallback otherwise — and share the same service surface as
``CommentsService`` / ``AppSettingsService``. The validation-run rows
themselves stay in Delta because they're append-only Spark output;
listing routes bulk-fetch effective statuses from this service and merge
in Python (see :meth:`bulk_get_effective`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class ReviewStatusRecord:
    """A persisted ``dq_run_review_status`` row + a flag telling the UI
    whether the value came from the table or from the virtual default."""

    __slots__ = ("run_id", "status", "updated_by", "updated_at", "is_default")

    def __init__(
        self,
        run_id: str,
        status: str,
        updated_by: str | None,
        updated_at: str | None,
        is_default: bool,
    ) -> None:
        self.run_id = run_id
        self.status = status
        self.updated_by = updated_by
        self.updated_at = updated_at
        # True iff this record reflects the catalogue default rather
        # than an explicit row in dq_run_review_status. The UI uses
        # this to render an "(auto)" hint and skip showing meaningless
        # ``updated_by`` / ``updated_at`` metadata.
        self.is_default = is_default


class ReviewStatusHistoryEntry:
    """One row from ``dq_run_review_status_history``."""

    __slots__ = ("run_id", "status", "previous_status", "changed_by", "changed_at")

    def __init__(
        self,
        run_id: str,
        status: str,
        previous_status: str | None,
        changed_by: str,
        changed_at: str | None,
    ) -> None:
        self.run_id = run_id
        self.status = status
        self.previous_status = previous_status
        self.changed_by = changed_by
        self.changed_at = changed_at


class ReviewStatusService:
    """CRUD + audit log for ``dq_run_review_status``.

    Constructed against the OLTP executor so the data co-locates with
    ``dq_app_settings`` (status catalogue) and ``dq_comments`` (the
    sibling reviewer-action table). All writes go through the app's
    service principal, mirroring :class:`CommentsService`.
    """

    # Capped to keep the detail-page request payload predictable. Most
    # reviews have <10 changes; the cap protects against pathological
    # cases where a script churns the status thousands of times.
    _HISTORY_LIMIT = 200

    def __init__(self, sql: SqlExecutor, settings: AppSettingsService) -> None:
        self._sql = sql
        self._settings = settings
        self._table = sql.fqn("dq_run_review_status")
        self._history_table = sql.fqn("dq_run_review_status_history")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_explicit(self, run_id: str) -> ReviewStatusRecord | None:
        """Return the persisted row for *run_id* or ``None`` if unset.

        Callers that want a value (even the catalogue default) for runs
        without a row should use :meth:`get_effective` instead.
        """
        er = escape_sql_string(run_id)
        sql = (
            f"SELECT run_id, status, updated_by, {self._sql.ts_text('updated_at')} "
            f"FROM {self._table} WHERE run_id = '{er}' LIMIT 1"
        )
        rows = self._sql.query(sql)
        if not rows:
            return None
        return ReviewStatusRecord(
            run_id=rows[0][0] or "",
            status=rows[0][1] or "",
            updated_by=rows[0][2],
            updated_at=rows[0][3],
            is_default=False,
        )

    def get_effective(self, run_id: str) -> ReviewStatusRecord:
        """Return the explicit row, or a virtual default record if unset.

        The default value comes from
        :meth:`AppSettingsService.get_default_run_review_status`.
        """
        explicit = self.get_explicit(run_id)
        if explicit is not None:
            return explicit
        return ReviewStatusRecord(
            run_id=run_id,
            status=self._settings.get_default_run_review_status(),
            updated_by=None,
            updated_at=None,
            is_default=True,
        )

    def bulk_get_effective(self, run_ids: list[str]) -> dict[str, ReviewStatusRecord]:
        """Return ``{run_id: ReviewStatusRecord}`` for every input run_id.

        Runs without an explicit row get a virtual-default record so the
        listing route can render *something* for every row. Uses a single
        ``WHERE run_id IN (...)`` query — at the existing listing LIMIT
        of 500 this is well inside any reasonable IN-list ceiling.
        """
        if not run_ids:
            return {}

        # Deduplicate before formatting so the ``IN`` list stays
        # compact even if the caller passes the same run twice.
        unique_ids = list(dict.fromkeys(run_ids))
        in_list = ", ".join(f"'{escape_sql_string(r)}'" for r in unique_ids)
        sql = (
            f"SELECT run_id, status, updated_by, {self._sql.ts_text('updated_at')} "
            f"FROM {self._table} WHERE run_id IN ({in_list})"
        )
        rows = self._sql.query(sql)

        explicit: dict[str, ReviewStatusRecord] = {
            (row[0] or ""): ReviewStatusRecord(
                run_id=row[0] or "",
                status=row[1] or "",
                updated_by=row[2],
                updated_at=row[3],
                is_default=False,
            )
            for row in rows
            if row and row[0]
        }

        default_value = self._settings.get_default_run_review_status()
        out: dict[str, ReviewStatusRecord] = {}
        for run_id in unique_ids:
            record = explicit.get(run_id)
            if record is not None:
                out[run_id] = record
            else:
                out[run_id] = ReviewStatusRecord(
                    run_id=run_id,
                    status=default_value,
                    updated_by=None,
                    updated_at=None,
                    is_default=True,
                )
        return out

    def get_history(self, run_id: str) -> list[ReviewStatusHistoryEntry]:
        """Return up to ``_HISTORY_LIMIT`` recent history rows, newest first."""
        er = escape_sql_string(run_id)
        sql = (
            f"SELECT run_id, status, previous_status, changed_by, "
            f"{self._sql.ts_text('changed_at')} "
            f"FROM {self._history_table} "
            f"WHERE run_id = '{er}' "
            f"ORDER BY changed_at DESC LIMIT {self._HISTORY_LIMIT}"
        )
        rows = self._sql.query(sql)
        return [
            ReviewStatusHistoryEntry(
                run_id=row[0] or "",
                status=row[1] or "",
                previous_status=row[2],
                changed_by=row[3] or "",
                changed_at=row[4],
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def set_status(
        self,
        run_id: str,
        status: str,
        *,
        user_email: str,
    ) -> ReviewStatusRecord:
        """Upsert the explicit row + append an audit row.

        ``status`` is validated against the live catalogue from
        :class:`AppSettingsService`; passing an unknown value raises
        ``ValueError`` so the route can return a 400. The audit row
        carries the effective previous status (virtual default included)
        so the history reads naturally on the UI.
        """
        if not run_id or not run_id.strip():
            raise ValueError("run_id must be a non-empty string.")
        if not status or not status.strip():
            raise ValueError("status must be a non-empty string.")
        if not user_email or not user_email.strip():
            raise ValueError("user_email must be a non-empty string.")

        cleaned_status = status.strip()
        allowed = {entry["value"] for entry in self._settings.get_run_review_statuses()}
        if cleaned_status not in allowed:
            raise ValueError(f"Unknown review status {cleaned_status!r}. " f"Allowed values: {sorted(allowed)}")

        previous_effective = self.get_effective(run_id).status

        # No-op writes still go through so the history captures the
        # human action (e.g. the reviewer re-confirmed the existing
        # status). UI shows the no-op rows as "Confirmed" if we want
        # that polish later; for now we just record the duplicate.
        self._sql.upsert(
            self._table,
            key_cols={"run_id": run_id},
            value_cols={
                "status": cleaned_status,
                "updated_by": user_email,
                "updated_at": RawSql("current_timestamp()"),
            },
        )

        self._append_history(
            run_id=run_id,
            status=cleaned_status,
            previous_status=previous_effective,
            changed_by=user_email,
        )

        logger.info(
            "Run %s review status set to %r (prev=%r) by %s",
            run_id,
            cleaned_status,
            previous_effective,
            user_email,
        )
        return ReviewStatusRecord(
            run_id=run_id,
            status=cleaned_status,
            updated_by=user_email,
            updated_at=datetime.now(timezone.utc).isoformat(),
            is_default=False,
        )

    def clear_status(self, run_id: str, *, user_email: str) -> ReviewStatusRecord:
        """Delete the explicit row + append an audit row for the revert.

        The run reverts to the catalogue default. The audit row records
        the default value as the new status so the history reads
        "Acknowledged → Pending review" rather than "Acknowledged →
        (none)" which would be ambiguous.
        """
        if not run_id or not run_id.strip():
            raise ValueError("run_id must be a non-empty string.")
        if not user_email or not user_email.strip():
            raise ValueError("user_email must be a non-empty string.")

        previous_effective = self.get_effective(run_id).status
        default_value = self._settings.get_default_run_review_status()

        er = escape_sql_string(run_id)
        self._sql.execute(f"DELETE FROM {self._table} WHERE run_id = '{er}'")

        self._append_history(
            run_id=run_id,
            status=default_value,
            previous_status=previous_effective,
            changed_by=user_email,
        )

        logger.info(
            "Run %s review status cleared (was %r) by %s",
            run_id,
            previous_effective,
            user_email,
        )
        return ReviewStatusRecord(
            run_id=run_id,
            status=default_value,
            updated_by=None,
            updated_at=None,
            is_default=True,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _append_history(
        self,
        *,
        run_id: str,
        status: str,
        previous_status: str | None,
        changed_by: str,
    ) -> None:
        """Insert one row into ``dq_run_review_status_history``.

        Hand-rolled INSERT (no ``upsert``) because the table is
        append-only with no natural key. We can't piggyback on the
        executor's ``_render_value`` helpers either — the Delta one
        passes ``current_timestamp()`` through verbatim (Postgres
        rejects the parenthesised form), and the Postgres one is not
        exposed publicly. ``now()`` is portable across both dialects
        (Postgres ships it as a built-in synonym for
        ``CURRENT_TIMESTAMP``; Delta SQL accepts it too — same idiom
        as :class:`CommentsService`), so we inline it directly here.
        """
        run_id_lit = f"'{escape_sql_string(run_id)}'"
        status_lit = f"'{escape_sql_string(status)}'"
        if previous_status is None:
            prev_lit = "NULL"
        else:
            prev_lit = f"'{escape_sql_string(previous_status)}'"
        changed_by_lit = f"'{escape_sql_string(changed_by)}'"

        self._sql.execute(
            f"INSERT INTO {self._history_table} "
            f"(run_id, status, previous_status, changed_by, changed_at) "
            f"VALUES ({run_id_lit}, {status_lit}, {prev_lit}, {changed_by_lit}, now())"
        )
