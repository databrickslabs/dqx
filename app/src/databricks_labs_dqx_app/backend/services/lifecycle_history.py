"""Lifecycle history of registry rules, monitored tables and collections.

All three kinds keep one append-only audit trail, ``dq_lifecycle_history``,
keyed by ``(object_type, object_id)``. A row records the action (create,
update, submit, approve, reject, withdraw, deprecate, reinstate, delete), the
status before and after, the version, what changed (``details``), the note left
with it, who did it, whether the app did it on their behalf (an auto-approval)
and when.

:meth:`LifecycleHistoryService.record` writes a step and
:meth:`LifecycleHistoryService.list_events` reads an object's steps for its
History tab, the same way for every kind. Each step also stores the object's
whole state after it (``snapshot``, read with
:meth:`LifecycleHistoryService.get_snapshot`), so any saved state can be compared
with any other, not just approved versions.
"""

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from databricks_labs_dqx_app.backend.common.approvals import AUTO_APPROVER_SUFFIX, mark_auto_approver
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql

logger = logging.getLogger(__name__)

HistoryObjectType = Literal["registry_rule", "monitored_table", "data_product"]
HistoryAction = Literal[
    "create", "update", "submit", "approve", "reject", "withdraw", "deprecate", "reinstate", "delete"
]
# What the History tab shows for each action. Deprecate / reinstate and delete aren't shown.
EventAction = Literal["created", "edited", "submitted", "approved", "rejected", "withdrawn"]

_EVENT_OF_ACTION: dict[str, EventAction] = {
    "create": "created",
    "update": "edited",
    "submit": "submitted",
    "approve": "approved",
    "reject": "rejected",
    "withdraw": "withdrawn",
}


@dataclass
class HistoryEvent:
    """One step of an object's history, as the History tab shows it."""

    action: EventAction
    actor: str | None
    created_at: str | None
    rationale: str | None = None
    version: int | None = None
    history_id: int | None = None
    #: The step stored the object's state after it, so it can be compared.
    has_snapshot: bool = False


#: Reads an object's current whole state (by its id) for a step's snapshot.
Snapshotter = Callable[[str], object]


class _CurrentState:
    """Marker for :meth:`LifecycleHistoryService.record`: snapshot the object as it is now."""


CURRENT_STATE = _CurrentState()


class LifecycleHistoryService:
    """Records and reads the lifecycle history of registry rules, monitored tables and collections."""

    def __init__(
        self,
        sql: OltpExecutorProtocol,
        snapshotters: Mapping[HistoryObjectType, Snapshotter] | None = None,
    ) -> None:
        self._sql = sql
        self._table = sql.fqn("dq_lifecycle_history")
        self._snapshotters = dict(snapshotters or {})

    def snapshot(self, object_type: HistoryObjectType, object_id: str) -> object | None:
        """The object's current whole state for a step's snapshot, or None (no snapshotter, or it's gone).

        Best-effort: a failed read never fails the step it's recorded with.
        """
        snapshotter = self._snapshotters.get(object_type)
        if snapshotter is None:
            return None
        try:
            return snapshotter(object_id)
        # Broad on purpose: the snapshot is extra detail on a change that has
        # already been made; failing to read it must not fail that change.
        except Exception:
            logger.warning(f"Failed to snapshot a {object_type} for its history (non-fatal)", exc_info=True)
            return None

    def record(
        self,
        object_type: HistoryObjectType,
        object_id: str,
        action: HistoryAction,
        actor: str,
        *,
        prev_status: str | None = None,
        new_status: str | None = None,
        version: int | None = None,
        details: dict[str, object] | None = None,
        rationale: str | None = None,
        changed_at: datetime | None = None,
        snapshot: object | None = CURRENT_STATE,
    ) -> None:
        """Append one history row. Best-effort: the change itself has already been made.

        An *actor* marked as an auto-approver (see :func:`mark_auto_approver`)
        is stored unmarked, with ``is_automatic`` set. *changed_at* defaults to
        now; the demo seed passes the back-dated time of the step it replays.
        *snapshot* defaults to the object's state now (none for a delete); pass
        the state explicitly when the caller already has it, or None for none.
        """
        if isinstance(snapshot, _CurrentState):
            snapshot = None if action == "delete" else self.snapshot(object_type, object_id)
        is_automatic = actor.endswith(AUTO_APPROVER_SUFFIX)
        if is_automatic:
            actor = actor.removesuffix(AUTO_APPROVER_SUFFIX)
        try:
            self._sql.insert(
                self._table,
                values={
                    "object_type": object_type,
                    "object_id": object_id,
                    "action": action,
                    "prev_status": prev_status,
                    "new_status": new_status,
                    "version": version,
                    "details": None if details is None else RawSql(self._sql.json_literal_expr(json.dumps(details))),
                    "snapshot": (
                        None
                        if snapshot is None
                        else RawSql(self._sql.json_literal_expr(json.dumps(snapshot, default=str)))
                    ),
                    "rationale": rationale,
                    "changed_by": actor,
                    "is_automatic": is_automatic,
                    "changed_at": RawSql("now()") if changed_at is None else changed_at.isoformat(),
                },
            )
        # Broad on purpose: the change is already committed, and a failed history
        # write (e.g. the table missing before its migration ran) must not turn
        # it into an error for the user.
        except Exception:
            logger.warning(f"Failed to record the {action} history of a {object_type} (non-fatal)", exc_info=True)

    def list_events(self, object_type: HistoryObjectType, object_id: str) -> list[HistoryEvent]:
        """Return the History steps of a rule, a table or a collection, newest first."""
        rows = self._sql.select_rows(
            self._table,
            ["action", "changed_by", "is_automatic", "changed_at", "rationale", "version", "history_id"],
            where={"object_type": object_type, "object_id": object_id},
        )
        # The steps without a snapshot, read on their own so the snapshots themselves aren't fetched.
        unsnapshotted = {
            _int(row[0])
            for row in self._sql.select_rows(
                self._table,
                ["history_id"],
                where={"object_type": object_type, "object_id": object_id, "snapshot": None},
            )
        }
        events = [
            HistoryEvent(
                action=_EVENT_OF_ACTION[row[0]],
                actor=_actor(row[1], row[2]),
                created_at=_text(row[3]),
                rationale=row[4],
                version=_int(row[5]) if row[0] == "approve" else None,
                history_id=_int(row[6]),
                has_snapshot=_int(row[6]) not in unsnapshotted,
            )
            for row in rows
            if row[0] in _EVENT_OF_ACTION
        ]
        return _newest_first(events)

    def get_snapshot(
        self, object_type: HistoryObjectType, object_id: str, history_id: int
    ) -> tuple[bool, object | None]:
        """The object's state recorded with one of its steps: ``(found, snapshot)``.

        *found* is False when *history_id* isn't one of the object's steps; the
        snapshot is None for a step that didn't record one.
        """
        rows = self._sql.select_rows(
            self._table,
            ["snapshot"],
            where={"object_type": object_type, "object_id": object_id, "history_id": history_id},
        )
        if not rows:
            return False, None
        return True, _json(rows[0][0])


def _actor(changed_by: object, is_automatic: object) -> str | None:
    """The actor as the History tab shows it: an automatic step keeps its ``(auto)`` marker."""
    if changed_by is None:
        return None
    return mark_auto_approver(str(changed_by)) if _truthy(is_automatic) else str(changed_by)


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("true", "t", "1")
    return bool(value)


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _json(value: object) -> object | None:
    """A JSONB value as read back: already decoded, or JSON text. Unparseable text is None."""
    if value is None or isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return None


def _parse_time(value: str | None) -> datetime:
    try:
        parsed = datetime.fromisoformat(value or "")
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _newest_first(events: list[HistoryEvent]) -> list[HistoryEvent]:
    """Newest first; steps recorded at the same time keep the order they were recorded in (by id)."""
    return sorted(events, key=lambda event: (_parse_time(event.created_at), event.history_id or 0), reverse=True)
