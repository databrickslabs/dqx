"""Lifecycle history of monitored tables, collections and registry rules.

Each kind keeps an append-only audit trail of the same shape: registry rules
in ``dq_rules_history`` (written by :class:`RegistryService`), monitored tables
in ``dq_monitored_tables_history`` and collections in
``dq_data_products_history`` (written here). A row records the action (create,
update, submit, approve, reject, withdraw, delete), the status before and
after, the version, an optional snapshot of what changed (``state``), the note
left with it, who did it and when.

The History tab reads all three the same way: :meth:`LifecycleHistoryService.list_events`
maps a trail to :class:`HistoryEvent` steps.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql

logger = logging.getLogger(__name__)

HistoryObjectType = Literal["monitored_table", "data_product"]
HistoryAction = Literal["create", "update", "submit", "approve", "reject", "withdraw", "delete"]
# What the History tab shows for each action; a delete isn't shown (the object is gone).
EventAction = Literal["created", "edited", "submitted", "approved", "rejected", "withdrawn"]

_EVENT_OF_ACTION: dict[str, EventAction] = {
    "create": "created",
    "update": "edited",
    "submit": "submitted",
    "approve": "approved",
    "reject": "rejected",
    "withdraw": "withdrawn",
}
# The history table, and its key column, of each object type.
_TRAILS: dict[HistoryObjectType, tuple[str, str]] = {
    "monitored_table": ("dq_monitored_tables_history", "binding_id"),
    "data_product": ("dq_data_products_history", "product_id"),
}


@dataclass
class HistoryEvent:
    """One step of an object's history, as the History tab shows it."""

    action: EventAction
    actor: str | None
    created_at: str | None
    rationale: str | None = None
    version: int | None = None


def rule_history_action(
    action: str, prev_status: str | None, new_status: str | None, rationale: str | None
) -> EventAction | None:
    """Map a ``dq_rules_history`` row to a History step, or None for one that isn't shown (seed, delete).

    Rejecting or withdrawing the review of a published rule both return it to
    ``approved``; they are recorded as ``reject`` / ``revoke``. Older rows wrote
    ``status:approved`` for both, so for those a note means a rejection (a
    withdrawal never carries one).
    """
    if action in ("create", "update", "approve", "reject"):
        return _EVENT_OF_ACTION[action]
    if action == "revoke":
        return "withdrawn"
    if action == "status:pending_approval":
        return "submitted"
    if action == "status:rejected":
        return "rejected"
    if prev_status == "pending_approval" and new_status == "draft":
        return "withdrawn"
    if prev_status == "pending_approval" and new_status == "approved":
        return "rejected" if rationale else "withdrawn"
    return None


class LifecycleHistoryService:
    """Records the lifecycle history of tables and collections, and reads that of all three kinds."""

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._rules_history_table = sql.fqn("dq_rules_history")

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
        state: dict[str, Any] | None = None,
        rationale: str | None = None,
    ) -> None:
        """Append one history row. Best-effort: the change itself has already been made."""
        table, key = _TRAILS[object_type]
        try:
            self._sql.insert(
                self._sql.fqn(table),
                values={
                    key: object_id,
                    "action": action,
                    "prev_status": prev_status,
                    "new_status": new_status,
                    "version": version,
                    "state": None if state is None else RawSql(self._sql.json_literal_expr(json.dumps(state))),
                    "rationale": rationale,
                    "changed_by": actor,
                    "changed_at": RawSql("now()"),
                },
            )
        # Broad on purpose: the change is already committed, and a failed history
        # write (e.g. the table missing before its migration ran) must not turn
        # it into an error for the user.
        except Exception:
            logger.warning(f"Failed to record the {action} history of a {object_type} (non-fatal)", exc_info=True)

    def list_events(self, object_type: HistoryObjectType, object_id: str) -> list[HistoryEvent]:
        """Return the History steps of a table or a collection, newest first."""
        table, key = _TRAILS[object_type]
        rows = self._sql.select_rows(
            self._sql.fqn(table),
            ["action", "changed_by", "changed_at", "rationale", "version"],
            where={key: object_id},
        )
        events = [
            HistoryEvent(
                action=_EVENT_OF_ACTION[row[0]],
                actor=row[1],
                created_at=_text(row[2]),
                rationale=row[3],
                version=_int(row[4]) if row[0] == "approve" else None,
            )
            for row in rows
            if row[0] in _EVENT_OF_ACTION
        ]
        return _newest_first(events)

    def list_rule_events(self, rule_id: str) -> list[HistoryEvent]:
        """Return a registry rule's History steps from ``dq_rules_history``, newest first."""
        rows = self._sql.select_rows(
            self._rules_history_table,
            ["action", "prev_status", "new_status", "rationale", "changed_by", "changed_at", "version"],
            where={"rule_id": rule_id},
        )
        events: list[HistoryEvent] = []
        for row in rows:
            action = rule_history_action(row[0], row[1], row[2], row[3])
            if action is None:
                continue
            events.append(
                HistoryEvent(
                    action=action,
                    actor=row[4],
                    created_at=_text(row[5]),
                    rationale=row[3],
                    version=_int(row[6]) if action == "approved" else None,
                )
            )
        return _newest_first(events)


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _newest_first(events: list[HistoryEvent]) -> list[HistoryEvent]:
    def key(event: HistoryEvent) -> datetime:
        try:
            parsed = datetime.fromisoformat(event.created_at or "")
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    return sorted(events, key=key, reverse=True)
