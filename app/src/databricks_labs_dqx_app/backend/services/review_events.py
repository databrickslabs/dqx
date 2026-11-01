"""The review trail: who submitted, approved, rejected or withdrew a table, a collection or a rule, and why.

Monitored tables and collections record each review step in
``dq_review_events`` (their own rows keep only the latest submit and decision
notes). Registry rules already keep a full audit trail in ``dq_rules_history``;
it is read and mapped to the same events here, so the History tab shows one
shape for all three.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql

logger = logging.getLogger(__name__)

ReviewAction = Literal["submitted", "approved", "rejected", "withdrawn"]
ReviewObjectType = Literal["monitored_table", "data_product"]


@dataclass
class ReviewEvent:
    """One review step: what happened, who did it, the note they left and the version approved."""

    action: ReviewAction
    actor: str | None
    created_at: str | None
    rationale: str | None = None
    version: int | None = None


def rule_history_action(
    action: str, prev_status: str | None, new_status: str | None, rationale: str | None
) -> ReviewAction | None:
    """Map a ``dq_rules_history`` row to a review step, or None for one that isn't (create, edit, delete).

    Rejecting or withdrawing the review of a published rule both return it to
    ``approved``; they are recorded as ``reject`` / ``revoke``. Older rows wrote
    ``status:approved`` for both, so for those a note means a rejection (a
    withdrawal never carries one).
    """
    if action == "approve":
        return "approved"
    if action == "reject":
        return "rejected"
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


class ReviewEventService:
    """Records and reads the review trail of tables and collections, and reads that of rules."""

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._events_table = sql.fqn("dq_review_events")
        self._rules_history_table = sql.fqn("dq_rules_history")

    def record(
        self,
        object_type: ReviewObjectType,
        object_id: str,
        action: ReviewAction,
        actor: str,
        *,
        rationale: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record one review step. Best-effort: the step itself has already happened."""
        try:
            self._sql.insert(
                self._events_table,
                values={
                    "id": uuid4().hex,
                    "object_type": object_type,
                    "object_id": object_id,
                    "action": action,
                    "version": version,
                    "rationale": rationale,
                    "actor": actor,
                    "created_at": RawSql("now()"),
                },
            )
        # Broad on purpose: the submit or decision is already committed, and a
        # failed trail write (e.g. the table missing before its migration ran)
        # must not turn it into an error for the user.
        except Exception:
            logger.warning(f"Failed to record the {action} review step of {object_type} (non-fatal)", exc_info=True)

    def list_events(self, object_type: ReviewObjectType, object_id: str) -> list[ReviewEvent]:
        """Return the review trail of a table or a collection, newest first."""
        rows = self._sql.select_rows(
            self._events_table,
            ["action", "actor", "created_at", "rationale", "version"],
            where={"object_type": object_type, "object_id": object_id},
        )
        events = [
            ReviewEvent(
                action=row[0],
                actor=row[1],
                created_at=_text(row[2]),
                rationale=row[3],
                version=_int(row[4]),
            )
            for row in rows
            if row[0] in ("submitted", "approved", "rejected", "withdrawn")
        ]
        return _newest_first(events)

    def list_rule_events(self, rule_id: str) -> list[ReviewEvent]:
        """Return a registry rule's review trail from ``dq_rules_history``, newest first."""
        rows = self._sql.select_rows(
            self._rules_history_table,
            ["action", "prev_status", "new_status", "rationale", "changed_by", "changed_at", "version"],
            where={"rule_id": rule_id},
        )
        events: list[ReviewEvent] = []
        for row in rows:
            action = rule_history_action(row[0], row[1], row[2], row[3])
            if action is None:
                continue
            events.append(
                ReviewEvent(
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


def _newest_first(events: list[ReviewEvent]) -> list[ReviewEvent]:
    def key(event: ReviewEvent) -> datetime:
        try:
            parsed = datetime.fromisoformat(event.created_at or "")
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    return sorted(events, key=key, reverse=True)
