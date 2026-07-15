"""Pending applications service (Bulk Contract Import — Phase 2).

Manages ``dq_pending_applications`` — the transient store of registry-rule
applications that were *staged* against a monitored table but can't be
applied yet because the rule isn't published.

The Bulk Contract Import execute step records one row here per (binding,
rule, column_mapping) whenever a freshly-created rule lands
``pending_approval`` (approval-enabled orgs). When that rule is later
approved, :func:`_publish_registry_rule` drains every pending row for the
rule into a real ``dq_applied_rules`` link (via
:meth:`ApplyRulesService.apply_rule`) and deletes it — see
:meth:`activate_for_rule`.

There is exactly one pending row per ``(binding_id, rule_id)`` pair: a
re-record replaces the stored mapping (the Postgres baseline enforces this
with ``UNIQUE (binding_id, rule_id)``; this service mirrors it on the Delta
OLTP-fallback baseline, which can't declare the constraint natively).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import ColumnMappingGroup
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


@dataclass
class PendingApplication:
    """Domain model for one ``dq_pending_applications`` row."""

    binding_id: str
    rule_id: str
    column_mapping: list[ColumnMappingGroup] = field(default_factory=list)
    id: str | None = None
    created_by: str | None = None
    created_at: datetime | None = None


class PendingApplicationService:
    """Manages ``dq_pending_applications`` (staged, approval-gated apply links)."""

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._table = sql.fqn("dq_pending_applications")
        column_mapping = sql.select_json_text("column_mapping")
        created_at = sql.ts_text("created_at")
        self._select_cols = (
            "id, binding_id, rule_id, "
            f"{column_mapping} AS column_mapping_json, created_by, {created_at} AS created_at"
        )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def record(
        self,
        binding_id: str,
        rule_id: str,
        column_mapping: list[ColumnMappingGroup],
        user_email: str,
    ) -> PendingApplication:
        """Stage (or replace) a pending application for ``(binding_id, rule_id)``.

        Upsert semantics: an existing pending row for the same
        ``(binding_id, rule_id)`` has its ``column_mapping`` overwritten
        rather than duplicated, so re-running the bulk import for the same
        contract bundle is idempotent.
        """
        existing = self._get_by_natural_key(binding_id, rule_id)
        if existing is not None:
            existing.column_mapping = column_mapping
            mapping_expr = self._sql.json_literal_expr(json.dumps(column_mapping))
            self._sql.execute(
                f"UPDATE {self._table} SET column_mapping = {mapping_expr} "  # noqa: S608
                f"WHERE id = '{escape_sql_string(existing.id or '')}'"
            )
            logger.info("Updated pending application for binding %s rule %s", binding_id, rule_id)
            return existing

        pending = PendingApplication(
            id=uuid4().hex[:16],
            binding_id=binding_id,
            rule_id=rule_id,
            column_mapping=column_mapping,
            created_by=user_email,
            created_at=datetime.now(timezone.utc),
        )
        mapping_expr = self._sql.json_literal_expr(json.dumps(pending.column_mapping))
        self._sql.execute(
            f"INSERT INTO {self._table} "  # noqa: S608
            "(id, binding_id, rule_id, column_mapping, created_by, created_at) VALUES "
            f"('{escape_sql_string(pending.id or '')}', '{escape_sql_string(binding_id)}', "
            f"'{escape_sql_string(rule_id)}', {mapping_expr}, {self._opt_str(user_email)}, now())"
        )
        logger.info("Recorded pending application for binding %s rule %s", binding_id, rule_id)
        return pending

    def delete(self, pending_id: str) -> None:
        """Delete one pending application by id (no-op if already gone)."""
        self._sql.execute(f"DELETE FROM {self._table} WHERE id = '{escape_sql_string(pending_id)}'")  # noqa: S608

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_for_rule(self, rule_id: str) -> list[PendingApplication]:
        """List every pending application staged against ``rule_id``."""
        e = escape_sql_string(rule_id)
        rows = self._sql.query(
            f"SELECT {self._select_cols} FROM {self._table} "  # noqa: S608
            f"WHERE rule_id = '{e}' ORDER BY created_at"
        )
        return [self._row_to_pending(row) for row in rows]

    def list_for_binding(self, binding_id: str) -> list[PendingApplication]:
        """List every pending application staged against ``binding_id``."""
        e = escape_sql_string(binding_id)
        rows = self._sql.query(
            f"SELECT {self._select_cols} FROM {self._table} "  # noqa: S608
            f"WHERE binding_id = '{e}' ORDER BY created_at"
        )
        return [self._row_to_pending(row) for row in rows]

    def _get_by_natural_key(self, binding_id: str, rule_id: str) -> PendingApplication | None:
        e_binding = escape_sql_string(binding_id)
        e_rule = escape_sql_string(rule_id)
        rows = self._sql.query(
            f"SELECT {self._select_cols} FROM {self._table} "  # noqa: S608
            f"WHERE binding_id = '{e_binding}' AND rule_id = '{e_rule}'"
        )
        if not rows:
            return None
        return self._row_to_pending(rows[0])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

    @staticmethod
    def _parse_column_mapping(raw: str | None) -> list[ColumnMappingGroup]:
        if not raw:
            return []
        try:
            parsed = json.loads(raw, strict=False)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        groups: list[ColumnMappingGroup] = []
        for item in parsed:
            if isinstance(item, dict):
                groups.append({str(k): str(v) for k, v in item.items()})
        return groups

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            logger.warning("Unparsable timestamp %r; treating as None", value)
            return None

    def _row_to_pending(self, row: list[Any]) -> PendingApplication:
        return PendingApplication(
            id=row[0],
            binding_id=row[1],
            rule_id=row[2],
            column_mapping=self._parse_column_mapping(row[3]),
            created_by=row[4],
            created_at=self._parse_timestamp(row[5]),
        )
