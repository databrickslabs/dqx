"""Rules Registry service (Phase 2B — registry CRUD + two-tier approval gate).

Manages the LIVE ``dq_rules`` template rows and their frozen
``dq_rule_versions`` publish snapshots, per
``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §3.1 and §5.

This is the REGISTRY gate (tier 1 of the two-tier approval model): a
table-agnostic rule definition moves ``draft -> pending_approval ->
approved (published) -> deprecated``, independent of whether/where it is
later *applied* to a monitored table (tier 2, Phase 3 — ``dq_applied_rules``
does not exist yet).

Mirrors :class:`~databricks_labs_dqx_app.backend.services.rules_catalog_service.RulesCatalogService`'s
shape (status machine, history recording, dialect-portable SQL via the
executor helpers) but operates on the registry tables instead of
per-table ``dq_quality_rules``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint
from databricks_labs_dqx_app.backend.registry_models import (
    AuthorKind,
    Polarity,
    RegistryRule,
    RuleDefinition,
    RuleMode,
    RuleStatus,
    RuleVersion,
    get_rule_dimension,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class RegistryService:
    """Manages the Rules Registry (``dq_rules`` / ``dq_rule_versions``) in the OLTP store."""

    VALID_STATUSES = {"draft", "pending_approval", "approved", "rejected", "deprecated"}

    VALID_TRANSITIONS: dict[str, set[str]] = {
        "draft": {"pending_approval"},
        "pending_approval": {"approved", "rejected"},
        "approved": {"deprecated"},
        "rejected": set(),
        "deprecated": {"approved"},
    }

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._table = sql.fqn("dq_rules")
        self._versions_table = sql.fqn("dq_rule_versions")
        self._history_table = sql.fqn("dq_rules_history")
        self._select_cols = self._build_select_cols()

    def _build_select_cols(self) -> str:
        definition = self._sql.select_json_text("definition")
        user_metadata = self._sql.select_json_text("user_metadata")
        created_at = self._sql.ts_text("created_at")
        updated_at = self._sql.ts_text("updated_at")
        return (
            "rule_id, mode, status, version, polarity, author_kind, "
            f"{definition} AS definition_json, {user_metadata} AS user_metadata_json, "
            f"fingerprint, steward, is_builtin, source, created_by, {created_at}, "
            f"updated_by, {updated_at}"
        )

    # ------------------------------------------------------------------
    # List / Get
    # ------------------------------------------------------------------

    def list_rules(
        self,
        status: str | None = None,
        dimension: str | None = None,
        severity: str | None = None,
        steward: str | None = None,
        tag: str | None = None,
    ) -> list[RegistryRule]:
        """List registry rules, optionally filtered.

        ``status`` and ``steward`` are pushed down into SQL; ``dimension``,
        ``severity``, and ``tag`` filter over ``user_metadata`` in Python
        (it's a JSON blob, not a column), matching how
        :class:`RulesCatalogService` handles free-text metadata.
        """
        clauses: list[str] = []
        if status:
            clauses.append(f"status = '{escape_sql_string(status)}'")
        if steward:
            clauses.append(f"steward = '{escape_sql_string(steward)}'")
        sql = f"SELECT {self._select_cols} FROM {self._table}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC LIMIT 2000"
        rows = self._sql.query(sql)
        rules = [self._row_to_rule(row) for row in rows]
        if dimension:
            rules = [r for r in rules if get_rule_dimension(r.user_metadata) == dimension]
        if severity:
            rules = [r for r in rules if get_rule_severity(r.user_metadata) == severity]
        if tag:
            rules = [r for r in rules if tag in r.user_metadata]
        return rules

    def get_rule(self, rule_id: str) -> RegistryRule | None:
        """Get a single registry rule (with its typed slots/params) by id."""
        return self._get(rule_id)

    def get_rule_with_version(self, rule_id: str) -> tuple[RegistryRule, RuleVersion | None] | None:
        """Get a registry rule plus its current published snapshot, if any."""
        rule = self._get(rule_id)
        if rule is None:
            return None
        if rule.version <= 0:
            return rule, None
        return rule, self._get_version(rule.rule_id, rule.version)

    def _get(self, rule_id: str) -> RegistryRule | None:
        e_rule_id = escape_sql_string(rule_id)
        sql = f"SELECT {self._select_cols} FROM {self._table} WHERE rule_id = '{e_rule_id}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_rule(rows[0])

    def _get_version(self, rule_id: str, version: int) -> RuleVersion | None:
        definition = self._sql.select_json_text("definition")
        user_metadata = self._sql.select_json_text("user_metadata")
        created_at = self._sql.ts_text("created_at")
        e_rule_id = escape_sql_string(rule_id)
        sql = (
            f"SELECT rule_id, version, {definition} AS definition_json, polarity, "
            f"{user_metadata} AS user_metadata_json, created_by, {created_at} "
            f"FROM {self._versions_table} WHERE rule_id = '{e_rule_id}' AND version = {int(version)}"  # noqa: S608
        )
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_version(rows[0])

    # ------------------------------------------------------------------
    # Create / update (draft only)
    # ------------------------------------------------------------------

    def create_rule(
        self,
        mode: RuleMode,
        definition: RuleDefinition,
        user_email: str,
        polarity: Polarity | None = None,
        author_kind: AuthorKind = "human",
        user_metadata: dict[str, Any] | None = None,
        steward: str | None = None,
    ) -> tuple[RegistryRule, str | None]:
        """Create a new draft registry rule.

        Returns ``(rule, dedup_warning)``. The dedup check is a WARNING,
        never a hard error — a published rule sharing the same structural
        fingerprint doesn't block creation, it just flags the possible
        duplicate for the author to review.
        """
        now = datetime.now(timezone.utc)
        rule = RegistryRule(
            rule_id=uuid4().hex[:16],
            mode=mode,
            status="draft",
            version=0,
            polarity=polarity,
            author_kind=author_kind,
            definition=definition,
            user_metadata=dict(user_metadata or {}),
            steward=steward,
            is_builtin=False,
            source="ui",
            created_by=user_email,
            created_at=now,
            updated_by=user_email,
            updated_at=now,
        )
        rule.fingerprint = compute_registry_rule_fingerprint(rule)
        warning = self._dedup_warning(rule)
        self._insert(rule)
        self._record_history(rule.rule_id, rule.definition, rule.version, "create", None, "draft", user_email)
        logger.info("Created registry rule %s (mode=%s)", rule.rule_id, rule.mode)
        return rule, warning

    def update_draft(
        self,
        rule_id: str,
        user_email: str,
        mode: RuleMode | None = None,
        definition: RuleDefinition | None = None,
        polarity: Polarity | None = None,
        user_metadata: dict[str, Any] | None = None,
        steward: str | None = None,
    ) -> RegistryRule:
        """Update a draft registry rule in place. Only ``draft`` rules are editable."""
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        if rule.status != "draft":
            raise ValueError(
                f"Cannot edit registry rule '{rule_id}': only draft rules can be edited "
                f"(current status='{rule.status}')."
            )
        if mode is not None:
            rule.mode = mode
        if definition is not None:
            rule.definition = definition
        if polarity is not None:
            rule.polarity = polarity
        if user_metadata is not None:
            rule.user_metadata = dict(user_metadata)
        if steward is not None:
            rule.steward = steward
        rule.fingerprint = compute_registry_rule_fingerprint(rule)
        rule.updated_by = user_email
        self._update(rule)
        self._record_history(rule.rule_id, rule.definition, rule.version, "update", "draft", "draft", user_email)
        logger.info("Updated draft registry rule %s", rule.rule_id)
        return rule

    def _dedup_warning(self, rule: RegistryRule) -> str | None:
        """Return a human-readable warning if a published rule shares this fingerprint."""
        if not rule.fingerprint:
            return None
        e_fp = escape_sql_string(rule.fingerprint)
        sql = (
            f"SELECT {self._select_cols} FROM {self._table} "
            f"WHERE fingerprint = '{e_fp}' AND status = 'approved' AND rule_id != '{escape_sql_string(rule.rule_id)}'"  # noqa: S608
        )
        rows = self._sql.query(sql)
        if not rows:
            return None
        existing = self._row_to_rule(rows[0])
        from databricks_labs_dqx_app.backend.registry_models import get_rule_name

        name = get_rule_name(existing.user_metadata) or existing.rule_id
        return (
            f"A published rule with an identical definition already exists: "
            f"'{name}' (rule_id={existing.rule_id})."
        )

    # ------------------------------------------------------------------
    # Lifecycle transitions
    # ------------------------------------------------------------------

    def submit(self, rule_id: str, user_email: str) -> RegistryRule:
        """Submit a draft rule for approval (draft -> pending_approval)."""
        return self._transition(rule_id, "pending_approval", user_email)

    def approve(self, rule_id: str, user_email: str) -> RegistryRule:
        """Approve (publish) a pending rule.

        Publishing bumps ``version`` (0 -> 1 on first publish) and writes a
        frozen ``dq_rule_versions`` snapshot — this IS the "publish" action
        described in the design spec, not a separate endpoint.
        """
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        self._check_transition(rule.status, "approved")
        prev_status = rule.status
        rule.status = "approved"
        rule.version += 1
        rule.updated_by = user_email
        self._update(rule)
        self._write_version_snapshot(rule, user_email)
        self._record_history(
            rule.rule_id, rule.definition, rule.version, "approve", prev_status, "approved", user_email
        )
        logger.info("Published registry rule %s as v%d", rule.rule_id, rule.version)
        return rule

    def reject(self, rule_id: str, user_email: str) -> RegistryRule:
        """Reject a pending rule (pending_approval -> rejected)."""
        return self._transition(rule_id, "rejected", user_email)

    def deprecate(self, rule_id: str, user_email: str) -> RegistryRule:
        """Deprecate a published rule (approved -> deprecated)."""
        return self._transition(rule_id, "deprecated", user_email)

    def undeprecate(self, rule_id: str, user_email: str) -> RegistryRule:
        """Reinstate a deprecated rule (deprecated -> approved). Does not re-bump version."""
        return self._transition(rule_id, "approved", user_email)

    def _transition(self, rule_id: str, new_status: RuleStatus, user_email: str) -> RegistryRule:
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        self._check_transition(rule.status, new_status)
        prev_status = rule.status
        rule.status = new_status
        rule.updated_by = user_email
        self._update(rule)
        self._record_history(
            rule.rule_id, rule.definition, rule.version, f"status:{new_status}", prev_status, new_status, user_email
        )
        logger.info("Registry rule %s transitioned %s -> %s", rule_id, prev_status, new_status)
        return rule

    def _check_transition(self, current_status: str, new_status: RuleStatus) -> None:
        if new_status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {new_status}. Must be one of {self.VALID_STATUSES}")
        allowed = self.VALID_TRANSITIONS.get(current_status, set())
        if new_status not in allowed:
            raise ValueError(
                f"Cannot transition from '{current_status}' to '{new_status}'. Allowed transitions: {allowed or 'none'}"
            )

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, rule_id: str, user_email: str) -> None:
        """Delete a registry rule.

        TODO(Phase 3): once ``dq_applied_rules`` exists, block (409) deletion
        of a rule that is currently applied to any monitored table. That
        table doesn't exist yet, so deletion is unconditionally allowed for
        now.
        """
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        e_rule_id = escape_sql_string(rule_id)
        self._sql.execute(f"DELETE FROM {self._table} WHERE rule_id = '{e_rule_id}'")
        self._record_history(rule_id, rule.definition, rule.version, "delete", rule.status, None, user_email)
        logger.info("Deleted registry rule %s (by %s)", rule_id, user_email)

    # ------------------------------------------------------------------
    # Internal persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

    def _insert(self, rule: RegistryRule) -> None:
        definition_expr = self._sql.json_literal_expr(json.dumps(rule.definition.model_dump(mode="json")))
        metadata_expr = self._sql.json_literal_expr(json.dumps(rule.user_metadata))
        sql = (
            f"INSERT INTO {self._table} "
            "(rule_id, mode, status, version, polarity, author_kind, definition, user_metadata, "
            "fingerprint, steward, is_builtin, source, created_by, created_at, updated_by, updated_at) VALUES "
            f"('{escape_sql_string(rule.rule_id)}', '{escape_sql_string(rule.mode)}', "
            f"'{escape_sql_string(rule.status)}', {rule.version}, {self._opt_str(rule.polarity)}, "
            f"{self._opt_str(rule.author_kind)}, {definition_expr}, {metadata_expr}, "
            f"{self._opt_str(rule.fingerprint)}, {self._opt_str(rule.steward)}, "
            f"{'TRUE' if rule.is_builtin else 'FALSE'}, {self._opt_str(rule.source)}, "
            f"{self._opt_str(rule.created_by)}, now(), {self._opt_str(rule.updated_by)}, now())"
        )
        self._sql.execute(sql)

    def _update(self, rule: RegistryRule) -> None:
        definition_expr = self._sql.json_literal_expr(json.dumps(rule.definition.model_dump(mode="json")))
        metadata_expr = self._sql.json_literal_expr(json.dumps(rule.user_metadata))
        e_rule_id = escape_sql_string(rule.rule_id)
        sql = (
            f"UPDATE {self._table} SET "
            f"  mode = '{escape_sql_string(rule.mode)}', "
            f"  status = '{escape_sql_string(rule.status)}', "
            f"  version = {rule.version}, "
            f"  polarity = {self._opt_str(rule.polarity)}, "
            f"  definition = {definition_expr}, "
            f"  user_metadata = {metadata_expr}, "
            f"  fingerprint = {self._opt_str(rule.fingerprint)}, "
            f"  steward = {self._opt_str(rule.steward)}, "
            f"  updated_by = {self._opt_str(rule.updated_by)}, "
            f"  updated_at = now() "
            f"WHERE rule_id = '{e_rule_id}'"
        )
        self._sql.execute(sql)

    def _write_version_snapshot(self, rule: RegistryRule, user_email: str) -> None:
        """Insert the frozen ``dq_rule_versions`` row for the just-published version.

        ``id`` is a Postgres ``BIGSERIAL`` (auto-generated — omitted from the
        insert) but a Delta ``STRING NOT NULL`` with no default (a schema
        asymmetry inherited from the Phase 2A baseline), so a hex id is
        supplied explicitly on that dialect only.
        """
        definition_expr = self._sql.json_literal_expr(json.dumps(rule.definition.model_dump(mode="json")))
        metadata_expr = self._sql.json_literal_expr(json.dumps(rule.user_metadata))
        e_rule_id = escape_sql_string(rule.rule_id)
        e_user = escape_sql_string(user_email)
        columns = "rule_id, version, definition, polarity, user_metadata, created_by, created_at"
        values = (
            f"'{e_rule_id}', {rule.version}, {definition_expr}, {self._opt_str(rule.polarity)}, "
            f"{metadata_expr}, '{e_user}', now()"
        )
        if self._sql.dialect != "postgres":
            columns = f"id, {columns}"
            values = f"'{uuid4().hex[:16]}', {values}"
        sql = f"INSERT INTO {self._versions_table} ({columns}) VALUES ({values})"
        self._sql.execute(sql)

    def _record_history(
        self,
        rule_id: str | None,
        definition: RuleDefinition | None,
        version: int,
        action: str,
        prev_status: str | None,
        new_status: str | None,
        user_email: str,
    ) -> None:
        """Insert an audit row into ``dq_rules_history`` (best-effort)."""
        try:
            definition_sql = (
                self._sql.json_literal_expr(json.dumps(definition.model_dump(mode="json")))
                if definition is not None
                else "NULL"
            )
            sql = (
                f"INSERT INTO {self._history_table} "
                "(rule_id, definition, version, action, prev_status, new_status, changed_by, changed_at) VALUES "
                f"({self._opt_str(rule_id)}, {definition_sql}, {version}, '{escape_sql_string(action)}', "
                f"{self._opt_str(prev_status)}, {self._opt_str(new_status)}, {self._opt_str(user_email)}, now())"
            )
            self._sql.execute(sql)
        except Exception:
            logger.warning("Failed to record registry history for %s (non-fatal)", rule_id, exc_info=True)

    # ------------------------------------------------------------------
    # Row <-> domain model
    # ------------------------------------------------------------------

    def _row_to_rule(self, row: list[str]) -> RegistryRule:
        definition = self._parse_definition(row[6])
        user_metadata = self._parse_metadata(row[7])
        return RegistryRule(
            rule_id=row[0],
            mode=row[1],  # type: ignore[arg-type]
            status=row[2],  # type: ignore[arg-type]
            version=int(row[3]) if row[3] else 0,
            polarity=row[4],  # type: ignore[arg-type]
            author_kind=row[5],  # type: ignore[arg-type]
            definition=definition,
            user_metadata=user_metadata,
            fingerprint=row[8],
            steward=row[9],
            is_builtin=str(row[10]).lower() == "true" if row[10] is not None else False,
            source=row[11],
            created_by=row[12],
            created_at=row[13],  # type: ignore[arg-type]
            updated_by=row[14],
            updated_at=row[15],  # type: ignore[arg-type]
        )

    def _row_to_version(self, row: list[str]) -> RuleVersion:
        definition = self._parse_definition(row[2])
        user_metadata = self._parse_metadata(row[4])
        return RuleVersion(
            rule_id=row[0],
            version=int(row[1]) if row[1] else 0,
            definition=definition,
            polarity=row[3],  # type: ignore[arg-type]
            user_metadata=user_metadata,
            created_by=row[5],
            created_at=row[6],  # type: ignore[arg-type]
        )

    @staticmethod
    def _parse_definition(raw: str | None) -> RuleDefinition:
        if not raw:
            return RuleDefinition()
        try:
            parsed = json.loads(raw, strict=False)
        except json.JSONDecodeError:
            return RuleDefinition()
        if not isinstance(parsed, dict):
            return RuleDefinition()
        return RuleDefinition.model_validate(parsed)

    @staticmethod
    def _parse_metadata(raw: str | None) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw, strict=False)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
