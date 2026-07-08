"""Rules Registry service (Phase 2B — registry CRUD + two-tier approval gate).

Manages the LIVE ``dq_rules`` template rows and their frozen
``dq_rule_versions`` publish snapshots, per
``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §3.1 and §5.

This is the REGISTRY gate (tier 1 of the two-tier approval model): a
table-agnostic rule definition moves ``draft -> pending_approval ->
approved (published) -> deprecated``, independent of whether/where it is
later *applied* to a monitored table (tier 2 — ``dq_applied_rules``, owned
by ``ApplyRulesService``).

Mirrors :class:`~databricks_labs_dqx_app.backend.services.rules_catalog_service.RulesCatalogService`'s
shape (status machine, history recording, dialect-portable SQL via the
executor helpers) but operates on the registry tables instead of
per-table ``dq_quality_rules``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, cast, get_args
from uuid import uuid4

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.labs.dqx.utils import is_sql_query_safe

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

    # Allowed values for the row-parsing Literal fields, derived from the
    # domain model's own Literal aliases (single source of truth) rather
    # than duplicated string sets — used by ``_parse_mode`` et al. to
    # validate raw OLTP row strings before narrowing them to the typed
    # Literal, per AGENTS.md Critical Rule #6 (no `# type: ignore`).
    _VALID_MODES: frozenset[str] = frozenset(get_args(RuleMode))
    _VALID_STATUS_VALUES: frozenset[str] = frozenset(get_args(RuleStatus))
    _VALID_POLARITIES: frozenset[str] = frozenset(get_args(Polarity))
    _VALID_AUTHOR_KINDS: frozenset[str] = frozenset(get_args(AuthorKind))

    # An ``approved`` rule can be re-submitted for review (approved ->
    # pending_approval): this is how an edit-in-place REVISION of an
    # already-published rule is sent back through the approval gate to be
    # published as vN+1. While it sits in ``pending_approval`` the rule's
    # ``version`` stays N, so the FOLLOWING materializer resolution keeps
    # serving the frozen vN snapshot until approval bumps it (see
    # :meth:`approve` / ``Materializer._iter_rendered_checks``).
    VALID_TRANSITIONS: dict[str, set[str]] = {
        "draft": {"pending_approval"},
        "pending_approval": {"approved", "rejected"},
        "approved": {"deprecated", "pending_approval"},
        "rejected": set(),
        "deprecated": {"approved"},
    }

    # Statuses whose LIVE ``dq_rules`` row may be edited in place (see
    # :meth:`update_draft`). ``approved`` is editable because that is how a
    # revision is authored — the edits stay inert (the frozen vN snapshot
    # keeps serving) until the revision is submitted and re-approved as vN+1.
    # ``pending_approval`` is intentionally NOT editable (it is under review —
    # reject or approve it first), and neither is ``rejected``/``deprecated``
    # (use "Duplicate" / undeprecate respectively).
    EDITABLE_STATUSES: frozenset[str] = frozenset({"draft", "approved"})

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
        self._attach_modified(rules)
        return rules

    def _attach_modified(self, rules: list[RegistryRule]) -> None:
        """Stamp ``modified_since_publish`` on each published rule in *rules*.

        Batches ONE query over ``dq_rule_versions`` for the current snapshot
        of every rule with ``version > 0`` (matched on the exact
        ``(rule_id, version)`` pair), then compares live vs. snapshot content
        via :meth:`_compute_modified`. Unpublished rules keep the default
        ``False``.
        """
        targets = [(r.rule_id, r.version) for r in rules if r.version and r.version > 0]
        if not targets:
            return
        definition = self._sql.select_json_text("definition")
        user_metadata = self._sql.select_json_text("user_metadata")
        pairs = " OR ".join(
            f"(rule_id = '{escape_sql_string(rid)}' AND version = {int(ver)})" for rid, ver in targets
        )
        sql = (
            f"SELECT rule_id, version, {definition} AS definition_json, polarity, "
            f"{user_metadata} AS user_metadata_json FROM {self._versions_table} WHERE {pairs}"  # noqa: S608
        )
        rows = self._sql.query(sql)
        snapshots: dict[str, RuleVersion] = {}
        for row in rows:
            # Pad the created_by/created_at columns the modified check ignores
            # with empty strings so the row matches ``_row_to_version``'s
            # ``list[str]`` shape (empty created_at parses to None).
            snapshot = self._row_to_version([row[0], row[1], row[2], row[3], row[4], "", ""])
            snapshots[snapshot.rule_id] = snapshot
        for rule in rules:
            snapshot = snapshots.get(rule.rule_id)
            if snapshot is not None and snapshot.version == rule.version:
                rule.modified_since_publish = self._compute_modified(rule, snapshot)

    def get_rule(self, rule_id: str) -> RegistryRule | None:
        """Get a single registry rule (with its typed slots/params) by id."""
        return self._get(rule_id)

    def get_rule_by_fingerprint(self, fingerprint: str) -> RegistryRule | None:
        """Get the first registry rule (any status) matching *fingerprint*.

        Used by the built-in seeding path (Phase 2C) to detect whether a
        structurally-identical rule already exists before inserting a
        duplicate — unlike :meth:`_dedup_warning` (which only looks at
        *published* rules and merely warns), this is an exact-identity
        lookup used to skip re-seeding.
        """
        e_fp = escape_sql_string(fingerprint)
        sql = f"SELECT {self._select_cols} FROM {self._table} WHERE fingerprint = '{e_fp}' LIMIT 1"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_rule(rows[0])

    def get_version(self, rule_id: str, version: int) -> RuleVersion | None:
        """Get a specific frozen ``dq_rule_versions`` snapshot by rule id + version number.

        Unlike :meth:`get_rule_with_version` (which always returns the
        LIVE rule's *current* version), this fetches an arbitrary historical
        snapshot — used by the materializer to resolve a ``pinned_version``
        that is older than the rule's current published version.
        """
        return self._get_version(rule_id, version)

    def get_rule_with_version(self, rule_id: str) -> tuple[RegistryRule, RuleVersion | None] | None:
        """Get a registry rule plus its current published snapshot, if any.

        Also stamps ``rule.modified_since_publish`` (see
        :meth:`_compute_modified`) so the detail read path can surface the
        "Modified since vN" state without a second query.
        """
        rule = self._get(rule_id)
        if rule is None:
            return None
        if rule.version <= 0:
            return rule, None
        version = self._get_version(rule.rule_id, rule.version)
        rule.modified_since_publish = self._compute_modified(rule, version)
        return rule, version

    def list_versions(self, rule_id: str) -> list[RuleVersion]:
        """List every frozen ``dq_rule_versions`` snapshot for *rule_id*, newest first.

        Powers the rule's version-history view — the published lineage a
        steward can inspect (each row is an immutable publish snapshot with
        its own definition/tags/author/date).
        """
        definition = self._sql.select_json_text("definition")
        user_metadata = self._sql.select_json_text("user_metadata")
        created_at = self._sql.ts_text("created_at")
        e_rule_id = escape_sql_string(rule_id)
        sql = (
            f"SELECT rule_id, version, {definition} AS definition_json, polarity, "
            f"{user_metadata} AS user_metadata_json, created_by, {created_at} "
            f"FROM {self._versions_table} WHERE rule_id = '{e_rule_id}' ORDER BY version DESC"  # noqa: S608
        )
        rows = self._sql.query(sql)
        return [self._row_to_version(row) for row in rows]

    @staticmethod
    def _compute_modified(rule: RegistryRule, snapshot: RuleVersion | None) -> bool:
        """Return True when *rule*'s live content differs from its current *snapshot*.

        Compares the fields a publish freezes — definition (body/slots/
        parameters/error_message), polarity, and ``user_metadata`` (name/
        description/dimension/severity/free-text tags) — so a metadata-only
        edit (e.g. bumping severity) is flagged too, not just definition
        changes. Returns False for an unpublished rule (``version <= 0`` or no
        snapshot): a draft is not "modified since publish", it just isn't
        published yet.
        """
        if snapshot is None or rule.version <= 0:
            return False
        if rule.polarity != snapshot.polarity:
            return True
        if rule.definition.model_dump(mode="json") != snapshot.definition.model_dump(mode="json"):
            return True
        return rule.user_metadata != snapshot.user_metadata

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

        Raises:
            UnsafeSqlQueryError: *definition*'s SQL body fails
                :meth:`_validate_definition_sql_safety` — the same check
                :meth:`update_draft` applies, so an unsafe query can't be
                persisted via either the initial create or the "save as new
                draft" clone path used when editing a non-draft rule.
        """
        self._validate_definition_sql_safety(mode, definition)
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

    def seed_builtin_rule(
        self,
        definition: RuleDefinition,
        user_metadata: dict[str, Any] | None = None,
        user_email: str = "system",
        steward: str | None = "system",
    ) -> RegistryRule:
        """Create a pre-published, ``is_builtin`` registry rule (Phase 2C seeding).

        Unlike :meth:`create_rule` (which always starts a rule at
        ``draft``/version 0), built-in DQX checks ship already published:
        the row is written directly at ``status='approved'``, ``version=1``,
        with a frozen ``dq_rule_versions`` snapshot — mirroring what
        :meth:`approve` does for a normal rule, without the draft/pending
        detour. Callers are responsible for idempotency (see
        :meth:`get_rule_by_fingerprint` and
        ``backend.builtin_rules_seed.seed_builtin_rules_if_absent``) — this
        method always inserts.
        """
        now = datetime.now(timezone.utc)
        rule = RegistryRule(
            rule_id=uuid4().hex[:16],
            mode="dqx_native",
            status="approved",
            version=1,
            polarity=None,
            author_kind="human",
            definition=definition,
            user_metadata=dict(user_metadata or {}),
            steward=steward,
            is_builtin=True,
            source="builtin",
            created_by=user_email,
            created_at=now,
            updated_by=user_email,
            updated_at=now,
        )
        rule.fingerprint = compute_registry_rule_fingerprint(rule)
        self._insert(rule)
        self._write_version_snapshot(rule, user_email)
        self._record_history(rule.rule_id, rule.definition, rule.version, "seed", None, "approved", user_email)
        logger.info("Seeded built-in registry rule %s (fingerprint=%s)", rule.rule_id, rule.fingerprint)
        return rule

    def update_draft(
        self,
        rule_id: str,
        user_email: str,
        mode: RuleMode | None = None,
        definition: RuleDefinition | None = None,
        polarity: Polarity | None = None,
        user_metadata: dict[str, Any] | None = None,
        steward: str | None = None,
        author_kind: AuthorKind | None = None,
    ) -> RegistryRule:
        """Update a registry rule's LIVE ``dq_rules`` row in place.

        Editable statuses are :data:`EDITABLE_STATUSES` — ``draft`` and
        ``approved``. Editing a ``draft`` works exactly as before. Editing an
        ``approved`` rule is the edit-in-place REVISION path: the live
        definition/tags change but ``version`` stays N and no new
        ``dq_rule_versions`` snapshot is written, so the frozen vN snapshot
        keeps serving everywhere (materialization, draft renders, the
        suggester corpus) until the revision is submitted and re-approved as
        vN+1 (see :meth:`submit`/:meth:`approve`). The rule therefore reads as
        "Modified since vN" (:meth:`_compute_modified`) while it carries
        unpublished edits. A ``pending_approval`` rule is under review and
        cannot be edited (reject or approve it first); ``rejected`` /
        ``deprecated`` rules aren't editable either (duplicate / undeprecate).

        *author_kind* lets an edit-in-place session re-stamp AI provenance
        (e.g. a human accepts an AI-suggested field on an otherwise
        human-authored draft, or vice versa) — omit it to leave the rule's
        existing provenance untouched.
        """
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        if rule.status not in self.EDITABLE_STATUSES:
            raise ValueError(
                f"Cannot edit registry rule '{rule_id}': only {sorted(self.EDITABLE_STATUSES)} rules can be "
                f"edited (current status='{rule.status}')."
            )
        if mode is not None:
            rule.mode = mode
        if definition is not None:
            self._validate_definition_sql_safety(rule.mode, definition)
            rule.definition = definition
        if polarity is not None:
            rule.polarity = polarity
        if user_metadata is not None:
            rule.user_metadata = dict(user_metadata)
        if steward is not None:
            rule.steward = steward
        if author_kind is not None:
            rule.author_kind = author_kind
        rule.fingerprint = compute_registry_rule_fingerprint(rule)
        rule.updated_by = user_email
        self._update(rule)
        self._record_history(
            rule.rule_id, rule.definition, rule.version, "update", rule.status, rule.status, user_email
        )
        logger.info("Updated registry rule %s (status=%s)", rule.rule_id, rule.status)
        return rule

    @staticmethod
    def _validate_definition_sql_safety(mode: RuleMode, definition: RuleDefinition) -> None:
        """Reject a definition whose SQL body fails :func:`is_sql_query_safe`.

        Mirrors the exact SQL-safety check :meth:`materializer.render_check`
        already applies at materialization time — enforcing it here too
        means an unsafe SQL/lowcode predicate or query, or a ``dqx_native``
        check that routes through ``sql_query``/``sql_expression``, is
        rejected at save time rather than only surfacing later when a
        binding is materialized. Slot placeholders (``{{slot}}``) in the raw,
        un-substituted text don't affect the prohibited-statement check.

        Raises:
            UnsafeSqlQueryError: the definition's SQL body is unsafe.
        """
        body = definition.body
        candidates: list[str] = []
        if mode in ("sql", "lowcode"):
            for key in ("sql_query", "predicate"):
                value = body.get(key)
                if isinstance(value, str) and value:
                    candidates.append(value)
        elif mode == "dqx_native":
            function = body.get("function")
            if function in ("sql_query", "sql_expression"):
                arguments = body.get("arguments")
                if isinstance(arguments, dict):
                    for key in ("query", "expression"):
                        value = arguments.get(key)
                        if isinstance(value, str) and value:
                            candidates.append(value)
        for candidate in candidates:
            if not is_sql_query_safe(candidate):
                raise UnsafeSqlQueryError(
                    "The rule's SQL contains prohibited statements (e.g. DROP, INSERT, UPDATE) and cannot be saved."
                )

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
        """Submit a rule for approval (-> pending_approval).

        Valid from ``draft`` (first publish) and from ``approved`` (an
        edit-in-place REVISION of an already-published rule going back through
        the gate to become vN+1). While pending, ``version`` is unchanged so
        the frozen vN snapshot keeps serving followers.
        """
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
        """Reject a pending rule — behaviour depends on whether it was ever published.

        Mirrors the Monitored Tables recovery semantics: rejecting the review
        of a first-time draft (``version == 0``) is terminal
        (``pending_approval -> rejected``), but rejecting a REVISION of an
        already-published rule (``version >= 1``) returns it to ``approved``
        at its current vN — the author's live edits are RETAINED (so it still
        reads as "Modified since vN") and the previously-published vN keeps
        serving throughout, letting the author fix and resubmit rather than
        dead-ending the rule.
        """
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        target: RuleStatus = "approved" if rule.version >= 1 else "rejected"
        return self._transition(rule_id, target, user_email)

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

        Unconditional at this layer — the applied-to-table (409) guard lives
        in the route handler (``routes/v1/registry_rules.py``), which checks
        ``ApplyRulesService.count_applications_for_rule`` before calling this
        method, since that check spans a different service/table.
        """
        rule = self._get(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        e_rule_id = escape_sql_string(rule_id)
        self._sql.execute(f"DELETE FROM {self._table} WHERE rule_id = '{e_rule_id}'")
        self._record_history(rule_id, rule.definition, rule.version, "delete", rule.status, None, user_email)
        logger.info("Deleted registry rule %s (by %s)", rule_id, user_email)

    def delete_builtin_rules(self) -> int:
        """Purge every ``is_builtin`` rule (and its version snapshots) from the registry.

        This is a manual, one-off developer cleanup action — it is not
        invoked at app startup or as part of any migration. It exists to
        remove built-in rules that were auto-seeded by a previous version of
        the app: the Rules Registry now starts empty and is populated only
        by rules authors create or import themselves. There are no
        foreign-key constraints between ``dq_rules`` and ``dq_rule_versions``
        (the link is service-enforced), so ordering is not FK-critical, but
        the version snapshots are deleted too so no orphans remain. Returns
        the number of ``dq_rules`` rows deleted; a no-op (returns 0) once
        run against a registry with no built-in rules left.
        """
        rows = self._sql.query(f"SELECT rule_id FROM {self._table} WHERE is_builtin = TRUE")  # noqa: S608
        rule_ids = [row[0] for row in rows]
        if not rule_ids:
            return 0
        id_list = ", ".join(f"'{escape_sql_string(rule_id)}'" for rule_id in rule_ids)
        self._sql.execute(f"DELETE FROM {self._versions_table} WHERE rule_id IN ({id_list})")
        self._sql.execute(f"DELETE FROM {self._table} WHERE is_builtin = TRUE")
        logger.info("Purged %d built-in registry rule(s)", len(rule_ids))
        return len(rule_ids)

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
            f"  author_kind = {self._opt_str(rule.author_kind)}, "
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
        rule_id = row[0]
        definition = self._parse_definition(row[6])
        user_metadata = self._parse_metadata(row[7])
        return RegistryRule(
            rule_id=rule_id,
            mode=self._parse_mode(row[1], rule_id=rule_id),
            status=self._parse_status(row[2], rule_id=rule_id),
            version=int(row[3]) if row[3] else 0,
            polarity=self._parse_polarity(row[4], rule_id=rule_id),
            author_kind=self._parse_author_kind(row[5], rule_id=rule_id),
            definition=definition,
            user_metadata=user_metadata,
            fingerprint=row[8],
            steward=row[9],
            is_builtin=str(row[10]).lower() == "true" if row[10] is not None else False,
            source=row[11],
            created_by=row[12],
            created_at=self._parse_timestamp(row[13], rule_id=rule_id, field="created_at"),
            updated_by=row[14],
            updated_at=self._parse_timestamp(row[15], rule_id=rule_id, field="updated_at"),
        )

    def _row_to_version(self, row: list[str]) -> RuleVersion:
        rule_id = row[0]
        definition = self._parse_definition(row[2])
        user_metadata = self._parse_metadata(row[4])
        return RuleVersion(
            rule_id=rule_id,
            version=int(row[1]) if row[1] else 0,
            definition=definition,
            polarity=self._parse_polarity(row[3], rule_id=rule_id),
            user_metadata=user_metadata,
            created_by=row[5],
            created_at=self._parse_timestamp(row[6], rule_id=rule_id, field="created_at"),
        )

    @classmethod
    def _parse_mode(cls, value: str | None, *, rule_id: str) -> RuleMode:
        """Validate *value* against :data:`RuleMode`'s allowed members and narrow it.

        Registry rows come back from :meth:`OltpExecutorProtocol.query` as
        plain strings, but ``RegistryRule.mode`` is a ``Literal`` type — a
        raw ``str`` can't be assigned to it without either validating the
        value (done here) or suppressing the type-checker. Real validation
        also protects against a corrupted/unexpected row value: builtin
        checks always insert a valid mode, but this is the read boundary
        where any bad or manually-edited row would otherwise surface only
        as a confusing Pydantic error deep inside ``RegistryRule(...)``.
        """
        if value not in cls._VALID_MODES:
            raise ValueError(f"Registry rule {rule_id!r} has invalid mode {value!r}; expected one of {sorted(cls._VALID_MODES)}")
        return cast(RuleMode, value)

    @classmethod
    def _parse_status(cls, value: str | None, *, rule_id: str) -> RuleStatus:
        """Validate *value* against :data:`RuleStatus`'s allowed members and narrow it. See :meth:`_parse_mode`."""
        if value not in cls._VALID_STATUS_VALUES:
            raise ValueError(
                f"Registry rule {rule_id!r} has invalid status {value!r}; expected one of {sorted(cls._VALID_STATUS_VALUES)}"
            )
        return cast(RuleStatus, value)

    @classmethod
    def _parse_polarity(cls, value: str | None, *, rule_id: str) -> Polarity | None:
        """Validate *value* against :data:`Polarity`'s allowed members and narrow it. ``None`` passes through untouched."""
        if value is None:
            return None
        if value not in cls._VALID_POLARITIES:
            raise ValueError(
                f"Registry rule {rule_id!r} has invalid polarity {value!r}; expected one of {sorted(cls._VALID_POLARITIES)}"
            )
        return cast(Polarity, value)

    @classmethod
    def _parse_author_kind(cls, value: str | None, *, rule_id: str) -> AuthorKind | None:
        """Validate *value* against :data:`AuthorKind`'s allowed members and narrow it. ``None`` passes through untouched."""
        if value is None:
            return None
        if value not in cls._VALID_AUTHOR_KINDS:
            raise ValueError(
                f"Registry rule {rule_id!r} has invalid author_kind {value!r}; "
                f"expected one of {sorted(cls._VALID_AUTHOR_KINDS)}"
            )
        return cast(AuthorKind, value)

    @staticmethod
    def _parse_timestamp(value: str | None, *, rule_id: str, field: str) -> datetime | None:
        """Parse an ISO-ish timestamp string (see :meth:`OltpExecutorProtocol.ts_text`) into a ``datetime``.

        Unlike the Literal fields above, no cast is needed here:
        ``datetime.fromisoformat`` genuinely returns a ``datetime``, so this
        is real coercion rather than a type-checker narrowing trick. A
        malformed value is logged and treated as ``None`` rather than
        failing the whole row — timestamps are informational, not part of
        rule identity or authorization decisions.
        """
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            logger.warning("Registry rule %s has unparsable %s timestamp %r; treating as None", rule_id, field, value)
            return None

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
