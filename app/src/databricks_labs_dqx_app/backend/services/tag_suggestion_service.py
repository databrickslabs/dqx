"""Tag-based rule matching for the apply-on-tag feature (suggest AND auto-apply).

Computes, for a monitored table, which published tag-mapped rules match its
columns — then either surfaces them as accept-to-attach SUGGESTIONS
(:meth:`TagSuggestionService.suggest`, when the ``tag_auto_apply`` toggle is
off) or AUTO-ATTACHES them (:meth:`TagSuggestionService.apply_matches`, when the
toggle is on, called from the register / open-table hooks).

Both paths share one OBO-authed match computation: for every published
(approved) rule that declares ``slot_tags``, the pure resolver
(:func:`tag_mapping_service.resolve`) runs against the table's columns to
produce the FULL Cartesian product of matching slot→column groups (one group per
valid assignment of a real column to every slot — a 1-column rule matching N
tagged columns yields N groups; a 2-column rule yields A·B), then any group
already applied (matched on ``(rule_id, mapping_hash)`` — mirroring the AI
suggester's exclusion keys) is dropped. Column reads go through an injected callable, built
over the CALLING USER's OBO credentials: this is deliberate and load-bearing —
the app service principal has no grant on user catalogs, so an SP-authed read of
``information_schema.column_tags`` returns nothing; running the match as the user
sees exactly the tags they can, which is what makes auto-apply work at all.

Best-effort by construction: a read failure or an unknown binding degrades to
``[]``/``0``; neither :meth:`suggest` nor :meth:`apply_matches` raises.
"""

from __future__ import annotations

import collections.abc
import logging
from dataclasses import dataclass

from databricks_labs_dqx_app.backend.registry_models import (
    ColumnMappingGroup,
    RegistryRule,
    compute_mapping_hash,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
    get_slot_tags,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.tag_mapping_service import ColumnInfo, resolve

logger = logging.getLogger(__name__)


@dataclass
class TagRuleSuggestion:
    """One tag-matched, accept-to-attach rule suggestion for a monitored table.

    ``column_mapping`` is ONE slot→column group the resolver produced (a rule
    that matches several columns/combinations yields several of these, one per
    group); ``explanation`` is a short factual string naming the tags that
    matched (never marketing copy).
    """

    rule_id: str
    rule_name: str | None
    dimension: str | None
    severity: str | None
    column_mapping: ColumnMappingGroup
    explanation: str


class TagSuggestionService:
    """Builds tag-based rule suggestions for a monitored table (apply-on-tag, OFF path)."""

    def __init__(
        self,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        apply_rules: ApplyRulesService,
        app_settings: AppSettingsService,
        read_columns: collections.abc.Callable[[str], list[ColumnInfo]],
    ) -> None:
        """Build the tag-matcher.

        Args:
            registry: Registry service (published rules + slots/slot_tags).
            monitored_tables: Monitored-table service; resolves binding→table_fqn.
            apply_rules: Apply/map service — the source of already-applied
                ``(rule_id, mapping_hash)`` exclusion keys, and the sink for
                :meth:`apply_matches` (``attach_auto_mapping``).
            app_settings: Reads the ``tag_auto_apply`` toggle deciding whether
                matches auto-attach (:meth:`apply_matches`) or merely surface as
                suggestions (:meth:`suggest`).
            read_columns: Column-tag reader returning a table's columns as
                :class:`ColumnInfo` (name, type_name, governed tags). OBO-authed
                for this user-facing path so it respects the caller's Unity
                Catalog permissions — this is what lets auto-apply see the tags
                the app service principal cannot. Injected as a plain callable so
                this service stays free of SDK details and is trivially testable.
        """
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._apply_rules = apply_rules
        self._app_settings = app_settings
        self._read_columns = read_columns

    def suggest(self, binding_id: str) -> list[TagRuleSuggestion]:
        """Tag matches for a monitored table, as accept-to-attach suggestions.

        For every published rule that declares slot_tags, resolve its tags against
        the table's columns (full Cartesian product — every matching column /
        combination), excluding (rule_id, mapping_hash) pairs already applied.
        Returns one suggestion per matching group. Best-effort: returns [] on read
        failure (never raises).

        When the ``tag_auto_apply`` admin toggle is ON, returns ``[]`` — the
        matches are auto-attached (:meth:`apply_matches`, from the register /
        open-table hooks) and then show in the table's normal applied-rule list,
        so there is nothing left to *suggest*.

        Args:
            binding_id: The monitored table binding to suggest rules for.

        Returns:
            One :class:`TagRuleSuggestion` per matching, not-yet-applied rule;
            ``[]`` when auto-apply is on.
        """
        if self._app_settings.get_tag_auto_apply():
            return []
        return self._matches(binding_id)

    def apply_matches(self, binding_id: str, user_email: str) -> int:
        """Auto-attach every tag match for a table (auto-apply ON path).

        Runs the SAME OBO-read match computation as :meth:`suggest` — so it sees
        exactly the tags the calling user can see, which is what makes auto-apply
        work where an app-service-principal reconcile cannot (the SP has no grant
        on user catalogs) — then attaches each match via
        :meth:`ApplyRulesService.attach_auto_mapping` (add-only, origin-stamped,
        idempotent, honouring user-removed tombstones). No-op returning ``0``
        when the ``tag_auto_apply`` toggle is off. Best-effort: an attach failure
        for one match is logged and skipped; never raises.

        Args:
            binding_id: The monitored table binding to attach matches to.
            user_email: Attributed as ``created_by`` on any new attachment.

        Returns:
            The number of mapping groups newly attached.
        """
        if not self._app_settings.get_tag_auto_apply():
            return 0
        attached = 0
        for match in self._matches(binding_id):
            try:
                result = self._apply_rules.attach_auto_mapping(
                    binding_id, match.rule_id, [match.column_mapping], user_email
                )
            except Exception:
                # rule_id is a controlled identifier; never log raw tag values.
                logger.warning(
                    "apply-on-tag: attach failed for rule %s on binding %s", match.rule_id, binding_id, exc_info=True
                )
                continue
            if result is not None:
                attached += 1
        if attached:
            logger.info("apply-on-tag: auto-attached %d tag-matched rule(s) to binding %s", attached, binding_id)
        return attached

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _matches(self, binding_id: str) -> list[TagRuleSuggestion]:
        """Compute tag matches for a table (shared by suggest + apply_matches).

        Every matching group (Cartesian product) per published tag-mapped rule,
        excluding groups already applied. Best-effort: [] on unknown binding or
        read failure.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            return []
        columns = self._read_columns_safe(detail.table.table_fqn)
        if not columns:
            return []
        already_applied = self._already_applied_keys(binding_id)
        matches: list[TagRuleSuggestion] = []
        for rule, slot_tags in self._tag_mapped_rules():
            matches.extend(self._suggestions_for_rule(rule, slot_tags, columns, already_applied))
        return matches

    def _tag_mapped_rules(self) -> list[tuple[RegistryRule, dict[str, list[str]]]]:
        """List every published rule that carries a non-empty slot_tags map."""
        pairs: list[tuple[RegistryRule, dict[str, list[str]]]] = []
        for rule in self._registry.list_rules(status="approved"):
            slot_tags = get_slot_tags(rule.user_metadata)
            if slot_tags:
                pairs.append((rule, slot_tags))
        return pairs

    def _read_columns_safe(self, table_fqn: str) -> list[ColumnInfo]:
        try:
            return self._read_columns(table_fqn)
        except Exception:
            # table_fqn is a controlled identifier (safe to log); degrade to []
            # so a raising reader never surfaces a 500 on this best-effort path.
            logger.warning("Failed to read columns for monitored table %s", table_fqn, exc_info=True)
            return []

    def _already_applied_keys(self, binding_id: str) -> set[tuple[str, str]]:
        """Mirror ``RuleSuggester._already_applied_keys`` — per-group exclusion keys."""
        keys: set[tuple[str, str]] = set()
        for applied_rule in self._apply_rules.list_applied(binding_id):
            for group in applied_rule.column_mapping:
                keys.add((applied_rule.rule_id, compute_mapping_hash([group])))
        return keys

    def _suggestions_for_rule(
        self,
        rule: RegistryRule,
        slot_tags: dict[str, list[str]],
        columns: list[ColumnInfo],
        already_applied: set[tuple[str, str]],
    ) -> list[TagRuleSuggestion]:
        """Resolve one rule to EVERY matching column-mapping group (Cartesian product).

        ``resolve(single=False)`` yields one group per valid assignment of a real
        column to every slot — so a 1-column rule matching N tagged columns
        produces N groups, and a 2-column rule matching A×B candidates produces
        A·B groups (a column can't fill two slots of the same group). Groups
        already applied to this table (``(rule_id, mapping_hash)``) are dropped.
        Returns ``[]`` when nothing fits or the resolve fails.
        """
        try:
            groups = resolve(rule.definition.slots, slot_tags, columns)
        except Exception:
            # Never log raw tag values; the rule id is a controlled identifier.
            logger.warning("Tag-suggestion resolve failed for rule %s", rule.rule_id, exc_info=True)
            return []
        out: list[TagRuleSuggestion] = []
        for group in groups:
            if (rule.rule_id, compute_mapping_hash([group])) in already_applied:
                continue
            out.append(
                TagRuleSuggestion(
                    rule_id=rule.rule_id,
                    rule_name=get_rule_name(rule.user_metadata),
                    dimension=get_rule_dimension(rule.user_metadata),
                    severity=get_rule_severity(rule.user_metadata),
                    column_mapping=group,
                    explanation=self._explanation(slot_tags),
                )
            )
        return out

    @staticmethod
    def _explanation(slot_tags: dict[str, list[str]]) -> str:
        """Build a short factual explanation naming the matched tags (deduped, sorted).

        Formatted as ``Matched tag <a>, <b>`` so it reads naturally as the AI
        suggest-rules dialog's per-suggestion reason line (never marketing copy).
        """
        tags = sorted({tag for tags in slot_tags.values() for tag in tags})
        return "Matched tag " + ", ".join(tags) if tags else "Matched tag"
