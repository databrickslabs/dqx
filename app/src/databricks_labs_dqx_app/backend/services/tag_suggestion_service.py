"""Tag-based rule suggestions for a monitored table.

Computes, for a monitored table, which published tag-mapped rules match its
columns and surfaces them as accept-to-attach SUGGESTIONS
(:meth:`TagSuggestionService.suggest`). Suggestions are always on; automatic
attachment is done by rule policies (:mod:`policy_apply_service`).

For every published (approved) rule that declares ``slot_tags``, the pure
resolver (:func:`tag_mapping_service.resolve`) runs against the table's columns
to produce the FULL Cartesian product of matching slot→column groups (one group
per valid assignment of a real column to every slot — a 1-column rule matching
N tagged columns yields N groups; a 2-column rule yields A·B), then any group
already applied (matched on ``(rule_id, mapping_hash)`` — mirroring the AI
suggester's exclusion keys) is dropped. Column reads go through an injected
callable, built over the CALLING USER's OBO credentials: the app service
principal has no grant on user catalogs, so an SP-authed read of
``information_schema.column_tags`` returns nothing; running the match as the
user sees exactly the tags they can.

Best-effort by construction: a read failure or an unknown binding degrades to
``[]``; :meth:`suggest` never raises.
"""

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
    """Builds tag-based rule suggestions for a monitored table."""

    def __init__(
        self,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        apply_rules: ApplyRulesService,
        read_columns: collections.abc.Callable[[str], list[ColumnInfo]],
    ) -> None:
        """Build the tag-matcher.

        Args:
            registry: Registry service (published rules + slots/slot_tags).
            monitored_tables: Monitored-table service; resolves binding→table_fqn.
            apply_rules: Apply/map service — the source of already-applied
                ``(rule_id, mapping_hash)`` exclusion keys.
            read_columns: Column-tag reader returning a table's columns as
                :class:`ColumnInfo` (name, type_name, governed tags). OBO-authed
                for this user-facing path so it respects the caller's Unity
                Catalog permissions — so it sees the tags the app service
                principal cannot. Injected as a plain callable so
                this service stays free of SDK details and is trivially testable.
        """
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._apply_rules = apply_rules
        self._read_columns = read_columns

    def suggest(self, binding_id: str) -> list[TagRuleSuggestion]:
        """Tag matches for a monitored table, as accept-to-attach suggestions.

        For every published rule that declares slot_tags, resolve its tags against
        the table's columns (full Cartesian product — every matching column /
        combination), excluding (rule_id, mapping_hash) pairs already applied.
        Returns one suggestion per matching group. Best-effort: returns [] on read
        failure (never raises).

        Args:
            binding_id: The monitored table binding to suggest rules for.

        Returns:
            One :class:`TagRuleSuggestion` per matching, not-yet-applied rule.
        """
        return self._matches(binding_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _matches(self, binding_id: str) -> list[TagRuleSuggestion]:
        """Compute tag matches for a table.

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
