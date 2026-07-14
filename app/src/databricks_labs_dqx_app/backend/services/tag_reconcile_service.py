"""Tag-reconcile orchestrator for the apply-on-tag feature.

Attaches tag-mapped registry rules to monitored tables. This is the side-effect
boundary that ties together the PURE resolver (``tag_mapping_service.resolve``),
the registry (published rules + their slot→tags map), the monitored-table
listing, and ``ApplyRulesService.apply_rule`` (the idempotent attach). All reads
of a table's columns/tags happen through an injected callable so this service
carries no SDK details and is trivially fakeable in unit tests.

Every attach goes through ``ApplyRulesService.attach_auto_mapping``, which stamps
new rows ``{origin: tag_auto}`` (see ``registry_models.ORIGIN_KEY`` /
``ORIGIN_TAG_AUTO``) so the reconcile loop only ever owns auto-created
attachments. ``attach_auto_mapping`` is ADD-ONLY: when a row already exists for
the natural key it is returned unchanged, so hand-applied rows are never touched
and re-running any reconcile method is idempotent (an existing auto row keeps its
pin/severity intact).

Every method is a no-op returning ``0`` when the ``tag_auto_apply`` app setting
is off (the default) — the feature only feeds rule suggestions in that mode.
"""

from __future__ import annotations

import collections.abc
import logging

from databricks_labs_dqx_app.backend.registry_models import (
    RegistryRule,
    get_slot_tags,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.tag_mapping_service import ColumnInfo, resolve

logger = logging.getLogger(__name__)


class TagReconcileService:
    """Attaches tag-mapped registry rules to monitored tables (apply-on-tag)."""

    def __init__(
        self,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        apply_rules: ApplyRulesService,
        app_settings: AppSettingsService,
        read_columns: collections.abc.Callable[[str], list[ColumnInfo]],
    ) -> None:
        """Build the orchestrator.

        Args:
            registry: Registry service (published rules + slots/slot_tags).
            monitored_tables: Monitored-table listing service.
            apply_rules: Apply/map service — the idempotent auto-attach.
            app_settings: Settings service; gates the whole feature on
                ``get_tag_auto_apply()``.
            read_columns: SP-authed reader returning a table's columns as
                :class:`ColumnInfo` (name, type_name, class.* + other tags).
                Injected as a plain callable so this service stays free of SDK
                details and is trivially testable with a fake.
        """
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._apply_rules = apply_rules
        self._app_settings = app_settings
        self._read_columns = read_columns

    # ------------------------------------------------------------------
    # Public reconcile surface
    # ------------------------------------------------------------------

    def reconcile_rule(self, rule_id: str, user_email: str) -> int:
        """Attach one published tag-mapped rule across every monitored table.

        No-op returning ``0`` when tag-auto-apply is off, when *rule_id* is
        missing/not approved, or when it carries no slot tags.

        Args:
            rule_id: The registry rule to reconcile.
            user_email: Attributed as ``created_by`` on any new attachment.

        Returns:
            The number of mapping groups attached across all tables.
        """
        if not self.is_enabled():
            return 0
        rule = self._registry.get_rule(rule_id)
        slot_tags = self._tag_mapped(rule)
        if rule is None or slot_tags is None:
            return 0
        attached = 0
        for summary in self._monitored_tables.list_monitored_tables():
            binding = summary.table
            attached += self._attach_rule_to_table(rule, slot_tags, binding.binding_id, binding.table_fqn, user_email)
        return attached

    def reconcile_table(self, binding_id: str, table_fqn: str, user_email: str) -> int:
        """Attach every published tag-mapped rule to one monitored table.

        No-op returning ``0`` when tag-auto-apply is off. Reads the table's
        columns once and reuses them across all rules.

        Args:
            binding_id: The monitored table binding to attach to.
            table_fqn: The table's fully-qualified name (SP-read for columns).
            user_email: Attributed as ``created_by`` on any new attachment.

        Returns:
            The number of mapping groups attached to this table.
        """
        if not self.is_enabled():
            return 0
        columns = self._read_columns_safe(table_fqn)
        attached = 0
        for rule, slot_tags in self._tag_mapped_rules():
            attached += self._attach_group_matches(rule, slot_tags, columns, binding_id, table_fqn, user_email)
        return attached

    def sweep(self, user_email: str) -> int:
        """Reconcile all published tag-mapped rules across all monitored tables.

        No-op returning ``0`` when tag-auto-apply is off. Reads each table's
        columns once and reuses them across every rule, so the SP column reads
        cost N (tables), not N×M (tables×rules).

        Args:
            user_email: Attributed as ``created_by`` on any new attachment.

        Returns:
            The total number of mapping groups attached.
        """
        if not self.is_enabled():
            return 0
        rules = self._tag_mapped_rules()
        if not rules:
            return 0
        attached = 0
        for summary in self._monitored_tables.list_monitored_tables():
            binding = summary.table
            columns = self._read_columns_safe(binding.table_fqn)
            for rule, slot_tags in rules:
                attached += self._attach_group_matches(
                    rule, slot_tags, columns, binding.binding_id, binding.table_fqn, user_email
                )
        return attached

    def is_enabled(self) -> bool:
        """Return whether tag-auto-apply is on (the feature gate).

        Public so callers can cheaply skip preparatory work (e.g. resolving a
        binding_id per table in bulk-register) before invoking a reconcile
        method — every reconcile method already no-ops internally when this is
        ``False``, so the check is purely an optimization, never a correctness
        gate.
        """
        return self._app_settings.get_tag_auto_apply()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _tag_mapped(rule: RegistryRule | None) -> dict[str, list[str]] | None:
        """Return *rule*'s non-empty slot_tags when it is an approved tag-mapped rule, else None."""
        if rule is None or rule.status != "approved":
            return None
        slot_tags = get_slot_tags(rule.user_metadata)
        return slot_tags or None

    def _tag_mapped_rules(self) -> list[tuple[RegistryRule, dict[str, list[str]]]]:
        """List every published rule that carries a non-empty slot_tags map."""
        pairs: list[tuple[RegistryRule, dict[str, list[str]]]] = []
        for rule in self._registry.list_rules(status="approved"):
            slot_tags = self._tag_mapped(rule)
            if slot_tags is not None:
                pairs.append((rule, slot_tags))
        return pairs

    def _read_columns_safe(self, table_fqn: str) -> list[ColumnInfo]:
        try:
            return self._read_columns(table_fqn)
        except Exception:
            # table_fqn is a controlled identifier (safe to log); the reader
            # already degrades to [] on SDK failure, but guard here too so a
            # raising fake/impl never aborts a whole sweep.
            logger.warning("Failed to read columns for monitored table %s", table_fqn, exc_info=True)
            return []

    def _attach_rule_to_table(
        self,
        rule: RegistryRule,
        slot_tags: dict[str, list[str]],
        binding_id: str,
        table_fqn: str,
        user_email: str,
    ) -> int:
        """Read one table's columns (guarded) and attach *rule*'s group matches to it."""
        columns = self._read_columns_safe(table_fqn)
        return self._attach_group_matches(rule, slot_tags, columns, binding_id, table_fqn, user_email)

    def _attach_group_matches(
        self,
        rule: RegistryRule,
        slot_tags: dict[str, list[str]],
        columns: list[ColumnInfo],
        binding_id: str,
        table_fqn: str,
        user_email: str,
    ) -> int:
        """Resolve *rule* against *columns* and attach one applied row per mapping group.

        Each (rule, table) unit is guarded so one failure never aborts the rest.
        Returns the number of groups successfully attached.
        """
        try:
            groups = resolve(rule.definition.slots, slot_tags, columns)
            attached = 0
            for group in groups:
                # One resolved group == one attachment == one applied row, so
                # each call passes a single-element ``[group]``. This keeps
                # ``mapping_hash`` per-group, which is what makes re-runs
                # idempotent (an identical group finds its existing row and is
                # left untouched rather than orphaned when the table's columns
                # later change). ``attach_auto_mapping`` is add-only: it never
                # mutates an existing row, so a steward's hand-applied row with
                # the same mapping is preserved (pin/severity/metadata intact).
                self._apply_rules.attach_auto_mapping(binding_id, rule.rule_id, [group], user_email)
                attached += 1
            return attached
        except Exception:
            # Never log raw tag values; the rule id, binding id, and table_fqn
            # are controlled identifiers, and the exception carries the detail.
            logger.warning(
                "Tag-reconcile failed for rule %s on table %s (binding %s)",
                rule.rule_id,
                table_fqn,
                binding_id,
                exc_info=True,
            )
            return 0
