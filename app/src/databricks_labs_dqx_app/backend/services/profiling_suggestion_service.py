"""Profile-page rule suggestions — surface the DQX profiler's generated checks
on a monitored table's Profile view and let a user apply one to the table.

The profiler (``GET /monitored-tables/{binding_id}/profile`` ->
``LatestProfile.generated_rules``) proposes concrete checks for the profiled
table. This service turns those into applicable registry-rule suggestions
*without side effects on read* (:meth:`list_suggestions` only introspects), and
applies a chosen one on demand (:meth:`apply_suggestion`):

* :func:`build_profiling_rule` resolves one profiler check into a table-agnostic
  registry-rule template plus the ``{slot -> column}`` binding, validating the
  function against ``CHECK_FUNC_REGISTRY`` and any SQL argument via
  ``is_sql_query_safe`` — an unmappable/unsafe check is skipped;
* on apply, :meth:`RegistryService.match_or_create_approved_rule` resolves the
  template to an existing approved rule by structural fingerprint or, absent
  one, creates + approves a ``dqx_native`` rule (idempotent — never spawns a
  duplicate, fully audited), then :meth:`ApplyRulesService.apply_rule` binds it
  to the monitored table.

This is the dqlake-style placement: profiler suggestions live on the Profile
page, NOT folded into the AI "Suggest rules" dialog.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    ColumnMappingGroup,
    compute_mapping_hash,
    get_rule_description,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    ApplyRulesService,
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.profiling_rule_builder import ProfilingRuleCandidate

logger = logging.getLogger(__name__)


class BindingNotFoundError(LookupError):
    """Raised when the target monitored table binding does not exist."""


class SuggestionNotApplicableError(ValueError):
    """Raised when a profiler check can't be resolved to an applicable registry rule.

    Covers both an out-of-range index and a check that :func:`build_profiling_rule`
    rejects (unregistered function, unmappable column slot, unsafe SQL argument),
    or a same-fingerprint rule that exists but isn't approved (so this flow won't
    duplicate or auto-approve it).
    """


@dataclass
class ProfilingSuggestion:
    """One applicable profiler-derived rule suggestion for the Profile page.

    ``index`` is the position of the source check in the latest profile's
    ``generated_rules`` — the stable handle :meth:`ProfilingSuggestionService.apply_suggestion`
    takes so the client never has to echo back a (trusted) rule definition.
    """

    index: int
    function: str
    rule_name: str | None
    description: str | None
    dimension: str | None
    severity: str | None
    column_mapping: ColumnMappingGroup = field(default_factory=dict)


@dataclass
class SuggestionApplyFailure:
    """One profiler suggestion that could not be applied during a batch apply.

    ``index`` mirrors the source-check position (as returned by
    :meth:`ProfilingSuggestionService.list_suggestions`); ``reason`` is a
    human-readable, non-sensitive explanation safe to surface to the client.
    """

    index: int
    reason: str


@dataclass
class EnrichedAppliedRule:
    """An un-persisted :class:`AppliedRule` paired with its registry rule's display tags.

    The display fields (name/dimension/severity) are derived from the candidate
    metadata at apply time so the frontend can render the rule name immediately
    on staging — without a separate registry join — matching the
    ``AppliedRuleOut.from_summary`` path used for persisted rows.
    """

    applied_rule: AppliedRule
    rule_name: str | None = None
    rule_dimension: str | None = None
    rule_severity: str | None = None


@dataclass
class BatchApplyResult:
    """Outcome of :meth:`ProfilingSuggestionService.apply_suggestions`.

    Partial success is expected and reported explicitly: ``applied`` holds the
    successfully bound rules and ``failed`` the per-index failures, so the
    caller can toast an accurate count without one bad suggestion aborting the
    rest.
    """

    applied: list[EnrichedAppliedRule] = field(default_factory=list)
    failed: list[SuggestionApplyFailure] = field(default_factory=list)


class ProfilingSuggestionService:
    """Lists and applies the profiler's generated checks as registry-rule suggestions."""

    def __init__(
        self,
        monitored_tables: MonitoredTableService,
        registry: RegistryService,
        apply_rules: ApplyRulesService,
    ) -> None:
        self._monitored_tables = monitored_tables
        self._registry = registry
        self._apply_rules = apply_rules

    @staticmethod
    def _build_candidate(check: object) -> "ProfilingRuleCandidate | None":
        """Introspect one profiler check into a registry-rule candidate (no side effects).

        ``build_profiling_rule`` is imported lazily: it pulls in the
        check-function introspection chain (``builtin_rules_seed`` ->
        ``routes.v1.check_functions`` -> ``dependencies``), so a top-level
        import here would form a circular import at app startup.
        """
        if not isinstance(check, dict):
            return None
        from databricks_labs_dqx_app.backend.profiling_rule_builder import build_profiling_rule

        return build_profiling_rule(check)

    def list_suggestions(self, binding_id: str) -> list[ProfilingSuggestion]:
        """Return the profiler's applicable rule suggestions for *binding_id*.

        Side-effect-free: each generated check is introspected with
        :func:`build_profiling_rule` (no rule is created or approved here — that
        happens only on :meth:`apply_suggestion`). Checks that can't be mapped
        safely and completely to a registry rule are skipped, as are ones that
        already resolve to a rule applied to this binding (recognised via a
        read-only fingerprint lookup — still no rule is created).

        Raises:
            BindingNotFoundError: *binding_id* does not exist.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise BindingNotFoundError(f"Monitored table not found: {binding_id}")
        profile = self._monitored_tables.get_latest_profile(detail.table.table_fqn)
        if profile is None:
            return []

        already_applied = self._already_applied_keys(binding_id)
        seen: set[tuple[str, str]] = set()
        out: list[ProfilingSuggestion] = []
        for index, check in enumerate(profile.generated_rules):
            candidate = self._build_candidate(check)
            if candidate is None:
                continue
            mapping_hash = compute_mapping_hash([candidate.mapping])
            # Recognise an already-applied (or duplicate-in-profile) suggestion
            # WITHOUT minting a rule: look up an existing approved rule with the
            # same structural fingerprint (read-only) and check its binding.
            existing = self._registry.find_approved_rule_for_definition(candidate.definition)
            if existing is not None:
                key = (existing.rule_id, mapping_hash)
                if key in already_applied or key in seen:
                    continue
                seen.add(key)
            out.append(
                ProfilingSuggestion(
                    index=index,
                    function=candidate.function,
                    rule_name=get_rule_name(candidate.metadata),
                    description=get_rule_description(candidate.metadata),
                    dimension=get_rule_dimension(candidate.metadata),
                    severity=get_rule_severity(candidate.metadata),
                    column_mapping=candidate.mapping,
                )
            )
        return out

    def _already_applied_keys(self, binding_id: str) -> set[tuple[str, str]]:
        """Return ``(rule_id, mapping_hash)`` keys already applied to *binding_id* (read-only)."""
        keys: set[tuple[str, str]] = set()
        for applied in self._apply_rules.list_applied(binding_id):
            for group in applied.column_mapping:
                keys.add((applied.rule_id, compute_mapping_hash([group])))
        return keys

    def apply_suggestion(self, binding_id: str, index: int, user_email: str) -> EnrichedAppliedRule:
        """Stage the profiler suggestion at *index* as an un-persisted :class:`EnrichedAppliedRule`.

        Resolves the source check to a registry rule via
        :meth:`RegistryService.match_or_create_approved_rule` (match an existing
        approved rule by structural fingerprint, else create + approve one —
        idempotent and audited) and constructs the in-memory
        :class:`AppliedRule` via
        :meth:`ApplyRulesService.build_applied_rule` WITHOUT persisting the
        table binding. The rule's display tags (name/dimension/severity) are
        derived from the candidate metadata and carried on the returned
        :class:`EnrichedAppliedRule` so the frontend can render them
        immediately on staging.

        The caller (route layer → frontend) is responsible for staging the
        returned row in the Apply Rules tab's unsaved selection — just as if
        the user had hand-picked the rule and not yet pressed Save.

        Args:
            binding_id: The monitored table binding to stage the suggestion for.
            index: Position of the source check in the latest profile's
                ``generated_rules`` (as returned by :meth:`list_suggestions`).
            user_email: The actor staging the suggestion — attributed as the
                rule author and in the registry audit log.

        Returns:
            An un-persisted :class:`EnrichedAppliedRule` with the profiler-derived
            column mapping and display tags.

        Raises:
            BindingNotFoundError: *binding_id* does not exist.
            SuggestionNotApplicableError: *index* is out of range, the check
                can't be mapped to a registry rule, or a same-fingerprint rule
                exists but isn't approved.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise BindingNotFoundError(f"Monitored table not found: {binding_id}")
        profile = self._monitored_tables.get_latest_profile(detail.table.table_fqn)
        generated = profile.generated_rules if profile is not None else []
        return self._apply_at(binding_id, generated, index, user_email)

    def apply_suggestions(self, binding_id: str, indices: list[int], user_email: str) -> BatchApplyResult:
        """Stage every profiler suggestion in *indices* as un-persisted :class:`AppliedRule` rows.

        Resolves the binding + latest profile once, then stages each selected
        suggestion through the same single-item path as :meth:`apply_suggestion`
        (:meth:`RegistryService.match_or_create_approved_rule` — match an
        existing approved rule by structural fingerprint, else create + approve
        one, idempotent and audited — followed by
        :meth:`ApplyRulesService.build_applied_rule`, which constructs the row
        in memory WITHOUT persisting the binding). Duplicate indices are
        collapsed so a rule template is never created twice for the same
        selection.

        The returned :class:`BatchApplyResult` contains un-persisted
        :class:`AppliedRule` rows. The route layer passes these directly to the
        frontend, which stages them in the Apply Rules tab's unsaved selection —
        exactly as if the user had hand-picked those rules but not yet pressed
        Save.

        Robust to partial failure: a suggestion that can't be staged (no longer
        available, unmappable, or a same-fingerprint rule that isn't published)
        is recorded in :attr:`BatchApplyResult.failed` and does not abort the
        rest — rule creation stays idempotent, so re-running is safe.

        Args:
            binding_id: The monitored table binding to stage the suggestions for.
            indices: Positions of the source checks in the latest profile's
                ``generated_rules`` (as returned by :meth:`list_suggestions`).
            user_email: The actor staging the suggestions — attributed as the
                rule author and in the registry audit log.

        Returns:
            A :class:`BatchApplyResult` with the staged (un-persisted) rules
            and per-index failures.

        Raises:
            BindingNotFoundError: *binding_id* does not exist.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise BindingNotFoundError(f"Monitored table not found: {binding_id}")
        profile = self._monitored_tables.get_latest_profile(detail.table.table_fqn)
        generated = profile.generated_rules if profile is not None else []

        result: BatchApplyResult = BatchApplyResult()
        seen: set[int] = set()
        for index in indices:
            if index in seen:
                continue
            seen.add(index)
            try:
                result.applied.append(self._apply_at(binding_id, generated, index, user_email))
            except (SuggestionNotApplicableError, MappingIncompleteError, RuleNotPublishedError, RuntimeError) as e:
                result.failed.append(SuggestionApplyFailure(index=index, reason=str(e)))
        return result

    def _apply_at(
        self, binding_id: str, generated: Sequence[object], index: int, user_email: str
    ) -> EnrichedAppliedRule:
        """Resolve-or-create + approve the suggestion at *index* and stage it (shared apply path).

        This is the single point that mints/approves a registry rule template
        for the profile-page suggestion flow (via
        :meth:`RegistryService.match_or_create_approved_rule`) — used by both
        :meth:`apply_suggestion` and :meth:`apply_suggestions`. The table
        binding is NOT persisted here; the caller receives an in-memory
        :class:`EnrichedAppliedRule` to stage in the frontend's unsaved
        selection. Display tags (name/dimension/severity) are derived from the
        candidate metadata so the frontend renders the rule name immediately —
        matching the ``AppliedRuleOut.from_summary`` path used for persisted rows.
        """
        if index < 0 or index >= len(generated):
            raise SuggestionNotApplicableError("Profiler suggestion is no longer available.")

        candidate = self._build_candidate(generated[index])
        if candidate is None:
            raise SuggestionNotApplicableError("This profiler suggestion can't be applied to the table.")

        rule, _created = self._registry.match_or_create_approved_rule(
            candidate.definition, candidate.metadata, user_email
        )
        if rule is None:
            raise SuggestionNotApplicableError(
                "A matching rule exists but isn't published yet, so it can't be applied."
            )
        applied_rule = self._apply_rules.build_applied_rule(binding_id, rule.rule_id, [candidate.mapping], user_email)
        return EnrichedAppliedRule(
            applied_rule=applied_rule,
            rule_name=get_rule_name(candidate.metadata),
            rule_dimension=get_rule_dimension(candidate.metadata),
            rule_severity=get_rule_severity(candidate.metadata),
        )
