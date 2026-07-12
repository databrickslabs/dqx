"""Rule-mapping suggester — Rules Registry Phase 4C (design spec §8).

Suggests published registry rules (with a complete slot→column mapping)
for a monitored table: Vector Search retrieve top-K -> LLM judge -> filter/
dedup/exclude-already-applied.

**Deploy-safe by construction**: every failure path — Vector Search /
embedding / AI not configured, retrieval error, judge error, or an
unparsable judge response — degrades to ``available=False`` with a
human-readable *reason*. The route calling :meth:`RuleSuggester.suggest`
always returns HTTP 200; it never raises for a missing-infra deployment.

The LLM judge's output is treated as **untrusted**: every suggested column
mapping is re-validated against the table's actual columns and the rule's
declared slots before it is returned (see :meth:`RuleSuggester._post_process`).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from databricks_labs_dqx_app.backend.registry_models import (
    ColumnMappingGroup,
    RegistryRule,
    compute_mapping_hash,
    get_rule_description,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIGateway,
    AIRateLimitExceededError,
    AIResponseParseError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, TableColumn
from databricks_labs_dqx_app.backend.services.monitored_table_service import LatestProfile, MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_retriever import RuleRetrievalUnavailableError, RuleRetriever

logger = logging.getLogger(__name__)

DEFAULT_TOP_K = 8

# Maps a Unity Catalog column type (leading token, upper-cased) to the slot
# ``family`` vocabulary the registry uses (see ``registry_models.SlotFamily``:
# numeric / text / temporal / boolean / array / any). Mirrors dqlake's family mapping
# so the judge can align a slot's declared family with a column's real type.
_TYPE_FAMILY: dict[str, str] = {
    "TINYINT": "numeric",
    "SMALLINT": "numeric",
    "INT": "numeric",
    "INTEGER": "numeric",
    "BIGINT": "numeric",
    "LONG": "numeric",
    "FLOAT": "numeric",
    "DOUBLE": "numeric",
    "DECIMAL": "numeric",
    "STRING": "text",
    "VARCHAR": "text",
    "CHAR": "text",
    "DATE": "temporal",
    "TIMESTAMP": "temporal",
    "TIMESTAMP_NTZ": "temporal",
    "BOOLEAN": "boolean",
    "ARRAY": "array",
}


def _family_for_type(type_name: str) -> str:
    """Classify a UC column ``type_name`` into a registry slot family."""
    head = (type_name or "").upper().split("(")[0].split("<")[0].strip()
    return _TYPE_FAMILY.get(head, "any")

# Human-readable reasons for the genuine "available, but nothing to show"
# outcomes. Kept as constants so the exact wording is asserted by tests and
# stays consistent with the dialog's empty-state copy.
_NO_PUBLISHED_RULES_REASON = "No published rules to suggest from yet. Publish rules to the registry first."
_NO_MATCH_REASON = "No published rules matched this table's columns."
_NO_CLEAN_MAPPING_REASON = "Found related rules, but none mapped cleanly to this table's columns."

# Attribution carried on every profiler-derived suggestion (verbatim, per the
# feature spec). Surfaced by the dialog to distinguish a profiling suggestion
# from an AI-judged one.
_PROFILING_REASON = "Suggested based on DQX profiling"

_JUDGE_SYSTEM_PROMPT = (
    "You are a conservative data-quality rule mapping assistant. Given a table's columns (each with a name, "
    "type, family, and optional comment) and a list of candidate published rules (each with input slots that "
    "declare a family), suggest which rules apply to which columns. Be conservative: it is correct to reject "
    "most candidates and to return an empty list when nothing genuinely fits.\n"
    "Every slot of a multi-slot rule MUST be filled with a distinct existing column before you suggest it — "
    "never suggest a partial mapping that leaves a slot empty; if you cannot fill all of a rule's slots well, "
    "reject that rule. Only suggest a rule when it is a good structural AND semantic match: a slot's family "
    "should match the column's family (a numeric-family slot maps to a numeric column; a temporal-family slot "
    "to a date/timestamp column; an array-family slot to an array column; an 'any'-family slot may map to any "
    "column), and the column's name/comment should be consistent with what the rule checks. Never invent a "
    "column name that is not in the provided column list.\n"
    "A single rule MAY genuinely apply to several different column choices (for example a one-slot not-null "
    "rule that fits both budget_amount and actual_spend, or a two-slot comparison that fits more than one "
    "valid pair of columns). When that is the case, emit ONE separate suggestion entry per (rule, complete "
    "column mapping) — each entry maps ALL of the rule's slots to one specific set of columns. Only do this "
    "when each mapping is genuinely a good fit; do not pad.\n"
    "Each 'explanation' MUST justify WHY the rule genuinely FITS that specific column (or columns): ground it "
    "in the column's name, type, role, and semantics together with WHAT the rule actually checks, and explain "
    "the connection between the two. Do NOT restate what the column is for, and do NOT write a circular or "
    "tautological sentence that just repeats the column's purpose or the rule's name. When the same rule "
    "appears in several entries, each explanation must name and justify its own column(s). Do NOT close an "
    "explanation by asserting that the column is suitable/appropriate/ideal/a good fit or candidate FOR the "
    "check (e.g. '...making it suitable for a uniqueness check') — that the rule fits is already implied by "
    "suggesting it, so such a clause is empty filler; give the substantive reason and stop. A close name "
    "match between a slot and a column (e.g. slot 'email' → column 'vendor_email') is concrete supporting "
    "evidence — mention it alongside the semantic reason, never as the only justification. Keep explanations "
    "to one or two plain sentences.\n"
    "Return STRICT JSON only, no prose, of the exact form: "
    '{"suggestions": [{"rule_id": "...", "mapping": {"slot_name": "column_name"}, '
    '"explanation": "short grounded reason"}]}. '
    'If nothing is a good match, return {"suggestions": []}.'
)


@dataclass
class ColumnMeta:
    """One resolved target-table column the suggester matches rules against.

    ``type`` is the raw Unity Catalog type name and ``family`` is its
    registry slot-family classification (see :func:`_family_for_type`); both
    are empty/``"any"`` when the column list falls back to profile names.
    """

    name: str
    type: str = ""
    family: str = "any"
    comment: str | None = None


@dataclass
class RuleSuggestion:
    """One validated, complete slot→column mapping suggestion for a monitored table."""

    rule_id: str
    rule_name: str | None
    dimension: str | None
    severity: str | None
    column_mapping: ColumnMappingGroup
    explanation: str = ""
    # Source attribution. Empty for AI-judged suggestions (which surface an
    # ``explanation`` instead); set to ``_PROFILING_REASON`` for profiler-derived
    # suggestions so the dialog can label their origin.
    reason: str = ""


@dataclass
class SuggestRulesResult:
    """Result of :meth:`RuleSuggester.suggest`. ``available=False`` covers every degraded path."""

    available: bool
    suggestions: list[RuleSuggestion] = field(default_factory=list)
    reason: str = ""


class RuleSuggester:
    """Suggests published registry rules for a monitored table's columns.

    Pipeline: build a query from the table + latest profile -> retrieve
    top-K candidates via the injected :class:`RuleRetriever` -> ask the
    :class:`AIGateway`-backed LLM judge to propose slot→column mappings ->
    post-process (drop invalid columns, enforce multi-slot completeness,
    dedup, exclude already-applied mappings).
    """

    def __init__(
        self,
        monitored_tables: MonitoredTableService,
        registry: RegistryService,
        apply_rules: ApplyRulesService,
        retriever: RuleRetriever,
        ai_gateway: AIGateway,
        discovery: DiscoveryService,
        top_k: int = DEFAULT_TOP_K,
    ) -> None:
        self._monitored_tables = monitored_tables
        self._registry = registry
        self._apply_rules = apply_rules
        self._retriever = retriever
        self._ai_gateway = ai_gateway
        self._discovery = discovery
        self._top_k = top_k

    async def suggest(self, binding_id: str, user_email: str) -> SuggestRulesResult:
        """Suggest rule/mapping candidates for the monitored table *binding_id*.

        Args:
            binding_id: The monitored table binding to suggest rules for.
            user_email: Caller identity, forwarded to the AIGateway for
                rate limiting and audit.

        Returns:
            A :class:`SuggestRulesResult`. Profiler-derived suggestions
            (independent of AI/Vector Search) are merged with the AI-judged
            ones. ``available=False`` (never an exception) covers the
            AI-degraded paths — unknown binding, embedding endpoint not
            configured, AI not configured/rate-limited, retrieval failure, or
            judge failure — UNLESS the profiler path produced suggestions, in
            which case those still surface with ``available=True``.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            return SuggestRulesResult(available=False, reason=f"Monitored table not found: {binding_id}")

        table_fqn = detail.table.table_fqn
        profile = self._monitored_tables.get_latest_profile(table_fqn)
        already_applied = self._already_applied_keys(binding_id)

        # Profiler-derived suggestions are computed first and independently of
        # AI/Vector Search — the profiler already proposed concrete checks, so
        # they reach the UI even on a deployment with no AI infra.
        profiling = self._profiling_suggestions(profile, already_applied, user_email)

        ai_available, ai_suggestions, ai_reason = await self._ai_suggestions(
            detail, table_fqn, profile, already_applied, user_email
        )

        merged = self._merge_suggestions(ai_suggestions, profiling)
        if merged:
            return SuggestRulesResult(available=True, suggestions=merged)
        # Nothing to show: preserve the AI path's degraded-vs-empty semantics so
        # the dialog can explain *why* (missing infra vs no matches) rather than
        # showing a blank panel.
        return SuggestRulesResult(available=ai_available, suggestions=[], reason=ai_reason)

    @staticmethod
    def _merge_suggestions(
        ai_suggestions: list[RuleSuggestion], profiling: list[RuleSuggestion]
    ) -> list[RuleSuggestion]:
        """Concatenate AI + profiling suggestions, dropping profiling duplicates.

        Dedup is by the resolved ``(rule_id, mapping_hash)`` — the AI path wins
        so its grounded explanation is kept when the same rule+mapping was found
        both ways.
        """
        ai_keys = {(s.rule_id, compute_mapping_hash([s.column_mapping])) for s in ai_suggestions}
        merged = list(ai_suggestions)
        for suggestion in profiling:
            key = (suggestion.rule_id, compute_mapping_hash([suggestion.column_mapping]))
            if key in ai_keys:
                continue
            merged.append(suggestion)
        return merged

    async def _ai_suggestions(
        self,
        detail: object,
        table_fqn: str,
        profile: LatestProfile | None,
        already_applied: set[tuple[str, str]],
        user_email: str,
    ) -> tuple[bool, list[RuleSuggestion], str]:
        """Run the AI retrieve -> judge -> post-process pipeline.

        Returns ``(available, suggestions, reason)`` instead of raising, so
        :meth:`suggest` can merge these with the profiler-derived suggestions.
        ``available=False`` covers every degraded AI path (Vector Search /
        embedding / AI not configured, retrieval error, judge error, unparsable
        judge response); ``reason`` carries the human-readable explanation.
        """
        available, reason = self._retriever.is_available()
        if not available:
            return False, [], reason
        if not self._ai_gateway.is_enabled() or not self._ai_gateway.endpoint_name():
            return False, [], "AI features are not configured."

        columns = await self._resolve_columns(table_fqn, profile)
        query_text = self._build_query_text(table_fqn, columns)

        try:
            candidates = self._retriever.retrieve(query_text, self._top_k)
        except RuleRetrievalUnavailableError as e:
            return False, [], str(e)
        except Exception:
            logger.warning("Rule retrieval failed for %s", table_fqn, exc_info=True)
            return False, [], "Rule retrieval failed."

        if not candidates:
            return True, [], _NO_PUBLISHED_RULES_REASON

        candidate_rules: list[RegistryRule] = []
        for candidate in candidates:
            rule = self._registry.get_rule(candidate.rule_id)
            if rule is not None and rule.status == "approved":
                candidate_rules.append(rule)

        if not candidate_rules:
            return True, [], _NO_PUBLISHED_RULES_REASON

        try:
            judged = await self._judge(candidate_rules, columns, table_fqn, user_email)
        except (AIUnavailableError, AIRateLimitExceededError) as e:
            return False, [], str(e)
        except AIResponseParseError:
            logger.warning("AI judge returned an unparsable response for %s", table_fqn, exc_info=True)
            return False, [], "AI judge returned an unparsable response."
        except Exception:
            logger.warning("AI judge failed for %s", table_fqn, exc_info=True)
            return False, [], "AI judge failed to produce suggestions."

        suggestions = self._post_process(judged, candidate_rules, columns, already_applied)
        if not suggestions:
            # Distinguish the two zero-result shapes: the judge proposed
            # mappings that all failed validation / were already applied, vs the
            # judge found no rule that fits this table's columns at all.
            return True, [], (_NO_CLEAN_MAPPING_REASON if judged else _NO_MATCH_REASON)
        return True, suggestions, ""

    def _profiling_suggestions(
        self,
        profile: LatestProfile | None,
        already_applied: set[tuple[str, str]],
        user_email: str,
    ) -> list[RuleSuggestion]:
        """Turn the latest profile's generated checks into registry-rule suggestions.

        Each profiler check is mapped to a registry rule via
        :func:`build_profiling_rule` + :meth:`RegistryService.match_or_create_approved_rule`
        (match an existing approved rule by structural fingerprint, else
        silently create + approve one — idempotent, so re-running never spawns
        duplicates). Suggestions already staged/applied for this binding, and
        duplicates within the profile, are excluded. Best-effort per check: a
        check that can't be mapped or persisted is skipped, never fatal.
        """
        if not isinstance(profile, LatestProfile):
            return []
        # Imported lazily: this module is imported (via ``models``) during app
        # startup, and ``profiling_rule_builder`` pulls in the check-function
        # introspection chain that imports ``models`` back — a top-level import
        # here would be a circular import.
        from databricks_labs_dqx_app.backend.profiling_rule_builder import build_profiling_rule

        seen: set[tuple[str, str]] = set()
        out: list[RuleSuggestion] = []
        for check in profile.generated_rules:
            if not isinstance(check, dict):
                continue
            candidate = build_profiling_rule(check)
            if candidate is None:
                continue
            try:
                rule, _created = self._registry.match_or_create_approved_rule(
                    candidate.definition, candidate.metadata, user_email
                )
            except Exception:
                logger.warning("Failed to match-or-create a profiling registry rule", exc_info=True)
                continue
            if rule is None:
                continue
            mapping_hash = compute_mapping_hash([candidate.mapping])
            key = (rule.rule_id, mapping_hash)
            if key in seen or key in already_applied:
                continue
            seen.add(key)
            out.append(
                RuleSuggestion(
                    rule_id=rule.rule_id,
                    rule_name=get_rule_name(rule.user_metadata),
                    dimension=get_rule_dimension(rule.user_metadata),
                    severity=get_rule_severity(rule.user_metadata),
                    column_mapping=candidate.mapping,
                    explanation="",
                    reason=_PROFILING_REASON,
                )
            )
        return out

    # ------------------------------------------------------------------
    # Query construction
    # ------------------------------------------------------------------

    async def _resolve_columns(self, table_fqn: str, profile: LatestProfile | None) -> list[ColumnMeta]:
        """Resolve the table's columns for matching — live UC schema first.

        Mirrors dqlake: read the real column set (name, type, family,
        comment) from Unity Catalog via the caller's OBO client, so matching
        works even for a table that has never been profiled in the app. Only
        when the UC read yields nothing (table dropped, insufficient
        permissions, or a non-3-part fqn) does it fall back to the latest
        profile's column names — the previous behaviour, which silently
        produced zero columns (and therefore zero suggestions) for any table
        without a prior profiling run. Best-effort: never raises.
        """
        parts = table_fqn.split(".")
        uc_columns: list[TableColumn] = []
        if len(parts) == 3:
            try:
                uc_columns = await self._discovery.get_table_columns_async(parts[0], parts[1], parts[2])
            except Exception:
                logger.info("Could not read UC columns for a monitored table; falling back to profile", exc_info=True)
        if uc_columns:
            return [
                ColumnMeta(
                    name=column.name,
                    type=column.type_name,
                    family=_family_for_type(column.type_name),
                    comment=column.comment,
                )
                for column in uc_columns
                if column.name
            ]
        return [ColumnMeta(name=name) for name in self._profile_columns(profile)]

    @staticmethod
    def _profile_columns(profile: LatestProfile | None) -> list[str]:
        """Return the column names known for this table from its latest profile.

        DQX profiler ``summary_stats`` (persisted as ``LatestProfile.summary``)
        is keyed by column name, so the dict's keys ARE the column list.
        Falls back to scanning ``generated_rules`` argument columns when the
        summary is empty/absent (e.g. an older or partial profiling run).
        """
        if profile is None:
            return []
        if isinstance(profile.summary, dict) and profile.summary:
            return sorted(profile.summary.keys())
        columns: set[str] = set()
        for rule in profile.generated_rules:
            if not isinstance(rule, dict):
                continue
            arguments = rule.get("check", {})
            arguments = arguments.get("arguments", {}) if isinstance(arguments, dict) else {}
            if not isinstance(arguments, dict):
                continue
            col = arguments.get("column")
            if isinstance(col, str):
                columns.add(col)
            cols = arguments.get("columns")
            if isinstance(cols, list):
                columns.update(c for c in cols if isinstance(c, str))
        return sorted(columns)

    @staticmethod
    def _build_query_text(table_fqn: str, columns: list[ColumnMeta]) -> str:
        parts = [f"table: {table_fqn}"]
        for column in columns:
            line = f"- {column.name} ({column.type or 'unknown'}, {column.family})"
            if column.comment:
                line += f": {column.comment}"
            parts.append(line)
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # LLM judge
    # ------------------------------------------------------------------

    async def _judge(
        self,
        candidate_rules: list[RegistryRule],
        columns: list[ColumnMeta],
        table_fqn: str,
        user_email: str,
    ) -> list[dict[str, Any]]:
        candidates_payload = [
            {
                "rule_id": rule.rule_id,
                "name": get_rule_name(rule.user_metadata) or rule.rule_id,
                "description": get_rule_description(rule.user_metadata) or "",
                "dimension": get_rule_dimension(rule.user_metadata),
                "severity": get_rule_severity(rule.user_metadata),
                "slots": [{"name": slot.name, "family": slot.family} for slot in rule.definition.slots],
            }
            for rule in candidate_rules
        ]
        columns_payload = [
            {"name": column.name, "type": column.type, "family": column.family, "comment": column.comment}
            for column in columns
        ]
        user_prompt = json.dumps(
            {"table": table_fqn, "columns": columns_payload, "candidate_rules": candidates_payload},
            sort_keys=True,
        )
        content = await self._ai_gateway.query(
            user_email=user_email,
            purpose="suggest-rules",
            messages=[
                {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )
        parsed = self._ai_gateway.parse_json_object(content)
        suggestions = parsed.get("suggestions")
        return suggestions if isinstance(suggestions, list) else []

    # ------------------------------------------------------------------
    # Post-processing (untrusted LLM output -> validated suggestions)
    # ------------------------------------------------------------------

    def _already_applied_keys(self, binding_id: str) -> set[tuple[str, str]]:
        applied = self._apply_rules.list_applied(binding_id)
        keys: set[tuple[str, str]] = set()
        for applied_rule in applied:
            for group in applied_rule.column_mapping:
                keys.add((applied_rule.rule_id, compute_mapping_hash([group])))
        return keys

    @staticmethod
    def _post_process(
        judged: list[dict[str, Any]],
        candidate_rules: list[RegistryRule],
        columns: list[ColumnMeta],
        already_applied: set[tuple[str, str]],
    ) -> list[RuleSuggestion]:
        rules_by_id = {rule.rule_id: rule for rule in candidate_rules}
        column_names = {column.name for column in columns}
        seen: set[tuple[str, str]] = set()
        out: list[RuleSuggestion] = []
        for item in judged:
            if not isinstance(item, dict):
                continue
            rule_id = item.get("rule_id")
            mapping = item.get("mapping")
            explanation = item.get("explanation")
            if not isinstance(rule_id, str) or not isinstance(mapping, dict):
                continue
            rule = rules_by_id.get(rule_id)
            if rule is None:
                continue

            # Untrusted LLM output: every mapped value must be a real column.
            if not mapping or not all(isinstance(v, str) and v in column_names for v in mapping.values()):
                continue

            # Multi-slot completeness: mapping keys must exactly equal the rule's slot names.
            expected_slots = {slot.name for slot in rule.definition.slots}
            if set(mapping.keys()) != expected_slots:
                continue

            mapping_typed: ColumnMappingGroup = {str(k): str(v) for k, v in mapping.items()}
            mapping_hash = compute_mapping_hash([mapping_typed])
            key = (rule_id, mapping_hash)
            if key in seen or key in already_applied:
                continue
            seen.add(key)

            out.append(
                RuleSuggestion(
                    rule_id=rule_id,
                    rule_name=get_rule_name(rule.user_metadata),
                    dimension=get_rule_dimension(rule.user_metadata),
                    severity=get_rule_severity(rule.user_metadata),
                    column_mapping=mapping_typed,
                    explanation=explanation if isinstance(explanation, str) else "",
                )
            )
        return out
