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
from databricks_labs_dqx_app.backend.services.monitored_table_service import LatestProfile, MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_retriever import RuleRetrievalUnavailableError, RuleRetriever

logger = logging.getLogger(__name__)

DEFAULT_TOP_K = 8

_JUDGE_SYSTEM_PROMPT = (
    "You are a conservative data-quality rule mapping assistant. Given a table's columns and a list of "
    "candidate published rules, suggest which rules apply to which columns. Every slot of a multi-slot rule "
    "MUST be filled with a distinct existing column before you suggest it — never suggest a partial mapping. "
    "Only suggest a rule when you are confident it is a good structural match (e.g. a numeric-family slot maps "
    "to a numeric column, a temporal-family slot maps to a date/timestamp column). Never invent a column name "
    "that is not in the provided column list.\n"
    "Return STRICT JSON only, no prose, of the exact form: "
    '{"suggestions": [{"rule_id": "...", "mapping": {"slot_name": "column_name"}, '
    '"explanation": "short grounded reason"}]}. '
    'If nothing is a good match, return {"suggestions": []}.'
)


@dataclass
class RuleSuggestion:
    """One validated, complete slot→column mapping suggestion for a monitored table."""

    rule_id: str
    rule_name: str | None
    dimension: str | None
    severity: str | None
    column_mapping: ColumnMappingGroup
    explanation: str = ""


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
        top_k: int = DEFAULT_TOP_K,
    ) -> None:
        self._monitored_tables = monitored_tables
        self._registry = registry
        self._apply_rules = apply_rules
        self._retriever = retriever
        self._ai_gateway = ai_gateway
        self._top_k = top_k

    async def suggest(self, binding_id: str, user_email: str) -> SuggestRulesResult:
        """Suggest rule/mapping candidates for the monitored table *binding_id*.

        Args:
            binding_id: The monitored table binding to suggest rules for.
            user_email: Caller identity, forwarded to the AIGateway for
                rate limiting and audit.

        Returns:
            A :class:`SuggestRulesResult`. ``available=False`` (never an
            exception) covers: unknown binding, Vector Search/embedding not
            configured, AI not configured/rate-limited, retrieval failure,
            or judge failure.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            return SuggestRulesResult(available=False, reason=f"Monitored table not found: {binding_id}")

        available, reason = self._retriever.is_available()
        if not available:
            return SuggestRulesResult(available=False, reason=reason)
        if not self._ai_gateway.is_enabled() or not self._ai_gateway.endpoint_name():
            return SuggestRulesResult(available=False, reason="AI features are not configured.")

        table_fqn = detail.table.table_fqn
        profile = self._monitored_tables.get_latest_profile(table_fqn)
        columns = self._profile_columns(profile)
        query_text = self._build_query_text(table_fqn, columns)

        try:
            candidates = self._retriever.retrieve(query_text, self._top_k)
        except RuleRetrievalUnavailableError as e:
            return SuggestRulesResult(available=False, reason=str(e))
        except Exception:
            logger.warning("Rule retrieval failed for binding %s", binding_id, exc_info=True)
            return SuggestRulesResult(available=False, reason="Rule retrieval failed.")

        if not candidates:
            return SuggestRulesResult(available=True, suggestions=[], reason="No candidate rules found.")

        candidate_rules: list[RegistryRule] = []
        for candidate in candidates:
            rule = self._registry.get_rule(candidate.rule_id)
            if rule is not None and rule.status == "approved":
                candidate_rules.append(rule)

        if not candidate_rules:
            return SuggestRulesResult(available=True, suggestions=[], reason="No published candidate rules found.")

        try:
            judged = await self._judge(candidate_rules, columns, table_fqn, user_email)
        except (AIUnavailableError, AIRateLimitExceededError) as e:
            return SuggestRulesResult(available=False, reason=str(e))
        except AIResponseParseError:
            logger.warning("AI judge returned an unparsable response for binding %s", binding_id, exc_info=True)
            return SuggestRulesResult(available=False, reason="AI judge returned an unparsable response.")
        except Exception:
            logger.warning("AI judge failed for binding %s", binding_id, exc_info=True)
            return SuggestRulesResult(available=False, reason="AI judge failed to produce suggestions.")

        already_applied = self._already_applied_keys(binding_id)
        suggestions = self._post_process(judged, candidate_rules, columns, already_applied)
        return SuggestRulesResult(available=True, suggestions=suggestions)

    # ------------------------------------------------------------------
    # Query construction
    # ------------------------------------------------------------------

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
    def _build_query_text(table_fqn: str, columns: list[str]) -> str:
        parts = [f"table: {table_fqn}"]
        if columns:
            parts.append("columns: " + ", ".join(columns))
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # LLM judge
    # ------------------------------------------------------------------

    async def _judge(
        self,
        candidate_rules: list[RegistryRule],
        columns: list[str],
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
        user_prompt = json.dumps(
            {"table": table_fqn, "columns": columns, "candidate_rules": candidates_payload},
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
        columns: list[str],
        already_applied: set[tuple[str, str]],
    ) -> list[RuleSuggestion]:
        rules_by_id = {rule.rule_id: rule for rule in candidate_rules}
        column_names = set(columns)
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
