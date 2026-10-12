"""Rule-mapping suggester — Rules Registry Phase 4C (design spec §8).

Suggests published registry rules (with a complete slot→column mapping)
for a monitored table: cosine retrieve top-K -> LLM judge -> filter/
dedup/exclude-already-applied.

**Deploy-safe by construction**: every failure path — embedding / AI not
configured, retrieval error, judge error, or an unparsable judge response —
degrades to ``available=False`` with a human-readable *reason*. The route
calling :meth:`RuleSuggester.suggest` always returns HTTP 200; it never
raises for a missing-infra deployment.

The LLM judge's output is treated as **untrusted**: every suggested column
mapping is re-validated against the table's actual columns and the rule's
declared slots before it is returned (see :meth:`RuleSuggester._post_process`).
Published rule descriptions and Unity Catalog column comments also reach the
judge prompt (an injection surface), but that same post-process gate keeps
the blast radius to a plausible-but-wrong suggestion rather than an unsafe
mapping that bypasses column/slot checks.
"""

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
from databricks.sdk.errors.platform import PermissionDenied

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
from databricks_labs_dqx_app.backend.services.rule_retriever import (
    RetrievedRule,
    RuleRetrievalUnavailableError,
    RuleRetriever,
)
from databricks_labs_dqx_app.backend.services.tag_mapping_service import family_for_type as _family_for_type

logger = logging.getLogger(__name__)

DEFAULT_TOP_K = 20  # Per-column retrieval unions per column; cap = max(top_k, top_k*3) in _retrieve_per_column

# Bound how many columns get their own retrieval query. Embedding calls are
# batched via :meth:`RuleRetriever.retrieve_many` since P4C, so the original
# per-column round-trip cost is gone; what remains is a safety cap for
# pathologically wide tables (thousands of columns) to keep the union set
# bounded. Set generously so realistic 100-200 column event/feature tables
# retrieve on every column and reach full recall — measured live on the
# 90-column eval fixture, raising the cap from 64 to 90 took recall from
# 0.750 to 1.000 without moving precision.
MAX_RETRIEVAL_COLUMNS = 512

# Judge output-budget scaling: the mapping response includes one JSON
# suggestion entry per (rule, column) pair the judge accepts, so response
# size grows with column count. The previous fixed budget (2048 tokens)
# truncated mid-JSON at ~50 columns on nano — the failure mode reported by
# field-eng. Base + per-column linear scaling covers realistic widths and
# the hard cap protects the endpoint against a runaway table. See
# ``AIGateway._extract_content`` / ``_looks_truncated_json`` for the
# matching error path when a caller still trips the cap.
_JUDGE_BUDGET_BASE_TOKENS = 2048
_JUDGE_BUDGET_PER_COLUMN_TOKENS = 80
_JUDGE_BUDGET_MAX_TOKENS = 16384

# NL "describe a rule" match path: retrieve against the user prompt (not
# column-built queries), then run the same mapping judge for staging.
DEFAULT_MATCH_TOP_K = 5
MIN_MATCH_SCORE = 0.45  # Cosine floor for match_from_query hits only; re-tune per embedding model

# Human-readable reasons for the genuine "available, but nothing to show"
# outcomes. Kept as constants so the exact wording is asserted by tests and
# stays consistent with the dialog's empty-state copy.
_NO_PUBLISHED_RULES_REASON = "No published rules to suggest from yet. Publish rules to the registry first."
_NO_MATCH_REASON = "No published rules matched this table's columns."
_NO_CLEAN_MAPPING_REASON = "Found related rules, but none mapped cleanly to this table's columns."
_NO_NL_MATCH_REASON = "No published rules closely matched that description."
_NO_NL_CLEAN_MAPPING_REASON = "Found related published rules, but none mapped cleanly to this table's columns."

_JUDGE_SYSTEM_PROMPT = (
    "You are a precise data-quality rule mapping assistant. Given a table's columns (each with a name, "
    "type, family, and optional comment) and a list of candidate published rules (each with input slots that "
    "declare a family), suggest which rules apply to which columns. Favour precision, but do NOT be so "
    "conservative that you omit obviously-correct mappings: propose EVERY mapping that genuinely fits, and "
    "return an empty list only when nothing fits at all.\n"
    "Apply universal data-integrity checks broadly, not narrowly. A single-slot 'any'-family rule that checks "
    "for null/emptiness/presence (e.g. 'Not Null Check', 'Value is present') fits EVERY column that should "
    "always be populated — at minimum every identifier/key column (names ending in _id, _key, or named id/"
    "key/code) and every column a reasonable analyst would consider required (amounts, statuses, timestamps, "
    "names, emails). Emit a separate entry for each such column. A uniqueness rule (e.g. 'Key is unique', "
    "'Unique values in target column') fits every column that identifies a row — primary keys and natural "
    "keys (columns named id, *_id, key, *_key, or clearly unique like email/username/code). Suggest these "
    "universal checks even when the column is domain-specific; a not-null check on order_id is correct.\n"
    "Every slot of a multi-slot rule MUST be filled with a distinct existing column before you suggest it — "
    "never suggest a partial mapping that leaves a slot empty; if you cannot fill all of a rule's slots well, "
    "reject that rule. Only suggest a rule when it is a good structural AND semantic match: a slot's family "
    "should match the column's family (a numeric-family slot maps to a numeric column; a temporal-family slot "
    "to a date/timestamp column; an 'any'-family slot may map to any "
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


@dataclass
class SuggestRulesResult:
    """Result of :meth:`RuleSuggester.suggest`. ``available=False`` covers every degraded path."""

    available: bool
    suggestions: list[RuleSuggestion] = field(default_factory=list)
    reason: str = ""


@dataclass
class MatchedRule:
    """One NL-matched published rule for a monitored table (describe-a-rule flow).

    ``column_mapping`` is set when the mapping judge produced a complete,
    validated slot→column map suitable for staging; ``None`` when retrieval
    found the rule but mapping could not be completed (UI may still show it
    as a weak hit, but staging prefers mapped matches).
    """

    rule_id: str
    rule_name: str | None
    dimension: str | None
    severity: str | None
    score: float
    column_mapping: ColumnMappingGroup | None = None
    explanation: str = ""


@dataclass
class MatchRulesResult:
    """Result of :meth:`RuleSuggester.match_from_query`. Same degrade contract as Suggest."""

    available: bool
    matches: list[MatchedRule] = field(default_factory=list)
    reason: str = ""


class RuleSuggester:
    """Suggests published registry rules for a monitored table's columns.

    Pipeline: build a query from the table + latest profile -> retrieve
    top-K candidates via the injected :class:`RuleRetriever` -> ask the
    :class:`AIGateway`-backed LLM judge to propose slot→column mappings ->
    post-process (drop invalid columns, enforce multi-slot completeness,
    dedup, exclude already-applied mappings).

    Rule descriptions and column comments are included in the judge prompt,
    so they are a prompt-injection surface; :meth:`_post_process` re-checks
    every mapping against real columns and declared slots, so the worst case
    is a wrong suggestion rather than an unsafe accepted mapping.
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
            A :class:`SuggestRulesResult`. ``available=False`` (never an
            exception) covers: unknown binding, embedding endpoint not
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
        columns = await self._resolve_columns(table_fqn, profile)

        try:
            candidates = self._retrieve_per_column(table_fqn, columns)
        except RuleRetrievalUnavailableError as e:
            return SuggestRulesResult(available=False, reason=str(e))
        except Exception:
            logger.warning("Rule retrieval failed for binding %s", binding_id, exc_info=True)
            return SuggestRulesResult(available=False, reason="Rule retrieval failed.")

        if not candidates:
            return SuggestRulesResult(available=True, suggestions=[], reason=_NO_PUBLISHED_RULES_REASON)

        candidate_rules: list[RegistryRule] = []
        for candidate in candidates:
            rule = self._registry.get_rule(candidate.rule_id)
            if rule is not None and rule.status == "approved":
                candidate_rules.append(rule)

        if not candidate_rules:
            return SuggestRulesResult(available=True, suggestions=[], reason=_NO_PUBLISHED_RULES_REASON)

        try:
            judged = await self._judge(candidate_rules, columns, table_fqn, user_email)
        except (AIUnavailableError, AIRateLimitExceededError) as e:
            return SuggestRulesResult(available=False, reason=str(e))
        except PermissionDenied:
            logger.warning("AI judge permission denied for binding %s", binding_id, exc_info=True)
            endpoint = self._ai_gateway.endpoint_name()
            return SuggestRulesResult(
                available=False,
                reason=(
                    f"AI suggestions are unavailable because you don't have permission to run the configured AI model"
                    f" ({endpoint}). Ask an admin to grant you EXECUTE on the serving endpoint."
                ),
            )
        except AIResponseParseError as e:
            # ``AIResponseParseError`` now distinguishes the two shapes: a
            # budget-exhausted response (finish_reason=length OR mid-JSON
            # truncation) reports the real cause, while a genuinely
            # malformed response keeps the generic reason. Propagate
            # ``str(e)`` so the UI can differentiate the two — the previous
            # blanket "unparsable" wording pointed users at the wrong fix.
            logger.warning("AI judge returned an unparsable response for binding %s", binding_id, exc_info=True)
            return SuggestRulesResult(available=False, reason=str(e))
        except Exception:
            logger.warning("AI judge failed for binding %s", binding_id, exc_info=True)
            return SuggestRulesResult(available=False, reason="AI judge failed to produce suggestions.")

        already_applied = self._already_applied_keys(binding_id)
        suggestions = self._post_process(judged, candidate_rules, columns, already_applied)
        if not suggestions:
            # AI ran successfully but produced nothing to add. Distinguish the
            # two zero-result shapes so the dialog can say *why* rather than
            # showing a blank panel: the judge proposed mappings that all
            # failed validation / were already applied, vs the judge found no
            # rule that fits this table's columns at all.
            reason = _NO_CLEAN_MAPPING_REASON if judged else _NO_MATCH_REASON
            return SuggestRulesResult(available=True, suggestions=[], reason=reason)
        return SuggestRulesResult(available=True, suggestions=suggestions)

    async def match_from_query(
        self,
        binding_id: str,
        query: str,
        user_email: str,
        *,
        top_k: int = DEFAULT_MATCH_TOP_K,
        min_score: float = MIN_MATCH_SCORE,
    ) -> MatchRulesResult:
        """Match a natural-language rule description against published registry rules.

        Unlike :meth:`suggest` (which builds retrieval queries from the table's
        columns), this embeds the owner's *query* text directly, ranks published
        rules by cosine similarity, then runs the mapping judge so hits can be
        staged onto the table the same way Suggest does.

        Args:
            binding_id: Monitored table binding to map slots against.
            query: Owner's natural-language description of the desired rule.
            user_email: Caller identity for AIGateway rate limiting / audit.
            top_k: Max retrieval hits to consider (default :data:`DEFAULT_MATCH_TOP_K`).
            min_score: Cosine floor; hits below this are discarded
                (default :data:`MIN_MATCH_SCORE`).

        Returns:
            A :class:`MatchRulesResult`. ``available=False`` covers the same
            degrade paths as :meth:`suggest`. An empty ``matches`` list with
            ``available=True`` means the NL search ran but nothing was close
            enough (or nothing mapped) — the UI then falls through to generate-rule.
        """
        query = (query or "").strip()
        if not query:
            return MatchRulesResult(available=False, reason="Query is required.")

        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            return MatchRulesResult(available=False, reason=f"Monitored table not found: {binding_id}")

        available, reason = self._retriever.is_available()
        if not available:
            return MatchRulesResult(available=False, reason=reason)
        if not self._ai_gateway.is_enabled() or not self._ai_gateway.endpoint_name():
            return MatchRulesResult(available=False, reason="AI features are not configured.")

        table_fqn = detail.table.table_fqn
        profile = self._monitored_tables.get_latest_profile(table_fqn)
        columns = await self._resolve_columns(table_fqn, profile)

        try:
            hits = self._retriever.retrieve(query, top_k)
        except RuleRetrievalUnavailableError as e:
            return MatchRulesResult(available=False, reason=str(e))
        except Exception:
            logger.warning("NL rule match retrieval failed for binding %s", binding_id, exc_info=True)
            return MatchRulesResult(available=False, reason="Rule retrieval failed.")

        # Keep only above-threshold hits; preserve score for ranking / UI.
        score_by_id: dict[str, float] = {}
        for hit in hits:
            if hit.score < min_score:
                continue
            prev = score_by_id.get(hit.rule_id)
            if prev is None or hit.score > prev:
                score_by_id[hit.rule_id] = hit.score

        candidate_rules: list[RegistryRule] = []
        for rule_id, _score in sorted(score_by_id.items(), key=lambda kv: kv[1], reverse=True):
            rule = self._registry.get_rule(rule_id)
            if rule is not None and rule.status == "approved":
                candidate_rules.append(rule)

        if not candidate_rules:
            # Distinguish "corpus empty / nothing above threshold" from infra failure.
            if not hits:
                return MatchRulesResult(available=True, matches=[], reason=_NO_PUBLISHED_RULES_REASON)
            return MatchRulesResult(available=True, matches=[], reason=_NO_NL_MATCH_REASON)

        try:
            judged = await self._judge(candidate_rules, columns, table_fqn, user_email)
        except (AIUnavailableError, AIRateLimitExceededError) as e:
            return MatchRulesResult(available=False, reason=str(e))
        except PermissionDenied:
            logger.warning("AI judge permission denied for NL match on binding %s", binding_id, exc_info=True)
            endpoint = self._ai_gateway.endpoint_name()
            return MatchRulesResult(
                available=False,
                reason=(
                    f"AI suggestions are unavailable because you don't have permission to run the configured AI model"
                    f" ({endpoint}). Ask an admin to grant you EXECUTE on the serving endpoint."
                ),
            )
        except AIResponseParseError as e:
            logger.warning("AI judge returned an unparsable response for NL match on %s", binding_id, exc_info=True)
            return MatchRulesResult(available=False, reason=str(e))
        except Exception:
            logger.warning("AI judge failed for NL match on binding %s", binding_id, exc_info=True)
            return MatchRulesResult(available=False, reason="AI judge failed to produce suggestions.")

        already_applied = self._already_applied_keys(binding_id)
        mapped = self._post_process(judged, candidate_rules, columns, already_applied)
        mapped_by_id: dict[str, RuleSuggestion] = {}
        for suggestion in mapped:
            # Prefer the first (post_process order); one mapping per rule for this UI.
            mapped_by_id.setdefault(suggestion.rule_id, suggestion)

        matches: list[MatchedRule] = []
        for rule in candidate_rules:
            suggestion = mapped_by_id.get(rule.rule_id)
            if suggestion is not None:
                matches.append(
                    MatchedRule(
                        rule_id=rule.rule_id,
                        rule_name=suggestion.rule_name,
                        dimension=suggestion.dimension,
                        severity=suggestion.severity,
                        score=score_by_id[rule.rule_id],
                        column_mapping=suggestion.column_mapping,
                        explanation=suggestion.explanation,
                    )
                )
            else:
                # Retrieval hit without a clean mapping — still surface so the
                # owner can see the related published rule, but column_mapping
                # stays None (not stageable one-click).
                matches.append(
                    MatchedRule(
                        rule_id=rule.rule_id,
                        rule_name=get_rule_name(rule.user_metadata),
                        dimension=get_rule_dimension(rule.user_metadata),
                        severity=get_rule_severity(rule.user_metadata),
                        score=score_by_id[rule.rule_id],
                        column_mapping=None,
                        explanation="",
                    )
                )

        # Prefer mappable matches first, then by score.
        matches.sort(key=lambda m: (m.column_mapping is not None, m.score), reverse=True)

        if not any(m.column_mapping for m in matches):
            # All hits need mapping — still return them, but set a reason so the
            # dialog can explain why one-click staging isn't available.
            return MatchRulesResult(available=True, matches=matches, reason=_NO_NL_CLEAN_MAPPING_REASON)
        return MatchRulesResult(available=True, matches=matches)

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

    def _retrieve_per_column(self, table_fqn: str, columns: list[ColumnMeta]) -> list[RetrievedRule]:
        """Retrieve candidate rules PER COLUMN, then union (dedup, best score wins).

        Prior behaviour embedded ONE blended query for the whole table and took
        the global top-K — so on a wide table a column-specific rule (e.g. an
        email-format rule for an ``email`` column) rarely made the global top-K
        and the judge never saw it for that column. Retrieving top-K per column
        and unioning guarantees each column's strongest matches reach the judge,
        while the judge still decides the final per-column fit. Scores are kept
        so the union can be capped deterministically (highest-scoring first).

        Embedding calls are batched via :meth:`RuleRetriever.retrieve_many`
        (chunked under FMAPI payload limits). Column count is also capped at
        :data:`MAX_RETRIEVAL_COLUMNS` so a very wide table neither crawls nor
        blows a single unbounded batch.

        Falls back to a single table-level query when the column list is empty
        (no profile yet) so behaviour degrades to the old path rather than
        returning nothing.
        """
        if not columns:
            return self._retriever.retrieve(self._build_query_text(table_fqn, columns), self._top_k)

        retrieval_columns = columns
        if len(columns) > MAX_RETRIEVAL_COLUMNS:
            logger.info(
                "Table %s has %d columns; retrieving against the first %d only",
                table_fqn,
                len(columns),
                MAX_RETRIEVAL_COLUMNS,
            )
            retrieval_columns = columns[:MAX_RETRIEVAL_COLUMNS]

        query_texts = [self._build_column_query_text(table_fqn, column) for column in retrieval_columns]
        best_by_rule: dict[str, RetrievedRule] = {}
        for hits in self._retriever.retrieve_many(query_texts, self._top_k):
            for hit in hits:
                existing = best_by_rule.get(hit.rule_id)
                if existing is None or hit.score > existing.score:
                    best_by_rule[hit.rule_id] = hit
        # Cap the unioned candidate set so a very wide table can't hand the judge
        # an unbounded prompt; keep the highest-scoring across all columns. The
        # cap scales with top_k so more columns still surface more candidates.
        union_cap = max(self._top_k, self._top_k * 3)
        return sorted(best_by_rule.values(), key=lambda c: c.score, reverse=True)[:union_cap]

    @staticmethod
    def _build_query_text(table_fqn: str, columns: list[ColumnMeta]) -> str:
        parts = [f"table: {table_fqn}"]
        for column in columns:
            line = f"- {column.name} ({column.type or 'unknown'}, {column.family})"
            if column.comment:
                line += f": {column.comment}"
            parts.append(line)
        return "\n".join(parts)

    @staticmethod
    def _build_column_query_text(table_fqn: str, column: ColumnMeta) -> str:
        """Single-column query text so retrieval matches rules to THIS column's
        name/type/family/comment (email → email-format rule, id → not-null/unique)."""
        line = f"table: {table_fqn}\ncolumn: {column.name} ({column.type or 'unknown'}, {column.family})"
        if column.comment:
            line += f": {column.comment}"
        return line

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
        # Response size grows with column count (one entry per accepted
        # column mapping); scale the budget accordingly so a 90-column table
        # doesn't blow the fixed cap mid-JSON. Bounded at the top end so a
        # pathological table can't request an unreasonable endpoint budget.
        max_tokens = min(
            _JUDGE_BUDGET_MAX_TOKENS,
            _JUDGE_BUDGET_BASE_TOKENS + _JUDGE_BUDGET_PER_COLUMN_TOKENS * len(columns),
        )
        content = await self._ai_gateway.query(
            user_email=user_email,
            purpose="suggest-rules",
            messages=[
                {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
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

            # Reject same-column multi-slot mappings: two or more distinct slot keys bound to the
            # same column value (e.g. {"start_ts": "order_ts", "end_ts": "order_ts"}) are always
            # wrong for a comparison/range rule and indicate the LLM forced a rule onto one column.
            if len(set(mapping.values())) < len(mapping):
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
