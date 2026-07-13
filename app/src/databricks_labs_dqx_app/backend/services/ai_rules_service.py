from __future__ import annotations

import json
import logging
import re
from importlib.resources import files
from pathlib import Path
from typing import Any, ClassVar, TypedDict

import yaml
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks  # type: ignore[import-untyped]
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.llm.llm_core import _filter_unsafe_sql_rules
from databricks.labs.dqx.llm.llm_utils import get_required_check_functions_definitions
from databricks.labs.dqx.utils import is_sql_query_safe

from databricks_labs_dqx_app.backend.config import AI_SAMPLE_ROW_LIMIT, conf
from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway, AIResponseParseError

logger = logging.getLogger(__name__)

_SYSTEM_TEMPLATE = """\
You are a data quality rule generator. Given a table schema and a business description, \
generate DQX data quality rules using the available check functions.

Return ONLY a JSON object with two fields:
  - "quality_rules": a valid JSON array of rule objects
  - "reasoning": a short explanation of why these rules were chosen

Rule format:
  {{"criticality": "error"|"warn", "check": {{"function": "<name>", "arguments": {{...}}}}, "filter": "<optional SQL>"}}

Guidelines:
- Use double quotes for all JSON keys and string values.
- For string literal values inside check arguments wrap them in single quotes.
- In SQL filter expressions use single quotes for string literals and capitalise SQL keywords.
- Use "filter" only when the rule applies to a subset of rows.

Available check functions:
{available_functions}"""

# Two-pass rule proposal prompt: the caller tries "dqx_native" first (a single
# named check function + arguments), falling back to "sql" (a predicate SQL
# expression) only if no dqx_native check fits the description. Both modes
# share the same response shape so `AiRulesService._validate_and_repair_proposal`
# can validate either uniformly.
_RULE_PROPOSAL_SYSTEM_TEMPLATE = """\
You are a data quality rule design assistant for the DQX Rules Registry. Given a business \
description of a data quality requirement (and optional table schema/sample data), propose \
ONE reusable rule in "{mode}" mode.

Return ONLY a JSON object with these fields:
  - "name": a short human-readable rule name (max 80 chars)
  - "description": a one-sentence description of what the rule checks
  - "dimension": one of Validity, Completeness, Accuracy, Consistency, Uniqueness, Timeliness
  - "severity": one of Low, Medium, High, Critical
  - "polarity": "pass" or "fail" (use "pass" for dqx_native; for sql, "pass" means the SQL \
predicate must be true for a row to pass)
  - "definition": {definition_shape}{columns_field}

Guidelines:
- Use double quotes for all JSON keys and string values.
- Do not include any prose outside the JSON object.{columns_guidance}

Available check functions:
{available_functions}"""

_DQX_NATIVE_DEFINITION_SHAPE = '{"function": "<check function name>", "arguments": {<arg name>: <value>, ...}}'
_SQL_DEFINITION_SHAPE = '{"sql_query": "<a SELECT-only predicate expression, no DML/DDL>"}'

# Extra prompt fragments injected only for the dqx_native mode so the model
# also names the VARIABLE COLUMN SLOTS the rule targets (item B2-32). A registry
# rule is table-agnostic, so each column argument is a reusable named slot, not a
# hard-coded column. The model picks meaningful slot names; the slot FAMILY it
# returns is only a hint — the backend re-derives (locks) each native slot's
# family from the check function's own semantics in `_derive_native_slots`.
_DQX_NATIVE_COLUMNS_FIELD = (
    '\n  - "columns": a JSON array of {"name": "<snake_case slot name>", "family": '
    '"any"|"numeric"|"text"|"temporal"|"boolean"|"array"} objects, ONE per column the rule targets'
)
_DQX_NATIVE_COLUMNS_GUIDANCE = (
    '\n- Give each targeted column a meaningful snake_case slot name (e.g. "user_email", '
    '"order_amount") in "columns", and use those exact names as the column argument VALUES '
    'inside "definition".arguments.'
)

_FIELD_SUGGESTION_SYSTEM_TEMPLATE = """\
You are helping a data steward fill in one field of a data quality rule definition. Given the \
rule's context, suggest a concise value for the field "{field}".

Return ONLY a JSON object: {{"value": "<suggested value>"}}"""

# --- SQL predicate authoring assistants (write / improve / explain) -------------
# Ported from dqlake's AiAssistMenu backend (backend/routers/ai.py). Predicates are
# DQX SQL boolean expressions authored in the Rules Registry SQL editor: reusable
# columns are referenced as {{slot}} placeholders (a registry rule is table-agnostic),
# and polarity is a separate PASS/FAIL switch. The returned predicate is always
# re-validated with `is_sql_query_safe` server-side (AGENTS.md 11-SEC) before it can
# reach the editor — never trust the model's SQL blindly.
_WRITE_SQL_SYSTEM_TEMPLATE = """\
You produce data-quality rule predicates for the DQX Rules Registry. Respond with ONLY a JSON \
object: {{"predicate": "<sql boolean expression>", "polarity": "pass"|"fail"}}.

Set "polarity" to "pass" if the predicate is TRUE when the row is VALID (the common case — \
users typically describe what a good row looks like). Set it to "fail" only when the user \
explicitly describes a failure condition (e.g. "flag rows where amount is negative" — the \
predicate then describes the failing rows). When in doubt, choose "pass".

Column reference rules:
- Reference every column as a {{{{slot}}}} placeholder — never a bare column identifier. A \
registry rule is table-agnostic, so columns are always placeholders.
- Prefer the provided declared slot names as-is when they fit.

Safety rules:
- The predicate must be a single boolean SQL expression only — no SELECT, no semicolons, no \
trailing punctuation, and no DDL/DML (DROP/DELETE/INSERT/UPDATE/CREATE/ALTER/TRUNCATE/MERGE/GRANT/REVOKE)."""

_IMPROVE_SQL_SYSTEM_TEMPLATE = """\
You refine a DQX SQL boolean predicate per the user's instruction. Respond with ONLY a JSON \
object: {{"predicate": "<sql boolean expression>", "polarity": "pass"|"fail"}}.

Keep every column reference as a {{{{slot}}}} placeholder; keep declared slot names unchanged. \
Set "polarity" to "pass" when a TRUE predicate means the row is VALID (the common case), or \
"fail" only when the user explicitly describes a failure condition; when in doubt choose "pass".

Safety rules:
- The predicate must be a single boolean SQL expression only — no SELECT, no semicolons, no \
trailing punctuation, and no DDL/DML."""

_EXPLAIN_SQL_SYSTEM_TEMPLATE = """\
Explain a DQX SQL boolean predicate for a data steward in plain language. Aim for one sentence; \
two at the absolute most. Declarative voice, plain language, no apologies, no preamble \
("This rule…"), no markdown, no quotes. Describe what the predicate is checking — not how the \
SQL is written. Treat {{{{slot}}}} placeholders as column names.

Return ONLY a JSON object: {{"explanation": "<plain-language explanation>"}}"""


class SqlPredicateResult(TypedDict):
    """An AI-written SQL predicate plus an optional inferred PASS/FAIL polarity."""

    predicate: str
    polarity: str | None


_VALID_DIMENSIONS = frozenset({"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"})
_VALID_SEVERITIES = frozenset({"Low", "Medium", "High", "Critical"})
_VALID_POLARITIES = frozenset({"pass", "fail"})
# Mirrors registry_models.SlotFamily — the closed vocabulary a native column slot's
# family may take. Used to validate any family hint the model returns for a slot.
_VALID_SLOT_FAMILIES = frozenset({"numeric", "text", "temporal", "boolean", "array", "any"})
_SLOT_TOKEN_RE = re.compile(r"^\{\{\s*(.+?)\s*\}\}$")


class AiRulesService:
    """Generates DQX rules using either the legacy ChatDatabricks leg or the AIGateway.

    Two request families live here, both entirely OBO-authenticated:

    - **Legacy / contract leg** (:meth:`generate`, :meth:`generate_from_schema_info`):
      ChatDatabricks-based generation used by the data-contract importer's natural-language
      quality-expectation path; predates the AIGateway. Uses the OBO WorkspaceClient for both
      the UC schema lookup and the model call itself, so the LLM invocation runs as the
      calling user, not the app's service principal. Left otherwise unchanged — it's a
      synchronous call chain consumed by
      :class:`~databricks_labs_dqx_app.backend.services.contract_rules_service.ContractRulesService`.
    - **AIGateway-backed purpose calls** (:meth:`generate_checks_via_gateway`,
      :meth:`generate_rule`, :meth:`suggest_field`): route through :class:`AIGateway` (itself
      OBO-authenticated — see ``services/ai_gateway.py``) for the kill-switch, per-user rate
      limit, and audit log described in the Rules Registry design spec §8.
      ``generate_checks_via_gateway`` is the reworked backing for the
      ``aiAssistedChecksGeneration`` route.

    The few-shot prompt and available-functions list are built once (ClassVar) and reused
    across requests. Only the schema lookup and LLM call are per-request.
    """

    _few_shot_messages: ClassVar[list[BaseMessage] | None] = None
    _available_functions: ClassVar[str | None] = None

    def __init__(self, obo_ws: WorkspaceClient, gateway: AIGateway) -> None:
        self._obo_ws = obo_ws  # user identity — UC table access + legacy ChatDatabricks leg
        self._gateway = gateway  # AIGateway-backed purpose calls (also OBO under the hood)

    # ------------------------------------------------------------------
    # Class-level prompt construction (once per process)
    # ------------------------------------------------------------------

    @classmethod
    def _get_available_functions(cls) -> str:
        if cls._available_functions is None:
            cls._available_functions = json.dumps(get_required_check_functions_definitions())
        return cls._available_functions

    @classmethod
    def _get_few_shot_messages(cls) -> list[BaseMessage]:
        if cls._few_shot_messages is None:
            cls._few_shot_messages = cls._build_few_shot_messages()
        return cls._few_shot_messages

    @classmethod
    def _build_few_shot_messages(cls) -> list[BaseMessage]:
        resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))
        examples: list[dict[str, Any]] = yaml.safe_load(resource.read_text(encoding="utf-8"))
        messages: list[BaseMessage] = []
        for ex in examples:
            human = HumanMessage(
                content=f"schema_info: {json.dumps(ex['schema_info'])}\n"
                f"business_description: {ex['business_description']}"
            )
            ai = AIMessage(
                content=json.dumps(
                    {"quality_rules": json.loads(ex["quality_rules"]), "reasoning": ex["reasoning"]},
                    indent=None,
                )
            )
            messages.extend([human, ai])
        return messages

    # ------------------------------------------------------------------
    # Per-request helpers
    # ------------------------------------------------------------------

    def _get_schema_info(self, table_fqn: str) -> str:
        table_info = self._obo_ws.tables.get(table_fqn)
        columns = [{"name": col.name or "", "type": col.type_text or ""} for col in (table_info.columns or [])]
        return json.dumps({"columns": columns})

    def _parse_response(self, content: str) -> list[dict[str, Any]]:
        for text in self._extract_json_candidates(content):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict) and "quality_rules" in parsed:
                    return parsed["quality_rules"]
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                continue
        logger.warning("Could not parse LLM response as JSON: %.500s", content)
        return []

    @staticmethod
    def _extract_json_candidates(content: str) -> list[str]:
        """Return candidate JSON strings from an LLM response, most specific first."""
        candidates = [content]
        code_block = re.search(r"```(?:json)?\s*\n?(.*?)```", content, re.DOTALL)
        if code_block:
            candidates.append(code_block.group(1).strip())
        brace_match = re.search(r"\{.*\}", content, re.DOTALL)
        if brace_match:
            candidates.append(brace_match.group(0))
        return candidates

    # ------------------------------------------------------------------
    # Legacy / contract leg — unchanged ChatDatabricks-based generation.
    # ------------------------------------------------------------------

    def generate(self, user_input: str, table_fqn: str | None = None) -> list[dict[str, Any]]:
        """Generate DQX quality rules from natural language.

        Args:
            user_input: Natural language description of data quality requirements.
            table_fqn: Optional fully-qualified table name for schema context.

        Returns:
            List of DQX rule dicts.
        """
        schema_info = self._get_schema_info(table_fqn) if table_fqn else ""
        return self.generate_from_schema_info(user_input=user_input, schema_info=schema_info)

    def generate_from_schema_info(self, user_input: str, schema_info: str = "") -> list[dict[str, Any]]:
        """Generate DQX rules from natural language with a pre-built schema_info.

        Used by the data-contract importer for text/natural-language quality
        expectations: the schema is already known from the contract, so there
        is no UC table to look up. This reuses the same ChatDatabricks prompt
        and few-shot context as :meth:`generate` — DQX's own contract text-rule
        path needs ``dspy`` + a SparkSession, which the stateless app container
        doesn't have, so we route contract text rules through this LLM leg
        instead and tag the results with ``rule_type: text_llm`` upstream. The
        model call itself runs with the caller's OBO WorkspaceClient (never the
        app's service principal), so it is subject to the calling user's own
        UC permissions on the configured serving endpoint.

        Args:
            user_input: Natural language description of the quality expectation.
            schema_info: JSON string describing the table columns (may be empty).

        Returns:
            List of DQX rule dicts.
        """
        system = SystemMessage(content=_SYSTEM_TEMPLATE.format(available_functions=self._get_available_functions()))
        human = HumanMessage(content=f"schema_info: {schema_info}\nbusiness_description: {user_input}")
        messages: list[BaseMessage] = [system, *self._get_few_shot_messages(), human]

        # ``max_tokens`` caps the per-call output budget (AGENTS.md / OWASP
        # LLM04): rule generation returns small JSON payloads, so a bounded
        # cap protects against pathological prompts triggering runaway,
        # expensive inference without truncating legitimate responses.
        llm = ChatDatabricks(
            endpoint=conf.llm_endpoint,
            workspace_client=self._obo_ws,
            max_tokens=conf.llm_max_tokens,
        )
        response = llm.invoke(messages)
        return self._parse_response(str(response.content))

    # ------------------------------------------------------------------
    # AIGateway-backed purpose calls (Phase 4A)
    # ------------------------------------------------------------------

    async def generate_checks_via_gateway(
        self,
        user_input: str,
        user_email: str,
        table_fqn: str | None = None,
    ) -> list[dict[str, Any]]:
        """Gateway-routed replacement for the legacy ``/ai/generate-checks`` path.

        Reworked per the Rules Registry design spec §8: ``aiAssistedChecksGeneration`` now
        goes through :class:`AIGateway` (kill-switch, per-user rate limit, audit) instead of
        calling ChatDatabricks directly. Any unsafe ``sql_query`` rule in the model's output
        is dropped via :func:`_filter_unsafe_sql_rules` before the checks are returned.

        Raises:
            AIUnavailableError: AI is disabled or unconfigured.
            AIRateLimitExceededError: caller is over their hourly quota.
        """
        schema_info = self._get_schema_info(table_fqn) if table_fqn else ""
        system = _SYSTEM_TEMPLATE.format(available_functions=self._get_available_functions())
        messages: list[dict[str, str]] = [{"role": "system", "content": system}]
        for message in self._get_few_shot_messages():
            role = "assistant" if isinstance(message, AIMessage) else "user"
            messages.append({"role": role, "content": str(message.content)})
        messages.append({"role": "user", "content": f"schema_info: {schema_info}\nbusiness_description: {user_input}"})

        content = await self._gateway.query(
            user_email=user_email,
            purpose="generate_checks",
            messages=messages,
            max_tokens=conf.llm_max_tokens,
            temperature=0,  # deterministic generation (B2-33); gateway retries w/o it for reasoning models
        )
        checks = self._parse_response(content)
        return _filter_unsafe_sql_rules(checks)

    async def generate_rule(
        self,
        description: str,
        user_email: str,
        table_fqn: str | None = None,
        columns: list[str] | None = None,
        sample_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate a full registry-rule proposal, two-pass (dqx_native, then sql fallback).

        The result is always DQX-validated (and unsafe SQL rejected) before being returned —
        never an invalid or unsafe rule. Returns a dict shaped like::

            {"name", "description", "mode", "dimension", "severity", "polarity",
             "definition", "author_kind"}

        Raises:
            AIUnavailableError: AI is disabled or unconfigured.
            AIRateLimitExceededError: caller is over their hourly quota.
            ValueError: no candidate mode produced a valid, safe rule.
        """
        schema_info = self._get_schema_info(table_fqn) if table_fqn else ""
        context = self._build_rule_context(description, schema_info, columns, sample_rows)

        for mode, shape in (("dqx_native", _DQX_NATIVE_DEFINITION_SHAPE), ("sql", _SQL_DEFINITION_SHAPE)):
            proposal = await self._generate_rule_candidate(mode, shape, context, user_email)
            if proposal is None:
                continue
            validated = self._validate_and_repair_proposal(proposal)
            if validated is not None:
                validated["author_kind"] = "ai_generated"
                return validated

        raise ValueError("AI could not generate a valid, safe rule for this description.")

    async def _generate_rule_candidate(
        self,
        mode: str,
        definition_shape: str,
        context: str,
        user_email: str,
    ) -> dict[str, Any] | None:
        is_native = mode == "dqx_native"
        system = _RULE_PROPOSAL_SYSTEM_TEMPLATE.format(
            mode=mode,
            definition_shape=definition_shape,
            columns_field=_DQX_NATIVE_COLUMNS_FIELD if is_native else "",
            columns_guidance=_DQX_NATIVE_COLUMNS_GUIDANCE if is_native else "",
            available_functions=self._get_available_functions(),
        )
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": context},
        ]
        content = await self._gateway.query(
            user_email=user_email,
            purpose=f"generate_rule:{mode}",
            messages=messages,
            max_tokens=conf.llm_max_tokens,
            # Deterministic generation (B2-33). The gateway transparently drops the
            # explicit temperature and retries for reasoning endpoints (e.g. the GPT-5
            # family) that reject any non-default temperature — see AIGateway._query_endpoint.
            temperature=0,
        )
        try:
            return AIGateway.parse_json_object(content)
        except AIResponseParseError:
            logger.warning("AI rule proposal (mode=%s) returned unparsable JSON", mode)
            return None

    @staticmethod
    def _build_rule_context(
        description: str,
        schema_info: str,
        columns: list[str] | None,
        sample_rows: list[dict[str, Any]] | None,
    ) -> str:
        parts = [f"business_description: {description}"]
        if schema_info:
            parts.append(f"schema_info: {schema_info}")
        if columns:
            parts.append(f"columns: {json.dumps(columns)}")
        if sample_rows:
            # Bounded to AI_SAMPLE_ROW_LIMIT (500) — the same sample cap the
            # "ask a question about this data" path uses — so every AI/LLM
            # sample-data path is consistent. Still a hard, finite bound
            # (OWASP LLM04/LLM06): it caps prompt size and the volume of raw
            # data echoed into a model call.
            parts.append(f"sample_rows: {json.dumps(sample_rows[:AI_SAMPLE_ROW_LIMIT])}")
        return "\n".join(parts)

    def _validate_and_repair_proposal(self, proposal: dict[str, Any]) -> dict[str, Any] | None:
        """DQX-native validation of a generated rule proposal.

        Never returns an invalid or unsafe rule: the ``dqx_native`` candidate is validated
        through :meth:`DQEngine.validate_checks`; the ``sql`` candidate's query must pass
        :func:`is_sql_query_safe`. Returns ``None`` (never raises) on any failure so the
        caller can fall through to the next candidate mode.
        """
        mode = proposal.get("mode") or proposal.get("_mode")
        definition = proposal.get("definition")
        if not isinstance(definition, dict):
            return None

        if "function" in definition:
            mode = "dqx_native"
        elif "sql_query" in definition:
            mode = "sql"

        slots: list[dict[str, Any]] = []
        if mode == "dqx_native":
            function = definition.get("function")
            arguments = definition.get("arguments", {})
            if not isinstance(function, str) or not function or not isinstance(arguments, dict):
                return None
            check = {"criticality": "error", "check": {"function": function, "arguments": arguments}}
            validation = DQEngine.validate_checks([check])
            if validation.has_errors:
                logger.warning("AI-generated dqx_native rule failed validation: %s", validation.errors)
                return None
            # Populate the typed column slots the create form binds to real columns
            # (item B2-32): names come from the model's chosen column references,
            # families are locked to the check function's own semantics.
            slots = self._derive_native_slots(function, arguments, proposal.get("columns"))
        elif mode == "sql":
            sql_query = definition.get("sql_query")
            if not isinstance(sql_query, str) or not sql_query.strip():
                return None
            if not is_sql_query_safe(sql_query):
                logger.warning("AI-generated sql rule dropped: unsafe SQL query")
                return None
        else:
            return None

        return {
            "name": self._clean_str(proposal.get("name")) or "AI-generated rule",
            "description": self._clean_str(proposal.get("description")) or "",
            "mode": mode,
            "dimension": self._clean_choice(proposal.get("dimension"), _VALID_DIMENSIONS),
            "severity": self._clean_choice(proposal.get("severity"), _VALID_SEVERITIES),
            "polarity": self._clean_choice(proposal.get("polarity"), _VALID_POLARITIES) or "pass",
            "definition": definition,
            "slots": slots,
        }

    @staticmethod
    def _derive_native_slots(
        function: str,
        arguments: dict[str, Any],
        ai_columns: object,
    ) -> list[dict[str, Any]]:
        """Build RuleSlot-shaped dicts for a validated ``dqx_native`` proposal.

        Each column-bearing parameter of *function* becomes one or more slots
        (a ``columns``-kind parameter can bind several). A slot's ``name`` is
        taken from the model's column reference in *arguments* (a ``{{token}}``
        placeholder or a bare identifier), falling back to a canonical
        ``column_N`` when the model referenced nothing usable. The slot
        ``family`` is LOCKED to the check function's declared column family
        (never the model's) — mirroring the authoring UI, which does not let a
        native slot's family be edited. When the arguments referenced nothing
        usable for a column parameter, its name is drawn from the model's
        top-level ``columns`` array, then finally a canonical ``column_N``. The
        ``arg_key`` records the real function parameter so the frontend rebuilds
        ``arguments`` from the (possibly author-renamed) slots correctly.

        Args:
            function: The validated check-function name.
            arguments: The proposal's ``definition.arguments`` (already validated).
            ai_columns: The model's optional top-level ``columns`` array; only
                its entries' ``name`` values are used, as a name fallback for a
                column parameter the arguments didn't reference. Non-list ignored.

        Returns:
            A list of RuleSlot-shaped dicts (``name``, ``family``, ``position``,
            ``cardinality``, ``arg_key``), or ``[]`` when the function is
            unknown or has no column parameters.
        """
        from ..routes.v1.check_functions import _introspect_check_functions  # noqa: PLC0415

        fn_def = next((f for f in _introspect_check_functions() if f.name == function), None)
        if fn_def is None:
            return []

        # Ordered pool of the model's declared column-slot names, consumed only
        # to name a column parameter the arguments didn't reference.
        fallback_names: list[str] = []
        if isinstance(ai_columns, list):
            for col in ai_columns:
                if isinstance(col, dict) and isinstance(col.get("name"), str) and col["name"].strip():
                    fallback_names.append(col["name"].strip())
        fallback_pool = iter(fallback_names)

        slots: list[dict[str, Any]] = []
        position = 0
        canonical_index = 1
        for param in fn_def.params:
            if param.kind not in ("column", "columns"):
                continue
            # Family is locked to the check's own semantics (item 10 typed slots),
            # never the model's — an author cannot edit a native slot's family.
            family = param.family if param.family in _VALID_SLOT_FAMILIES else "any"
            raw_names = AiRulesService._slot_names_from_arg(arguments.get(param.name))
            if not raw_names:
                next_name = next(fallback_pool, None)
                raw_names = [next_name] if next_name else [f"column_{canonical_index}"]
                if next_name is None:
                    canonical_index += 1
            for raw_name in raw_names:
                name = AiRulesService._sanitize_slot_name(raw_name)
                if not name:
                    name = f"column_{canonical_index}"
                    canonical_index += 1
                slots.append(
                    {
                        "name": name,
                        "family": family,
                        "position": position,
                        "cardinality": "one",
                        "arg_key": param.name,
                    }
                )
                position += 1
        return slots

    @staticmethod
    def _slot_names_from_arg(value: object) -> list[str]:
        """Extract the model's column reference name(s) from one argument value.

        A ``{{token}}`` placeholder yields the inner name; a bare string yields
        itself; a list yields each of its usable string members, in order. Any
        non-string member is skipped.
        """

        def one(candidate: object) -> str | None:
            if not isinstance(candidate, str):
                return None
            text = candidate.strip()
            if not text:
                return None
            token = _SLOT_TOKEN_RE.match(text)
            return token.group(1).strip() if token else text

        if isinstance(value, list):
            return [name for name in (one(item) for item in value) if name]
        name = one(value)
        return [name] if name else []

    @staticmethod
    def _sanitize_slot_name(raw: str) -> str:
        """Normalise a model-proposed column reference into a safe snake_case slot name."""
        return re.sub(r"[^0-9a-zA-Z_]+", "_", raw.strip()).strip("_").lower()

    @staticmethod
    def _clean_str(value: Any) -> str | None:
        return value.strip() if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _clean_choice(value: Any, allowed: frozenset[str]) -> str | None:
        return value if isinstance(value, str) and value in allowed else None

    async def suggest_field(self, field: str, context: str, user_email: str) -> str:
        """Suggest a value for a single rule field (e.g. name/description/dimension/severity).

        Raises:
            AIUnavailableError: AI is disabled or unconfigured.
            AIRateLimitExceededError: caller is over their hourly quota.
            AIResponseParseError: the model's response did not contain a usable suggestion.
        """
        system = _FIELD_SUGGESTION_SYSTEM_TEMPLATE.format(field=field)
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": context},
        ]
        content = await self._gateway.query(
            user_email=user_email,
            purpose=f"suggest_field:{field}",
            messages=messages,
            max_tokens=2048,
            temperature=0,  # deterministic suggestion (B2-33); gateway retries w/o it for reasoning models
        )
        parsed = AIGateway.parse_json_object(content)
        value = parsed.get("value")
        if not isinstance(value, str) or not value.strip():
            raise AIResponseParseError(f"AI did not return a usable suggestion for field '{field}'.")
        return value.strip()

    # ------------------------------------------------------------------
    # SQL predicate authoring assistants (write / improve / explain)
    # ------------------------------------------------------------------

    async def write_sql(
        self,
        description: str,
        user_email: str,
        columns: list[str] | None = None,
        table_fqn: str | None = None,
    ) -> SqlPredicateResult:
        """Write a SQL predicate for a rule from a natural-language description.

        Returns ``{"predicate": <str>, "polarity": "pass"|"fail"|None}``. The predicate is
        always re-validated with :func:`is_sql_query_safe` before being returned.

        Raises:
            AIUnavailableError: AI is disabled or unconfigured.
            AIRateLimitExceededError: caller is over their hourly quota.
            AIResponseParseError: the model's response was not parsable JSON.
            ValueError: the model returned no predicate, or an unsafe one.
        """
        schema_info = self._get_schema_info(table_fqn) if table_fqn else ""
        context = self._build_sql_context(description, schema_info, columns)
        content = await self._gateway.query(
            user_email=user_email,
            purpose="write_sql",
            messages=[
                {"role": "system", "content": _WRITE_SQL_SYSTEM_TEMPLATE},
                {"role": "user", "content": context},
            ],
            max_tokens=conf.llm_max_tokens,
            temperature=0,  # deterministic generation (B2-33); gateway retries w/o it for reasoning models
        )
        return self._parse_sql_predicate(content)

    async def improve_sql(
        self,
        predicate: str,
        instruction: str,
        user_email: str,
        columns: list[str] | None = None,
    ) -> SqlPredicateResult:
        """Refine an existing SQL predicate per a free-text instruction.

        Returns ``{"predicate": <str>, "polarity": "pass"|"fail"|None}``. The refined
        predicate is always re-validated with :func:`is_sql_query_safe` before being returned.

        Raises:
            AIUnavailableError: AI is disabled or unconfigured.
            AIRateLimitExceededError: caller is over their hourly quota.
            AIResponseParseError: the model's response was not parsable JSON.
            ValueError: the model returned no predicate, or an unsafe one.
        """
        parts = [f"current_predicate: {predicate}", f"instruction: {instruction}"]
        if columns:
            parts.append(f"declared_columns: {json.dumps(columns)}")
        content = await self._gateway.query(
            user_email=user_email,
            purpose="improve_sql",
            messages=[
                {"role": "system", "content": _IMPROVE_SQL_SYSTEM_TEMPLATE},
                {"role": "user", "content": "\n".join(parts)},
            ],
            max_tokens=conf.llm_max_tokens,
            temperature=0,  # deterministic refinement (B2-33); gateway retries w/o it for reasoning models
        )
        return self._parse_sql_predicate(content)

    async def explain_sql(self, predicate: str, user_email: str) -> str:
        """Explain a SQL predicate in plain language.

        The predicate is treated as untrusted data (never executed); only its meaning is
        described. Returns a short plain-language string.

        Raises:
            AIUnavailableError: AI is disabled or unconfigured.
            AIRateLimitExceededError: caller is over their hourly quota.
            AIResponseParseError: the model returned no usable explanation.
        """
        content = await self._gateway.query(
            user_email=user_email,
            purpose="explain_sql",
            messages=[
                {"role": "system", "content": _EXPLAIN_SQL_SYSTEM_TEMPLATE},
                {"role": "user", "content": predicate},
            ],
            max_tokens=2048,
            temperature=0,  # deterministic explanation (B2-33); gateway retries w/o it for reasoning models
        )
        parsed = AIGateway.parse_json_object(content)
        explanation = parsed.get("explanation")
        if not isinstance(explanation, str) or not explanation.strip():
            raise AIResponseParseError("AI did not return a usable explanation for this predicate.")
        return explanation.strip()

    @staticmethod
    def _build_sql_context(description: str, schema_info: str, columns: list[str] | None) -> str:
        parts = [f"description: {description}"]
        if columns:
            parts.append(f"declared_columns: {json.dumps(columns)}")
        if schema_info:
            parts.append(f"schema_info: {schema_info}")
        return "\n".join(parts)

    @staticmethod
    def _parse_sql_predicate(content: str) -> SqlPredicateResult:
        """Parse and safety-validate a model-written SQL predicate response.

        Raises:
            AIResponseParseError: the response was not parsable JSON.
            ValueError: no predicate was returned, or the predicate failed
                :func:`is_sql_query_safe` (AGENTS.md 11-SEC — never surface unsafe AI SQL).
        """
        parsed = AIGateway.parse_json_object(content)
        predicate = parsed.get("predicate")
        if not isinstance(predicate, str) or not predicate.strip():
            raise ValueError("AI did not return a SQL predicate.")
        predicate = predicate.strip()
        if not is_sql_query_safe(predicate):
            logger.warning("AI-written SQL predicate rejected: unsafe SQL")
            raise ValueError("AI produced an unsafe SQL predicate. Try rephrasing your request.")
        polarity = parsed.get("polarity")
        clean_polarity = polarity if isinstance(polarity, str) and polarity in _VALID_POLARITIES else None
        return {"predicate": predicate, "polarity": clean_polarity}
