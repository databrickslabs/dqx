"""Tests for the AIGateway-backed purpose calls on ``AiRulesService`` (Rules Registry Phase 4A).

The legacy ChatDatabricks-based ``generate``/``generate_from_schema_info`` leg (used by the
data-contract importer) is unchanged and already implicitly covered by
``test_contract_rules_service.py`` mocking the whole service. These tests cover the new
gateway-backed methods: ``generate_checks_via_gateway``, ``generate_rule``, and
``suggest_field`` — in particular the DQX-native validation/repair contract (never return an
invalid or unsafe rule) required by AGENTS.md.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway, AIResponseParseError
from databricks_labs_dqx_app.backend.services.ai_rules_service import (
    _DEFAULT_DIMENSIONS,
    _DEFAULT_SEVERITIES,
    _DQX_NATIVE_COVERAGE_GUIDANCE,
    _EXPLAIN_SQL_SYSTEM_TEMPLATE,
    _IMPROVE_SQL_SYSTEM_TEMPLATE,
    _LOWCODE_PROPOSAL_SYSTEM_TEMPLATE,
    _RULE_PROPOSAL_SYSTEM_TEMPLATE,
    _WRITE_SQL_SYSTEM_TEMPLATE,
    AiRulesService,
    parse_rule_type_intent,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


def _service(gateway: MagicMock, app_settings: object | None = None) -> AiRulesService:
    return AiRulesService(obo_ws=MagicMock(), gateway=gateway, app_settings=app_settings)


def _app_settings_with_labels(definitions: list[dict]) -> MagicMock:
    """A stub AppSettingsService whose get_label_definitions returns *definitions*."""
    stub = create_autospec(AppSettingsService, instance=True)
    stub.get_label_definitions.return_value = definitions
    return stub


def _gateway_returning(*contents: str) -> MagicMock:
    gateway = create_autospec(AIGateway, instance=True)
    gateway.query.side_effect = list(contents)
    gateway.parse_json_object.side_effect = AIGateway.parse_json_object
    return gateway


# Sentinels that make one pass of the cascade miss, so a gateway script can reach
# the pass it actually means to exercise (B2-132: dqx_native -> lowcode -> sql).
# A native response DECLINING the requirement (no single built-in covers it):
_NATIVE_DECLINE = json.dumps({"decline": True})
# A parsable but UNUSABLE low-code response (empty rows -> compiles to nothing):
_LOWCODE_MISS = json.dumps({"name": "n", "description": "d", "lowcode_ast": {"rows": [], "joins": []}})


class TestGenerateChecksViaGateway:
    async def test_happy_path_returns_parsed_checks(self):
        payload = json.dumps(
            {
                "quality_rules": [
                    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
                ],
                "reasoning": "id must not be null",
            }
        )
        gateway = _gateway_returning(payload)
        service = _service(gateway)

        checks = await service.generate_checks_via_gateway(user_input="id must not be null", user_email="a@x")

        assert checks == [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
        gateway.query.assert_called_once()
        assert gateway.query.call_args.kwargs["user_email"] == "a@x"
        assert gateway.query.call_args.kwargs["purpose"] == "generate_checks"

    async def test_unsafe_sql_query_check_is_dropped(self):
        payload = json.dumps(
            {
                "quality_rules": [
                    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
                    {
                        "criticality": "error",
                        "check": {
                            "function": "sql_query",
                            "arguments": {"query": "DROP TABLE foo", "merge_columns": []},
                        },
                    },
                ],
                "reasoning": "x",
            }
        )
        gateway = _gateway_returning(payload)
        service = _service(gateway)

        checks = await service.generate_checks_via_gateway(user_input="desc", user_email="a@x")

        assert len(checks) == 1
        assert checks[0]["check"]["function"] == "is_not_null"

    async def test_unparsable_response_returns_empty_list(self):
        gateway = _gateway_returning("not json at all")
        service = _service(gateway)

        checks = await service.generate_checks_via_gateway(user_input="desc", user_email="a@x")

        assert checks == []


class TestGenerateRule:
    async def test_valid_dqx_native_proposal_is_returned(self):
        proposal = json.dumps(
            {
                "name": "ID not null",
                "description": "id column must not be null",
                "dimension": "Completeness",
                "severity": "High",
                "polarity": "pass",
                "definition": {"function": "is_not_null", "arguments": {"column": "id"}},
            }
        )
        # "null" is a basic-check signal, so dqx_native is tried FIRST here and
        # wins on the first pass — no low-code miss ahead of it.
        gateway = _gateway_returning(proposal)
        service = _service(gateway)

        result = await service.generate_rule(description="id must not be null", user_email="a@x")

        assert result["mode"] == "dqx_native"
        assert result["name"] == "ID not null"
        assert result["dimension"] == "Completeness"
        assert result["severity"] == "High"
        assert result["author_kind"] == "ai_generated"
        assert result["definition"] == {"function": "is_not_null", "arguments": {"column": "id"}}

    async def test_dqx_native_proposal_populates_typed_column_slots(self):
        # The model names the target column ("id"); the slot carries that name,
        # its position/cardinality/arg_key, and a family LOCKED to the check's
        # semantics (is_not_null is polymorphic -> "any").
        proposal = json.dumps(
            {
                "name": "ID not null",
                "description": "id must not be null",
                "definition": {"function": "is_not_null", "arguments": {"column": "id"}},
                "columns": [{"name": "id", "family": "numeric"}],
            }
        )
        # "null" prefers dqx_native, so it is the first (and only) pass.
        service = _service(_gateway_returning(proposal))

        result = await service.generate_rule(description="id must not be null", user_email="a@x")

        assert result["slots"] == [
            {"name": "id", "family": "any", "position": 0, "cardinality": "one", "arg_key": "column"}
        ]

    async def test_dqx_native_slot_family_locked_to_check_semantics(self):
        # is_valid_email's column argument is a "text" slot; the model's chosen
        # column name is preserved but the family comes from the check function.
        proposal = json.dumps(
            {
                "name": "Email valid",
                "description": "email must be valid",
                "definition": {"function": "is_valid_email", "arguments": {"column": "user_email"}},
            }
        )
        service = _service(_gateway_returning(proposal))

        result = await service.generate_rule(description="email must be valid", user_email="a@x")

        assert result["slots"] == [
            {"name": "user_email", "family": "text", "position": 0, "cardinality": "one", "arg_key": "column"}
        ]

    async def test_sql_proposal_carries_no_slots(self):
        invalid_dqx_native = json.dumps({"definition": {"function": "nope", "arguments": {}}})
        valid_sql = json.dumps({"name": "sql", "description": "d", "definition": {"sql_query": "id IS NOT NULL"}})
        service = _service(_gateway_returning(invalid_dqx_native, _LOWCODE_MISS, valid_sql))

        result = await service.generate_rule(description="d", user_email="a@x")

        assert result["mode"] == "sql"
        assert result["slots"] == []

    def test_sql_proposal_derives_slots_from_predicate_tokens(self):
        """A col-vs-col SQL proposal declares a slot per {{token}} so the RHS column
        is substituted and runs (item 42)."""
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        proposal = {
            "name": "amount within limit",
            "description": "amount must not exceed credit_limit",
            "mode": "sql",
            "dimension": "validity",
            "severity": "error",
            "polarity": "pass",
            "definition": {"sql_query": "{{amount}} <= {{credit_limit}}"},
            "columns": [
                {"name": "amount", "family": "numeric"},
                {"name": "credit_limit", "family": "numeric"},
            ],
        }
        result = svc._validate_and_repair_proposal(proposal)
        assert result is not None
        names = [s["name"] for s in result["slots"]]
        assert names == ["amount", "credit_limit"]
        assert {s["family"] for s in result["slots"]} == {"numeric"}

    async def test_generation_requests_deterministic_temperature(self):
        proposal = json.dumps(
            {
                "name": "n",
                "description": "d",
                "definition": {"function": "is_not_null", "arguments": {"column": "id"}},
            }
        )
        gateway = _gateway_returning(proposal)
        service = _service(gateway)

        await service.generate_rule(description="d", user_email="a@x")

        assert gateway.query.call_args.kwargs["temperature"] == 0

    async def test_invalid_dqx_native_falls_back_to_sql_candidate(self):
        invalid_dqx_native = json.dumps(
            {
                "name": "bad",
                "description": "d",
                "dimension": "Validity",
                "severity": "Low",
                "polarity": "pass",
                # Unknown check function -> DQEngine.validate_checks rejects this.
                "definition": {"function": "not_a_real_check_function", "arguments": {"column": "id"}},
            }
        )
        valid_sql = json.dumps(
            {
                "name": "good sql",
                "description": "d",
                "dimension": "Validity",
                "severity": "Low",
                "polarity": "pass",
                "definition": {"sql_query": "id IS NOT NULL"},
            }
        )
        gateway = _gateway_returning(invalid_dqx_native, _LOWCODE_MISS, valid_sql)
        service = _service(gateway)

        result = await service.generate_rule(description="d", user_email="a@x")

        assert result["mode"] == "sql"
        assert result["definition"] == {"sql_query": "id IS NOT NULL"}
        # Three passes: dqx_native (invalid) -> lowcode (miss) -> sql (accepted).
        assert gateway.query.call_count == 3

    async def test_unsafe_sql_candidate_is_rejected_and_generation_fails(self):
        invalid_dqx_native = json.dumps({"definition": {"function": "nope", "arguments": {}}})
        unsafe_sql = json.dumps(
            {
                "name": "bad sql",
                "description": "d",
                "definition": {"sql_query": "DROP TABLE foo; --"},
            }
        )
        gateway = _gateway_returning(invalid_dqx_native, _LOWCODE_MISS, unsafe_sql)
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.generate_rule(description="d", user_email="a@x")

    async def test_all_candidates_unparsable_raises_value_error(self):
        gateway = _gateway_returning("garbage zero", "garbage one", "garbage two")
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.generate_rule(description="d", user_email="a@x")
        # dqx_native, lowcode, sql — all three unparsable.
        assert gateway.query.call_count == 3

    async def test_bounds_sample_rows_sent_to_the_model(self):
        proposal = json.dumps(
            {
                "name": "n",
                "description": "d",
                "definition": {"function": "is_not_null", "arguments": {"column": "id"}},
            }
        )
        gateway = _gateway_returning(proposal)
        service = _service(gateway)
        # More than the AI sample cap (500) — the service must forward at most
        # the first AI_SAMPLE_ROW_LIMIT rows, dropping the overflow.
        sample_rows = [{"id": i} for i in range(600)]

        await service.generate_rule(description="d", user_email="a@x", sample_rows=sample_rows)

        sent_context = gateway.query.call_args.kwargs["messages"][-1]["content"]
        assert '"id": 499' in sent_context
        assert '"id": 500' not in sent_context


def _lowcode_proposal(**over: object) -> str:
    base = {
        "name": "Amount Positive",
        "description": "amount must be positive",
        "dimension": "Validity",
        "severity": "Medium",
        "polarity": "pass",
        "columns": [{"name": "amount", "family": "numeric"}],
        "group_by_columns": None,
        "lowcode_ast": {
            "rows": [{"kind": "row", "combinator": None, "column_ref": "amount", "operator": ">", "value": 0}],
            "joins": [],
        },
    }
    base.update(over)
    return json.dumps(base)


def _sql_proposal() -> str:
    """A valid, safe sql proposal — the cascade's last resort."""
    return json.dumps(
        {
            "name": "Amount Positive",
            "description": "amount must be positive",
            "dimension": "Validity",
            "severity": "Medium",
            "polarity": "pass",
            "definition": {"sql_query": "{{amount}} > 0"},
            "columns": [{"name": "amount", "family": "numeric"}],
        }
    )


class TestGenerateRuleLowcode:
    """Cascade ordering (B2-132): dqx_native tried first, then lowcode, then sql."""

    async def test_valid_lowcode_proposal_is_returned_when_native_declines(self):
        gateway = _gateway_returning(_NATIVE_DECLINE, _lowcode_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="amount must be positive", user_email="a@x")

        assert result["mode"] == "lowcode"
        assert result["name"] == "Amount Positive"
        assert result["polarity"] == "pass"
        assert result["author_kind"] == "ai_generated"
        # Compiled to the simple sql_expression predicate body + one declared slot.
        assert result["definition"]["predicate"] == "{{amount}} > 0"
        assert result["definition"]["lowcode_ast"]["rows"][0]["column_ref"] == "amount"
        assert result["slots"] == [
            {"name": "amount", "family": "numeric", "position": 0, "cardinality": "one", "arg_key": None}
        ]
        # dqx_native (declined) -> lowcode (accepted); no sql fallback needed.
        assert gateway.query.call_count == 2
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:lowcode"

    async def test_group_by_lowcode_compiles_to_sql_query_body(self):
        proposal = _lowcode_proposal(
            columns=[{"name": "order_id", "family": "numeric"}, {"name": "customer_id", "family": "any"}],
            group_by_columns="{{customer_id}}",
            lowcode_ast={
                "rows": [
                    {
                        "kind": "aggregated",
                        "combinator": None,
                        "aggregate": "count",
                        "column_ref": "order_id",
                        "operator": "<=",
                        "value": 100,
                    }
                ],
                "joins": [],
            },
        )
        service = _service(_gateway_returning(_NATIVE_DECLINE, proposal))

        result = await service.generate_rule(description="no customer over 100 orders", user_email="a@x")

        assert result["mode"] == "lowcode"
        body = result["definition"]
        assert "predicate" not in body
        assert body["merge_columns"] == ["{{customer_id}}"]
        assert body["sql_query"] == (
            "SELECT {{customer_id}}, (NOT (COUNT({{order_id}}) <= 100)) AS condition "
            "FROM {{input_view}} GROUP BY {{customer_id}}"
        )
        assert body["group_by"] == "{{customer_id}}"
        # One slot per placeholder in the compiled body, families from the hint.
        assert {s["name"]: s["family"] for s in result["slots"]} == {"order_id": "numeric", "customer_id": "any"}

    def test_col_vs_col_value_compiles_rhs_column_and_declares_both_slots(self):
        # Item 42 / col-vs-col bug: a row whose value is a column reference
        # {"$col": "b"} must compile to a NON-blank RHS ({{b}}) and declare both
        # column slots, so the lowcode pass produces a runnable rule instead of
        # `{{a}} < <empty>` that never falls through to sql.
        proposal = json.loads(
            _lowcode_proposal(
                name="A Less Than B",
                description="a must be less than b",
                columns=[{"name": "a", "family": "numeric"}, {"name": "b", "family": "numeric"}],
                lowcode_ast={
                    "rows": [
                        {
                            "kind": "row",
                            "combinator": None,
                            "column_ref": "a",
                            "operator": "<",
                            "value": {"$col": "b"},
                        }
                    ],
                    "joins": [],
                },
            )
        )
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())

        validated = svc._validate_lowcode_proposal(proposal)

        assert validated is not None
        assert validated["mode"] == "lowcode"
        # RHS is the {{b}} placeholder, not a NULL/blank operand.
        assert validated["definition"]["predicate"] == "{{a}} < {{b}}"
        assert {s["name"] for s in validated["slots"]} == {"a", "b"}
        assert {s["name"]: s["family"] for s in validated["slots"]} == {"a": "numeric", "b": "numeric"}

    async def test_unusable_lowcode_falls_through_to_sql(self):
        # Low-code AST has no compilable rows -> fall through to the last resort.
        unusable_lowcode = json.dumps({"name": "n", "description": "d", "lowcode_ast": {"rows": [], "joins": []}})
        gateway = _gateway_returning(_NATIVE_DECLINE, unusable_lowcode, _sql_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="amount must be positive", user_email="a@x")

        assert result["mode"] == "sql"
        assert gateway.query.call_count == 3

    async def test_unsafe_compiled_lowcode_is_rejected_and_falls_through(self):
        # A text value carrying a forbidden keyword compiles to unsafe SQL, so
        # the low-code candidate is dropped and generation falls through.
        unsafe_lowcode = _lowcode_proposal(
            columns=[{"name": "note", "family": "text"}],
            lowcode_ast={
                "rows": [
                    {
                        "kind": "row",
                        "combinator": None,
                        "column_ref": "note",
                        "operator": "contains",
                        "value": "DROP TABLE users",
                    }
                ],
                "joins": [],
            },
        )
        gateway = _gateway_returning(_NATIVE_DECLINE, unsafe_lowcode, _sql_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="d", user_email="a@x")

        assert result["mode"] == "sql"
        assert gateway.query.call_count == 3

    async def test_unparsable_lowcode_falls_through(self):
        gateway = _gateway_returning(_NATIVE_DECLINE, "not json", _sql_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="d", user_email="a@x")

        assert result["mode"] == "sql"
        assert gateway.query.call_count == 3


class TestGenerateRulePrefersBuiltInCheck:
    """The cascade tries dqx_native FIRST, and only falls through when it can't cover the ask."""

    async def test_unique_description_goes_to_native_first(self):
        # "unique" maps crisply onto is_unique — the built-in wins on the FIRST pass.
        native = json.dumps(
            {
                "name": "ID unique",
                "description": "id must be unique",
                "definition": {"function": "is_unique", "arguments": {"columns": ["id"]}},
            }
        )
        gateway = _gateway_returning(native)
        service = _service(gateway)

        result = await service.generate_rule(description="the id column must be unique", user_email="a@x")

        assert result["mode"] == "dqx_native"
        # dqx_native tried first — no low-code pass ahead of it.
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:dqx_native"

    async def test_simple_comparison_also_goes_to_native_first(self):
        # A plain "must be positive" is a built-in (is_not_less_than), so it no
        # longer lands in the low-code builder just because it reads like a
        # magnitude comparison.
        native = json.dumps(
            {
                "name": "Amount positive",
                "description": "amount must be positive",
                "definition": {
                    "function": "is_not_less_than",
                    "arguments": {"column": "order_amount", "limit": 0},
                },
                "columns": [{"name": "order_amount", "family": "numeric"}],
            }
        )
        gateway = _gateway_returning(native)
        service = _service(gateway)

        result = await service.generate_rule(description="order amount must be positive", user_email="a@x")

        assert result["mode"] == "dqx_native"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:dqx_native"

    async def test_native_first_falls_through_to_lowcode_when_invalid(self):
        # dqx_native goes first, but if it can't produce a DQX-valid check the
        # cascade still falls through to low-code.
        invalid_native = json.dumps({"definition": {"function": "not_a_real_check", "arguments": {}}})
        gateway = _gateway_returning(invalid_native, _lowcode_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="no duplicate amounts", user_email="a@x")

        assert result["mode"] == "lowcode"
        assert gateway.query.call_count == 2
        assert gateway.query.call_args_list[0].kwargs["purpose"] == "generate_rule:dqx_native"
        assert gateway.query.call_args_list[1].kwargs["purpose"] == "generate_rule:lowcode"

    async def test_declined_native_falls_through_to_lowcode(self):
        # The guard that makes native-first safe: when no single built-in covers
        # the whole requirement the native pass declines, and the compound
        # condition is built in low-code instead of as a half-complete check.
        gateway = _gateway_returning(json.dumps({"decline": True}), _lowcode_proposal())
        service = _service(gateway)

        result = await service.generate_rule(
            description="amount must be positive and less than 1000000", user_email="a@x"
        )

        assert result["mode"] == "lowcode"
        assert gateway.query.call_count == 2
        assert gateway.query.call_args_list[0].kwargs["purpose"] == "generate_rule:dqx_native"
        assert gateway.query.call_args_list[1].kwargs["purpose"] == "generate_rule:lowcode"

    async def test_bounded_range_description_goes_to_native_first(self):
        # "between X and Y" maps onto is_in_range, so the built-in wins the first
        # pass instead of the low-code builder hand-rolling the same two bounds.
        native = json.dumps(
            {
                "name": "Score in range",
                "description": "score must be between 0 and 100",
                "definition": {
                    "function": "is_in_range",
                    "arguments": {"column": "score", "min_limit": 0, "max_limit": 100},
                },
            }
        )
        gateway = _gateway_returning(native)
        service = _service(gateway)

        result = await service.generate_rule(description="score must be between 0 and 100", user_email="a@x")

        assert result["mode"] == "dqx_native"
        assert result["definition"]["function"] == "is_in_range"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:dqx_native"

    def test_native_prompt_offers_the_decline_escape_hatch(self):
        # Without an explicit way to decline, a model asked for one check always
        # produces one — so the coverage wording is what the fall-through rests on.
        assert "IN FULL" in _DQX_NATIVE_COVERAGE_GUIDANCE
        assert '{"decline": true}' in _DQX_NATIVE_COVERAGE_GUIDANCE


class TestParseRuleTypeIntent:
    """Pure intent parse (B2-140): explicit type -> that mode; else None."""

    @pytest.mark.parametrize(
        "description",
        [
            "sql rule for negative amounts",
            "Write a SQL check that flags null ids",
            "give me a rule in SQL",
            "a predicate using SQL",
        ],
    )
    def test_explicit_sql(self, description: str):
        assert parse_rule_type_intent(description) == "sql"

    @pytest.mark.parametrize(
        "description",
        [
            "low-code rule for positive amounts",
            "lowcode check please",
            "make a low code rule",
            "a lowcode rule that ensures amount > 0",
            # New friendly name for the lowcode mode: "Custom Checks".
            "a custom check for positive amounts",
            "make a custom rule that ensures amount > 0",
            "a custom condition for the order total",
            # Current UI friendly name for the lowcode mode: "Condition Builder".
            "use the condition builder for the order total",
            "make a condition builder rule",
        ],
    )
    def test_explicit_lowcode(self, description: str):
        assert parse_rule_type_intent(description) == "lowcode"

    @pytest.mark.parametrize(
        "description",
        [
            "dqx native rule for nulls",
            "use a native check",
            "a built-in function to validate emails",
            "native function that checks not null",
            # The "basic"/"simple" phrasings predate the current friendly name
            # for the dqx_native mode ("Built-in check") and still route here.
            "a basic rule for null ids",
            "make a basic check that validates emails",
            # "Simple Rule"/"Simple Check" also route to dqx_native.
            "a simple rule for null ids",
            "make a simple check that validates emails",
        ],
    )
    def test_explicit_native(self, description: str):
        assert parse_rule_type_intent(description) == "dqx_native"

    @pytest.mark.parametrize(
        "description",
        [
            "amount must be positive",
            "the sql_query column should never be null",  # incidental 'sql' — not a type request
            "ensure the native currency code is valid",  # 'native' not naming a rule kind
            "",
        ],
    )
    def test_no_explicit_type_returns_none(self, description: str):
        assert parse_rule_type_intent(description) is None


class TestGenerateRuleExplicitType:
    """generate_rule honours an explicit rule-type request, bypassing the cascade (B2-140)."""

    async def test_explicit_sql_goes_straight_to_sql(self):
        valid_sql = json.dumps({"name": "sql", "description": "d", "definition": {"sql_query": "id IS NOT NULL"}})
        gateway = _gateway_returning(valid_sql)
        service = _service(gateway)

        result = await service.generate_rule(description="write a sql rule for null ids", user_email="a@x")

        assert result["mode"] == "sql"
        # Straight to SQL — no lowcode/native passes were tried.
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:sql"

    async def test_explicit_lowcode_goes_straight_to_lowcode(self):
        gateway = _gateway_returning(_lowcode_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="a low-code rule for amount", user_email="a@x")

        assert result["mode"] == "lowcode"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:lowcode"

    async def test_explicit_native_goes_straight_to_native(self):
        native = json.dumps(
            {"name": "n", "description": "d", "definition": {"function": "is_not_null", "arguments": {"column": "id"}}}
        )
        gateway = _gateway_returning(native)
        service = _service(gateway)

        result = await service.generate_rule(description="use a built-in function for null id", user_email="a@x")

        assert result["mode"] == "dqx_native"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:dqx_native"

    async def test_explicit_mode_failure_raises_without_falling_back(self):
        # An explicit SQL request whose SQL candidate is unusable must FAIL —
        # never silently substitute a lowcode/native rule (B2-140).
        unusable_sql = json.dumps({"name": "bad", "description": "d", "definition": {"sql_query": "   "}})
        gateway = _gateway_returning(unusable_sql)
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.generate_rule(description="write a sql rule", user_email="a@x")
        # Only the SQL pass ran — no fallback to other modes.
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:sql"

    async def test_friendly_custom_check_name_goes_straight_to_lowcode(self):
        # "Custom Checks" is the user-facing name for the lowcode mode.
        gateway = _gateway_returning(_lowcode_proposal())
        service = _service(gateway)

        result = await service.generate_rule(description="a custom check for amount", user_email="a@x")

        assert result["mode"] == "lowcode"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:lowcode"

    async def test_friendly_basic_rule_name_goes_straight_to_native(self):
        # "basic rule" is a legacy user-facing name for the dqx_native mode.
        native = json.dumps(
            {"name": "n", "description": "d", "definition": {"function": "is_not_null", "arguments": {"column": "id"}}}
        )
        gateway = _gateway_returning(native)
        service = _service(gateway)

        result = await service.generate_rule(description="a basic rule for null id", user_email="a@x")

        assert result["mode"] == "dqx_native"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:dqx_native"

    async def test_no_explicit_type_still_cascades(self):
        # No type named -> the default dqx_native -> lowcode -> sql cascade, which
        # a valid built-in check wins on the first pass.
        native = json.dumps(
            {"name": "n", "description": "d", "definition": {"function": "is_not_null", "arguments": {"column": "id"}}}
        )
        gateway = _gateway_returning(native)
        service = _service(gateway)

        result = await service.generate_rule(description="amount must be positive", user_email="a@x")

        assert result["mode"] == "dqx_native"
        assert gateway.query.call_count == 1
        assert gateway.query.call_args.kwargs["purpose"] == "generate_rule:dqx_native"


class TestDeriveNativeSlots:
    """Direct unit tests for the typed-column-slot derivation (item B2-32)."""

    def test_names_slot_from_argument_and_locks_family(self):
        slots = AiRulesService._derive_native_slots("is_valid_email", {"column": "{{user_email}}"}, None)
        assert slots == [
            {"name": "user_email", "family": "text", "position": 0, "cardinality": "one", "arg_key": "column"}
        ]

    def test_falls_back_to_declared_columns_then_stays_locked_family(self):
        # No column reference in the arguments -> name drawn from the model's
        # columns array; family stays locked to the check function (numeric hint ignored).
        slots = AiRulesService._derive_native_slots("is_not_null", {}, [{"name": "customer_id", "family": "numeric"}])
        assert slots == [
            {"name": "customer_id", "family": "any", "position": 0, "cardinality": "one", "arg_key": "column"}
        ]

    def test_canonical_name_when_nothing_provided(self):
        slots = AiRulesService._derive_native_slots("is_not_null", {}, None)
        assert slots == [
            {"name": "column_1", "family": "any", "position": 0, "cardinality": "one", "arg_key": "column"}
        ]

    def test_unknown_function_yields_no_slots(self):
        assert AiRulesService._derive_native_slots("not_a_real_check", {"column": "x"}, None) == []


class TestSuggestField:
    async def test_returns_suggested_value(self):
        gateway = _gateway_returning(json.dumps({"value": "Completeness"}))
        service = _service(gateway)

        value = await service.suggest_field(field="dimension", context="rule checks nulls", user_email="a@x")

        assert value == "Completeness"
        assert gateway.query.call_args.kwargs["purpose"] == "suggest_field:dimension"

    async def test_missing_value_raises_parse_error(self):
        gateway = _gateway_returning(json.dumps({"not_value": "x"}))
        service = _service(gateway)

        with pytest.raises(AIResponseParseError):
            await service.suggest_field(field="dimension", context="ctx", user_email="a@x")


class TestWriteSql:
    async def test_returns_safe_predicate_and_polarity(self):
        gateway = _gateway_returning(json.dumps({"predicate": "{{amount}} > 0", "polarity": "pass"}))
        service = _service(gateway)

        result = await service.write_sql(description="amount must be positive", user_email="a@x", columns=["amount"])

        assert result == {
            "predicate": "{{amount}} > 0",
            "polarity": "pass",
            # Undeclared placeholders still come back as slots (family "any") so the
            # editor can declare every one of them without the author retyping it.
            "slots": [{"name": "amount", "family": "any"}],
        }
        assert gateway.query.call_args.kwargs["purpose"] == "write_sql"
        # Declared slots are forwarded so the model reuses them as {{slot}}s.
        assert "amount" in gateway.query.call_args.kwargs["messages"][-1]["content"]

    async def test_cross_table_join_names_the_table_outright_not_as_a_slot(self):
        """A joined table is written as a literal FQN, so it declares no slot.

        A rule belongs to one table; the table it joins is part of the rule's own
        SQL rather than something each binding rebinds, so only the COLUMNS the
        rule reads come back as slots for the author to map.
        """
        gateway = _gateway_returning(
            json.dumps(
                {
                    "predicate": (
                        "{{amount}} * fx.rate_to_usd < 10000\n"
                        "LEFT JOIN main.ref.fx_rates fx ON fx.country_code = {{country_code}}"
                    ),
                    "polarity": "pass",
                    "slots": [
                        {"name": "amount", "family": "numeric"},
                        {"name": "country_code", "family": "text"},
                    ],
                }
            )
        )
        service = _service(gateway)

        result = await service.write_sql(description="sales below 10000 in USD", user_email="a@x")

        assert "LEFT JOIN main.ref.fx_rates fx" in result["predicate"]
        # Slot order follows the predicate, not the model's declaration order.
        assert result["slots"] == [
            {"name": "amount", "family": "numeric"},
            {"name": "country_code", "family": "text"},
        ]

    async def test_slots_not_present_in_the_predicate_are_dropped(self):
        gateway = _gateway_returning(
            json.dumps(
                {
                    "predicate": "{{amount}} > 0",
                    "polarity": "pass",
                    "slots": [
                        {"name": "amount", "family": "numeric"},
                        {"name": "ghost", "family": "text"},
                    ],
                }
            )
        )
        service = _service(gateway)

        result = await service.write_sql(description="d", user_email="a@x")

        assert result["slots"] == [{"name": "amount", "family": "numeric"}]

    async def test_unknown_slot_family_falls_back_to_any(self):
        gateway = _gateway_returning(
            json.dumps(
                {
                    "predicate": "{{amount}} > 0",
                    "polarity": "pass",
                    "slots": [{"name": "amount", "family": "currency"}],
                }
            )
        )
        service = _service(gateway)

        result = await service.write_sql(description="d", user_email="a@x")

        assert result["slots"] == [{"name": "amount", "family": "any"}]

    async def test_unsafe_predicate_is_rejected(self):
        gateway = _gateway_returning(json.dumps({"predicate": "DROP TABLE foo", "polarity": "pass"}))
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.write_sql(description="d", user_email="a@x")

    async def test_missing_predicate_raises_value_error(self):
        gateway = _gateway_returning(json.dumps({"polarity": "pass"}))
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.write_sql(description="d", user_email="a@x")

    async def test_invalid_polarity_is_dropped(self):
        gateway = _gateway_returning(json.dumps({"predicate": "{{x}} IS NOT NULL", "polarity": "MAYBE"}))
        service = _service(gateway)

        result = await service.write_sql(description="d", user_email="a@x")

        assert result["polarity"] is None


class TestImproveSql:
    async def test_returns_safe_refined_predicate(self):
        gateway = _gateway_returning(
            json.dumps({"predicate": "{{amount}} > 0 AND {{amount}} < 100", "polarity": "pass"})
        )
        service = _service(gateway)

        result = await service.improve_sql(
            predicate="{{amount}} > 0", instruction="cap it at 100", user_email="a@x", columns=["amount"]
        )

        assert result["predicate"] == "{{amount}} > 0 AND {{amount}} < 100"
        assert gateway.query.call_args.kwargs["purpose"] == "improve_sql"
        content = gateway.query.call_args.kwargs["messages"][-1]["content"]
        assert "cap it at 100" in content
        assert "{{amount}} > 0" in content

    async def test_unsafe_refinement_is_rejected(self):
        gateway = _gateway_returning(json.dumps({"predicate": "1=1; DELETE FROM t"}))
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.improve_sql(predicate="1=1", instruction="x", user_email="a@x")


class TestExplainSql:
    async def test_returns_explanation(self):
        gateway = _gateway_returning(json.dumps({"explanation": "Amount is greater than zero."}))
        service = _service(gateway)

        text = await service.explain_sql(predicate="{{amount}} > 0", user_email="a@x")

        assert text == "Amount is greater than zero."
        assert gateway.query.call_args.kwargs["purpose"] == "explain_sql"

    async def test_missing_explanation_raises_parse_error(self):
        gateway = _gateway_returning(json.dumps({"nope": "x"}))
        service = _service(gateway)

        with pytest.raises(AIResponseParseError):
            await service.explain_sql(predicate="{{amount}} > 0", user_email="a@x")


class TestPassPreferencePrompts:
    """The prompt templates must STRONGLY PREFER the passing-case polarity=pass.

    These tests assert the templates carry the strengthened wording so a
    reviewer/CI catch can detect accidental prompt regressions.
    """

    def test_lowcode_template_strongly_prefers_pass(self):
        assert "STRONGLY PREFER" in _LOWCODE_PROPOSAL_SYSTEM_TEMPLATE
        assert '"pass"' in _LOWCODE_PROPOSAL_SYSTEM_TEMPLATE
        assert "VALID" in _LOWCODE_PROPOSAL_SYSTEM_TEMPLATE
        # Escape hatch preserved: "fail" is mentioned as the exception, not the default.
        assert '"fail"' in _LOWCODE_PROPOSAL_SYSTEM_TEMPLATE

    def test_rule_proposal_template_strongly_prefers_pass(self):
        # The template has {definition_shape} etc. as format placeholders;
        # the polarity guidance is literal text (not a placeholder).
        assert "STRONGLY PREFER" in _RULE_PROPOSAL_SYSTEM_TEMPLATE
        assert '"pass"' in _RULE_PROPOSAL_SYSTEM_TEMPLATE
        assert "VALID" in _RULE_PROPOSAL_SYSTEM_TEMPLATE
        assert '"fail"' in _RULE_PROPOSAL_SYSTEM_TEMPLATE

    def test_write_sql_template_strongly_prefers_pass(self):
        assert "STRONGLY PREFER" in _WRITE_SQL_SYSTEM_TEMPLATE
        assert '"pass"' in _WRITE_SQL_SYSTEM_TEMPLATE
        assert "VALID" in _WRITE_SQL_SYSTEM_TEMPLATE
        assert '"fail"' in _WRITE_SQL_SYSTEM_TEMPLATE

    def test_improve_sql_template_strongly_prefers_pass(self):
        assert "STRONGLY PREFER" in _IMPROVE_SQL_SYSTEM_TEMPLATE
        assert '"pass"' in _IMPROVE_SQL_SYSTEM_TEMPLATE
        # Improve template uses "VALID" indirectly — check the key pass-preference signal.
        assert '"fail"' in _IMPROVE_SQL_SYSTEM_TEMPLATE

    def test_sql_templates_are_not_format_escaped(self):
        """These three templates are sent to the model VERBATIM (no ``.format()``).

        They were copied from the ``.format()``-ed templates above, which doubles
        every brace. Left escaped, the model is shown ``{{{{slot}}}}`` for the
        placeholder syntax and — worse — a malformed ``{{"predicate": ...}}`` as
        its required output shape, which is not parsable JSON.
        """
        for template in (_WRITE_SQL_SYSTEM_TEMPLATE, _IMPROVE_SQL_SYSTEM_TEMPLATE, _EXPLAIN_SQL_SYSTEM_TEMPLATE):
            assert "{{{{" not in template
            assert '{{"' not in template
            # The placeholder syntax the author actually types stays double-braced.
            assert "{{slot}}" in template

    def test_write_sql_template_teaches_cross_table_joins_on_a_named_table(self):
        assert "JOIN" in _WRITE_SQL_SYSTEM_TEMPLATE
        # A joined table is named outright; only the checked table's columns are
        # placeholders, so the author has nothing extra to bind.
        assert "never a {{placeholder}}" in _WRITE_SQL_SYSTEM_TEMPLATE
        assert "catalog.schema.<table>" in _WRITE_SQL_SYSTEM_TEMPLATE
        assert '"slots"' in _WRITE_SQL_SYSTEM_TEMPLATE


class TestPolarityDefaultsToPass:
    """When the model omits polarity or returns an unrecognised value, the parse
    path must default to "pass" — never to "fail".
    """

    def test_validate_and_repair_proposal_defaults_polarity_to_pass(self):
        # Model returned a valid sql rule but with no polarity field.
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        proposal = {
            "name": "id not null",
            "description": "id must not be null",
            "definition": {"sql_query": "{{id}} IS NOT NULL"},
        }
        result = svc._validate_and_repair_proposal(proposal)
        assert result is not None
        assert result["polarity"] == "pass"

    def test_validate_and_repair_proposal_invalid_polarity_defaults_to_pass(self):
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        proposal = {
            "name": "id not null",
            "description": "d",
            "polarity": "MAYBE",
            "definition": {"sql_query": "{{id}} IS NOT NULL"},
        }
        result = svc._validate_and_repair_proposal(proposal)
        assert result is not None
        assert result["polarity"] == "pass"

    def test_validate_lowcode_proposal_defaults_polarity_to_pass(self):
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        proposal = {
            "name": "Amount Positive",
            "description": "amount must be positive",
            # No polarity field at all.
            "columns": [{"name": "amount", "family": "numeric"}],
            "group_by_columns": None,
            "lowcode_ast": {
                "rows": [{"kind": "row", "combinator": None, "column_ref": "amount", "operator": ">", "value": 0}],
                "joins": [],
            },
        }
        result = svc._validate_lowcode_proposal(proposal)
        assert result is not None
        assert result["polarity"] == "pass"

    async def test_write_sql_invalid_polarity_returns_none(self):
        # _parse_sql_predicate normalises an unrecognised polarity to None
        # (caller retains current polarity when AI returns nothing usable).
        gateway = _gateway_returning(json.dumps({"predicate": "{{id}} IS NOT NULL", "polarity": "unknown"}))
        service = _service(gateway)

        result = await service.write_sql(description="d", user_email="a@x")

        assert result["polarity"] is None


class TestResolveLabelVocab:
    """The dimension/severity vocab is read from the injected AppSettingsService,
    falling back to the hard-coded defaults when absent/empty/malformed (W-B)."""

    def test_defaults_when_no_settings_injected(self):
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        dimensions, severities = svc._resolve_label_vocab()
        assert dimensions == list(_DEFAULT_DIMENSIONS)
        assert severities == list(_DEFAULT_SEVERITIES)

    def test_reads_configured_custom_vocab(self):
        app_settings = _app_settings_with_labels(
            [
                {"key": "dimension", "values": ["Validity", "Relevance"]},
                {"key": "severity", "values": ["Info", "Warning", "Blocker"]},
            ]
        )
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock(), app_settings=app_settings)
        dimensions, severities = svc._resolve_label_vocab()
        assert dimensions == ["Validity", "Relevance"]
        assert severities == ["Info", "Warning", "Blocker"]

    def test_missing_entries_fall_back_to_defaults(self):
        # No dimension/severity entries at all -> defaults for both.
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock(), app_settings=_app_settings_with_labels([]))
        dimensions, severities = svc._resolve_label_vocab()
        assert dimensions == list(_DEFAULT_DIMENSIONS)
        assert severities == list(_DEFAULT_SEVERITIES)

    def test_malformed_or_empty_values_fall_back_to_defaults(self):
        app_settings = _app_settings_with_labels(
            [
                {"key": "dimension", "values": "not-a-list"},  # malformed
                {"key": "severity", "values": []},  # empty
            ]
        )
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock(), app_settings=app_settings)
        dimensions, severities = svc._resolve_label_vocab()
        assert dimensions == list(_DEFAULT_DIMENSIONS)
        assert severities == list(_DEFAULT_SEVERITIES)

    def test_settings_read_failure_degrades_to_defaults(self):
        app_settings = create_autospec(AppSettingsService, instance=True)
        app_settings.get_label_definitions.side_effect = RuntimeError("boom")
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock(), app_settings=app_settings)
        dimensions, severities = svc._resolve_label_vocab()
        assert dimensions == list(_DEFAULT_DIMENSIONS)
        assert severities == list(_DEFAULT_SEVERITIES)


class TestVocabDrivenValidation:
    """_clean_choice accepts a configured value and rejects an off-list one (W-B)."""

    def test_validate_and_repair_accepts_configured_value_rejects_off_list(self):
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        vocab = (["Relevance"], ["Blocker"])
        proposal = {
            "name": "n",
            "description": "d",
            "dimension": "Relevance",  # configured -> accepted
            "severity": "High",  # NOT in the configured severities -> rejected
            "definition": {"sql_query": "{{id}} IS NOT NULL"},
        }
        result = svc._validate_and_repair_proposal(proposal, vocab)
        assert result is not None
        assert result["dimension"] == "Relevance"
        assert result["severity"] is None

    def test_lowcode_validation_uses_configured_vocab(self):
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        vocab = (["Relevance"], ["Blocker"])
        proposal = {
            "name": "Amount Positive",
            "description": "amount must be positive",
            "dimension": "Relevance",
            "severity": "Blocker",
            "columns": [{"name": "amount", "family": "numeric"}],
            "group_by_columns": None,
            "lowcode_ast": {
                "rows": [{"kind": "row", "combinator": None, "column_ref": "amount", "operator": ">", "value": 0}],
                "joins": [],
            },
        }
        result = svc._validate_lowcode_proposal(proposal, vocab)
        assert result is not None
        assert result["dimension"] == "Relevance"
        assert result["severity"] == "Blocker"

    def test_default_vocab_still_accepts_builtin_values(self):
        # No vocab passed -> hard-coded defaults still validate the built-in set.
        svc = AiRulesService(obo_ws=MagicMock(), gateway=MagicMock())
        proposal = {
            "name": "n",
            "description": "d",
            "dimension": "Completeness",
            "severity": "High",
            "definition": {"sql_query": "{{id}} IS NOT NULL"},
        }
        result = svc._validate_and_repair_proposal(proposal)
        assert result is not None
        assert result["dimension"] == "Completeness"
        assert result["severity"] == "High"


class TestVocabDrivenPrompt:
    """The proposal prompt option lists interpolate the configured vocab (W-B)."""

    async def test_native_prompt_lists_configured_values(self):
        app_settings = _app_settings_with_labels(
            [
                {"key": "dimension", "values": ["Relevance"]},
                {"key": "severity", "values": ["Blocker"]},
            ]
        )
        proposal = json.dumps(
            {"name": "n", "description": "d", "definition": {"function": "is_not_null", "arguments": {"column": "id"}}}
        )
        gateway = _gateway_returning(proposal)
        service = _service(gateway, app_settings=app_settings)

        await service.generate_rule(description="amount must be positive", user_email="a@x")

        # The dqx_native pass (1st query) carries the configured vocab in its system prompt.
        native_system = gateway.query.call_args_list[0].kwargs["messages"][0]["content"]
        assert "Relevance" in native_system
        assert "Blocker" in native_system
        # A default value the admin removed is no longer offered.
        assert "Completeness" not in native_system

    async def test_lowcode_prompt_lists_configured_values(self):
        app_settings = _app_settings_with_labels(
            [
                {"key": "dimension", "values": ["Relevance"]},
                {"key": "severity", "values": ["Blocker"]},
            ]
        )
        gateway = _gateway_returning(
            _NATIVE_DECLINE, _lowcode_proposal(dimension="Relevance", severity="Blocker")
        )
        service = _service(gateway, app_settings=app_settings)

        await service.generate_rule(description="amount must be positive", user_email="a@x")

        # The lowcode pass is the 2nd query, after the native pass declines.
        lowcode_system = gateway.query.call_args_list[1].kwargs["messages"][0]["content"]
        assert "Relevance" in lowcode_system
        assert "Blocker" in lowcode_system
        # Escaped JSON braces in the lowcode template survive .format() intact.
        assert '{"rows":' in lowcode_system or '{"kind":' in lowcode_system
