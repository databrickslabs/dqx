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
from databricks_labs_dqx_app.backend.services.ai_rules_service import AiRulesService


def _service(gateway: MagicMock) -> AiRulesService:
    return AiRulesService(obo_ws=MagicMock(), gateway=gateway)


def _gateway_returning(*contents: str) -> MagicMock:
    gateway = create_autospec(AIGateway, instance=True)
    gateway.query.side_effect = list(contents)
    gateway.parse_json_object.side_effect = AIGateway.parse_json_object
    return gateway


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
        valid_sql = json.dumps(
            {"name": "sql", "description": "d", "definition": {"sql_query": "id IS NOT NULL"}}
        )
        service = _service(_gateway_returning(invalid_dqx_native, valid_sql))

        result = await service.generate_rule(description="d", user_email="a@x")

        assert result["mode"] == "sql"
        assert result["slots"] == []

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
        gateway = _gateway_returning(invalid_dqx_native, valid_sql)
        service = _service(gateway)

        result = await service.generate_rule(description="d", user_email="a@x")

        assert result["mode"] == "sql"
        assert result["definition"] == {"sql_query": "id IS NOT NULL"}
        assert gateway.query.call_count == 2

    async def test_unsafe_sql_candidate_is_rejected_and_generation_fails(self):
        invalid_dqx_native = json.dumps({"definition": {"function": "nope", "arguments": {}}})
        unsafe_sql = json.dumps(
            {
                "name": "bad sql",
                "description": "d",
                "definition": {"sql_query": "DROP TABLE foo; --"},
            }
        )
        gateway = _gateway_returning(invalid_dqx_native, unsafe_sql)
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.generate_rule(description="d", user_email="a@x")

    async def test_both_candidates_unparsable_raises_value_error(self):
        gateway = _gateway_returning("garbage one", "garbage two")
        service = _service(gateway)

        with pytest.raises(ValueError):
            await service.generate_rule(description="d", user_email="a@x")

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


class TestDeriveNativeSlots:
    """Direct unit tests for the typed-column-slot derivation (item B2-32)."""

    def test_names_slot_from_argument_and_locks_family(self):
        slots = AiRulesService._derive_native_slots(
            "is_valid_email", {"column": "{{user_email}}"}, None
        )
        assert slots == [
            {"name": "user_email", "family": "text", "position": 0, "cardinality": "one", "arg_key": "column"}
        ]

    def test_falls_back_to_declared_columns_then_stays_locked_family(self):
        # No column reference in the arguments -> name drawn from the model's
        # columns array; family stays locked to the check function (numeric hint ignored).
        slots = AiRulesService._derive_native_slots(
            "is_not_null", {}, [{"name": "customer_id", "family": "numeric"}]
        )
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

        result = await service.write_sql(
            description="amount must be positive", user_email="a@x", columns=["amount"]
        )

        assert result == {"predicate": "{{amount}} > 0", "polarity": "pass"}
        assert gateway.query.call_args.kwargs["purpose"] == "write_sql"
        # Declared slots are forwarded so the model reuses them as {{slot}}s.
        assert "amount" in gateway.query.call_args.kwargs["messages"][-1]["content"]

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
        gateway = _gateway_returning(json.dumps({"predicate": "{{amount}} > 0 AND {{amount}} < 100", "polarity": "pass"}))
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
