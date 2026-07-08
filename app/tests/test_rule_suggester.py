"""Tests for ``services/rule_suggester.py`` — Rules Registry Phase 4C.

Exercises the full pipeline with fakes (no Databricks, no Vector Search, no
serving endpoint): a fake ``RuleRetriever`` returning candidates, a fake
``AIGateway`` judge, and ``create_autospec`` doubles for the registry /
monitored-table / apply-rules services (dependency injection per AGENTS.md).
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import (
    AIGateway,
    AIRateLimitExceededError,
    AIUnavailableError,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, TableColumn
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    LatestProfile,
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_retriever import RetrievedRule, RuleRetrievalUnavailableError
from databricks_labs_dqx_app.backend.services.rule_suggester import (
    _NO_CLEAN_MAPPING_REASON,
    _NO_MATCH_REASON,
    _NO_PUBLISHED_RULES_REASON,
    RuleSuggester,
)


def _column(name: str, type_name: str = "STRING") -> TableColumn:
    return TableColumn(name=name, type_name=type_name, comment=None, nullable=True, position=0)


def _discovery(columns: list[TableColumn] | None = None) -> create_autospec:
    """Fake DiscoveryService. Defaults to returning NO UC columns so tests
    that seed a profile exercise the profile fallback path unchanged; pass
    ``columns`` to exercise the UC-schema-first path."""
    svc = create_autospec(DiscoveryService, instance=True)
    svc.get_table_columns_async.return_value = columns or []
    return svc


def _rule(rule_id: str, slot_names: list[str], severity: str = "High") -> RegistryRule:
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {n: f"{{{{{n}}}}}" for n in slot_names}},
            "slots": [
                {"name": n, "family": "any", "position": i, "cardinality": "one"} for i, n in enumerate(slot_names)
            ],
            "parameters": [],
        }
    )
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=1,
        definition=definition,
        user_metadata={"name": f"Rule {rule_id}", "dimension": "Completeness", "severity": severity},
    )


def _binding_detail(table_fqn: str = "cat.sch.tbl") -> MonitoredTableDetail:
    table = MonitoredTable(binding_id="b1", table_fqn=table_fqn, status="approved")
    return MonitoredTableDetail(table=table, applied_rules=[])


def _profile(columns: dict) -> LatestProfile:
    return LatestProfile(run_id="run1", source_table_fqn="cat.sch.tbl", summary=columns)


class FakeRetriever:
    def __init__(self, *, available: bool = True, reason: str = "", candidates: list[RetrievedRule] | None = None):
        self._available = available
        self._reason = reason
        self._candidates = candidates or []
        self.error: Exception | None = None

    def is_available(self) -> tuple[bool, str]:
        return self._available, self._reason

    def retrieve(self, query_text: str, top_k: int) -> list[RetrievedRule]:
        if self.error is not None:
            raise self.error
        return self._candidates[:top_k]


def _gateway(judge_response: dict | str | None = None, error: Exception | None = None) -> create_autospec:
    gw = create_autospec(AIGateway, instance=True)
    gw.is_enabled.return_value = True
    gw.endpoint_name.return_value = "ai-endpoint"
    if error is not None:
        gw.query.side_effect = error
    else:
        content = (
            judge_response if isinstance(judge_response, str) else json.dumps(judge_response or {"suggestions": []})
        )
        gw.query.return_value = content
    gw.parse_json_object.side_effect = AIGateway.parse_json_object
    return gw


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def registry():
    return create_autospec(RegistryService, instance=True)


@pytest.fixture
def apply_rules():
    svc = create_autospec(ApplyRulesService, instance=True)
    svc.list_applied.return_value = []
    return svc


def _suggester(monitored_tables, registry, apply_rules, retriever, gateway, discovery=None) -> RuleSuggester:
    return RuleSuggester(
        monitored_tables=monitored_tables,
        registry=registry,
        apply_rules=apply_rules,
        retriever=retriever,
        ai_gateway=gateway,
        discovery=discovery if discovery is not None else _discovery(),
    )


class TestUnavailablePaths:
    async def test_unknown_binding(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = None
        suggester = _suggester(monitored_tables, registry, apply_rules, FakeRetriever(), _gateway())

        result = await suggester.suggest("missing", "user@x")

        assert result.available is False
        assert "missing" in result.reason

    async def test_retriever_unavailable(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        retriever = FakeRetriever(available=False, reason="no embedding endpoint is configured")
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, _gateway())

        result = await suggester.suggest("b1", "user@x")

        assert result.available is False
        assert "embedding endpoint" in result.reason

    async def test_ai_not_enabled(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        gateway = _gateway()
        gateway.is_enabled.return_value = False
        suggester = _suggester(monitored_tables, registry, apply_rules, FakeRetriever(), gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.available is False

    async def test_retrieval_unavailable_error(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = None
        retriever = FakeRetriever()
        retriever.error = RuleRetrievalUnavailableError("embedding endpoint down")
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, _gateway())

        result = await suggester.suggest("b1", "user@x")

        assert result.available is False
        assert "embedding endpoint down" in result.reason

    async def test_retrieval_unexpected_error_degrades_gracefully(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = None
        retriever = FakeRetriever()
        retriever.error = RuntimeError("boom")
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, _gateway())

        result = await suggester.suggest("b1", "user@x")

        assert result.available is False

    @pytest.mark.parametrize("error_cls", [AIUnavailableError, AIRateLimitExceededError])
    async def test_judge_gateway_errors_degrade_gracefully(self, monitored_tables, registry, apply_rules, error_cls):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}})
        rule = _rule("r1", ["id"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        error = error_cls("boom") if error_cls is AIUnavailableError else error_cls(5)
        gateway = _gateway(error=error)
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.available is False

    async def test_judge_unparsable_response_degrades_gracefully(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}})
        rule = _rule("r1", ["id"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway(judge_response="not json at all, no braces")
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.available is False


class TestHappyPath:
    async def test_returns_mapped_suggestion(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}, "email": {}})
        rule = _rule("r1", ["column"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway(
            {
                "suggestions": [
                    {"rule_id": "r1", "mapping": {"column": "email"}, "explanation": "email looks nullable-checkable"}
                ]
            }
        )
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.available is True
        assert len(result.suggestions) == 1
        suggestion = result.suggestions[0]
        assert suggestion.rule_id == "r1"
        assert suggestion.column_mapping == {"column": "email"}
        assert suggestion.explanation == "email looks nullable-checkable"
        assert suggestion.dimension == "Completeness"
        assert suggestion.severity == "High"

    async def test_no_candidates_from_retriever(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = None
        suggester = _suggester(monitored_tables, registry, apply_rules, FakeRetriever(candidates=[]), _gateway())

        result = await suggester.suggest("b1", "user@x")

        assert result.available is True
        assert result.suggestions == []
        assert result.reason == _NO_PUBLISHED_RULES_REASON

    async def test_no_matching_rule_sets_distinct_reason(self, monitored_tables, registry, apply_rules):
        # Candidates retrieved + published, judge ran, but proposed nothing.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}})
        registry.get_rule.return_value = _rule("r1", ["column"])
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, _gateway({"suggestions": []}))

        result = await suggester.suggest("b1", "user@x")

        assert result.available is True
        assert result.suggestions == []
        assert result.reason == _NO_MATCH_REASON

    async def test_all_judged_filtered_sets_no_clean_mapping_reason(self, monitored_tables, registry, apply_rules):
        # Judge proposed a mapping, but it maps to a column that doesn't exist,
        # so post-processing drops it — the reason must say so, not stay blank.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}})
        registry.get_rule.return_value = _rule("r1", ["column"])
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column": "does_not_exist"}}]})
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.available is True
        assert result.suggestions == []
        assert result.reason == _NO_CLEAN_MAPPING_REASON

    async def test_unpublished_candidate_rule_is_dropped(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}})
        draft_rule = _rule("r1", ["column"])
        draft_rule.status = "draft"
        registry.get_rule.return_value = draft_rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, _gateway())

        result = await suggester.suggest("b1", "user@x")

        assert result.available is True
        assert result.suggestions == []
        assert result.reason == _NO_PUBLISHED_RULES_REASON


class TestPostProcessing:
    async def test_drops_mapping_to_nonexistent_column(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"id": {}})
        rule = _rule("r1", ["column"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column": "does_not_exist"}}]})
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.available is True
        assert result.suggestions == []

    async def test_enforces_multi_slot_completeness(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"a": {}, "b": {}})
        rule = _rule("r1", ["column_a", "column_b"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        # Only fills one of two slots — must be dropped.
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column_a": "a"}}]})
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.suggestions == []

    async def test_complete_multi_slot_mapping_is_kept(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"a": {}, "b": {}})
        rule = _rule("r1", ["column_a", "column_b"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column_a": "a", "column_b": "b"}}]})
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert len(result.suggestions) == 1

    async def test_dedups_identical_mappings(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"a": {}})
        rule = _rule("r1", ["column"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway(
            {
                "suggestions": [
                    {"rule_id": "r1", "mapping": {"column": "a"}},
                    {"rule_id": "r1", "mapping": {"column": "a"}},
                ]
            }
        )
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert len(result.suggestions) == 1

    async def test_excludes_already_applied_mapping(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"a": {}})
        rule = _rule("r1", ["column"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column": "a"}}]})
        apply_rules.list_applied.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "a"}])
        ]
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.suggestions == []

    async def test_unknown_rule_id_in_judge_output_is_ignored(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"a": {}})
        rule = _rule("r1", ["column"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "not-a-candidate", "mapping": {"column": "a"}}]})
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway)

        result = await suggester.suggest("b1", "user@x")

        assert result.suggestions == []


class TestColumnResolution:
    """Columns resolve from the live UC schema first (dqlake behaviour), with
    the latest profile as a fallback — so a never-profiled table still works."""

    async def test_uc_columns_used_when_table_never_profiled(self, monitored_tables, registry, apply_rules):
        # No profile at all — the OLD behaviour produced zero columns and hence
        # zero suggestions. Now the UC schema supplies the columns.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = None
        rule = _rule("r1", ["column"])
        registry.get_rule.return_value = rule
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column": "email"}}]})
        discovery = _discovery([_column("id", "BIGINT"), _column("email", "STRING")])
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway, discovery)

        result = await suggester.suggest("b1", "user@x")

        assert len(result.suggestions) == 1
        assert result.suggestions[0].column_mapping == {"column": "email"}

    async def test_uc_column_type_family_reaches_the_judge(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = None
        registry.get_rule.return_value = _rule("r1", ["column"])
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": []})
        discovery = _discovery([_column("amount", "DECIMAL(10,2)")])
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway, discovery)

        await suggester.suggest("b1", "user@x")

        _, kwargs = gateway.query.call_args
        user_prompt = kwargs["messages"][1]["content"]
        assert "amount" in user_prompt
        # DECIMAL classifies to the numeric family and reaches the judge payload.
        assert "numeric" in user_prompt

    async def test_falls_back_to_profile_when_uc_read_fails(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile({"legacy_col": {}})
        registry.get_rule.return_value = _rule("r1", ["column"])
        retriever = FakeRetriever(candidates=[RetrievedRule(rule_id="r1", score=0.9)])
        gateway = _gateway({"suggestions": [{"rule_id": "r1", "mapping": {"column": "legacy_col"}}]})
        discovery = _discovery()
        discovery.get_table_columns_async.side_effect = RuntimeError("permission denied")
        suggester = _suggester(monitored_tables, registry, apply_rules, retriever, gateway, discovery)

        result = await suggester.suggest("b1", "user@x")

        assert len(result.suggestions) == 1
        assert result.suggestions[0].column_mapping == {"column": "legacy_col"}
