"""Tests for ``services/profiling_suggestion_service.py`` (B2-82).

Profiler rule suggestions live on the Profile page. The critical guarantee
(B2-91): LISTING suggestions has ZERO side effects — no registry rule is
created or approved — while APPLYING one resolves-or-creates + approves the
rule and binds it to the table.

Uses ``create_autospec`` doubles for the registry / monitored-table /
apply-rules services (dependency injection per AGENTS.md); real profiler check
dicts flow through the real ``build_profiling_rule`` introspection.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    LatestProfile,
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.profiling_suggestion_service import (
    BindingNotFoundError,
    ProfilingSuggestionService,
    SuggestionNotApplicableError,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import RuleNotPublishedError
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


def _binding_detail(table_fqn: str = "cat.sch.tbl") -> MonitoredTableDetail:
    table = MonitoredTable(binding_id="b1", table_fqn=table_fqn, status="approved")
    return MonitoredTableDetail(table=table, applied_rules=[])


def _check(function: str, arguments: dict) -> dict:
    return {"name": "gen", "criticality": "warn", "check": {"function": function, "arguments": arguments}}


def _profile(generated_rules: list[dict]) -> LatestProfile:
    return LatestProfile(run_id="run1", source_table_fqn="cat.sch.tbl", generated_rules=generated_rules)


def _approved_rule(rule_id: str) -> RegistryRule:
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            "slots": [{"name": "column", "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=1,
        definition=definition,
        user_metadata={"name": f"Rule {rule_id}", "dimension": "Completeness", "severity": "Medium"},
    )


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def registry():
    svc = create_autospec(RegistryService, instance=True)
    # By default no structurally-equal approved rule exists yet.
    svc.find_approved_rule_for_definition.return_value = None
    return svc


@pytest.fixture
def apply_rules():
    svc = create_autospec(ApplyRulesService, instance=True)
    svc.list_applied.return_value = []
    return svc


def _service(monitored_tables, registry, apply_rules) -> ProfilingSuggestionService:
    return ProfilingSuggestionService(monitored_tables=monitored_tables, registry=registry, apply_rules=apply_rules)


class TestListSuggestions:
    def test_unknown_binding_raises(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = None
        svc = _service(monitored_tables, registry, apply_rules)
        with pytest.raises(BindingNotFoundError):
            svc.list_suggestions("missing")

    def test_no_profile_returns_empty(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = None
        svc = _service(monitored_tables, registry, apply_rules)
        assert svc.list_suggestions("b1") == []

    def test_returns_mappable_suggestions(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        svc = _service(monitored_tables, registry, apply_rules)

        suggestions = svc.list_suggestions("b1")

        assert len(suggestions) == 1
        assert suggestions[0].index == 0
        assert suggestions[0].function == "is_not_null"
        assert suggestions[0].column_mapping == {"column": "amount"}

    def test_listing_creates_no_registry_rule(self, monitored_tables, registry, apply_rules):
        # B2-91: showing suggestions MUST NOT create or approve anything.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        svc = _service(monitored_tables, registry, apply_rules)

        svc.list_suggestions("b1")

        registry.match_or_create_approved_rule.assert_not_called()
        registry.create_rule.assert_not_called()
        registry.submit.assert_not_called()
        registry.approve.assert_not_called()

    def test_skips_unmappable_check(self, monitored_tables, registry, apply_rules):
        # An unregistered function can't be mapped and is dropped.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile(
            [_check("totally_not_a_real_check_fn", {"column": "amount"})]
        )
        svc = _service(monitored_tables, registry, apply_rules)

        assert svc.list_suggestions("b1") == []

    def test_excludes_already_applied(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        # A structurally-equal approved rule already exists AND is applied.
        registry.find_approved_rule_for_definition.return_value = _approved_rule("R")
        apply_rules.list_applied.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="R", column_mapping=[{"column": "amount"}])
        ]
        svc = _service(monitored_tables, registry, apply_rules)

        assert svc.list_suggestions("b1") == []
        registry.match_or_create_approved_rule.assert_not_called()


class TestApplySuggestion:
    def test_apply_creates_and_binds(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        registry.match_or_create_approved_rule.return_value = (_approved_rule("prof1"), True)
        apply_rules.apply_rule.return_value = AppliedRule(
            id="ar1", binding_id="b1", rule_id="prof1", column_mapping=[{"column": "amount"}]
        )
        svc = _service(monitored_tables, registry, apply_rules)

        applied = svc.apply_suggestion("b1", 0, "user@x")

        assert applied.rule_id == "prof1"
        registry.match_or_create_approved_rule.assert_called_once()
        apply_rules.apply_rule.assert_called_once_with("b1", "prof1", [{"column": "amount"}], "user@x")

    def test_unknown_binding_raises(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = None
        svc = _service(monitored_tables, registry, apply_rules)
        with pytest.raises(BindingNotFoundError):
            svc.apply_suggestion("missing", 0, "user@x")

    def test_out_of_range_index_raises(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        svc = _service(monitored_tables, registry, apply_rules)

        with pytest.raises(SuggestionNotApplicableError):
            svc.apply_suggestion("b1", 5, "user@x")
        registry.match_or_create_approved_rule.assert_not_called()

    def test_unmappable_check_raises(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile(
            [_check("totally_not_a_real_check_fn", {"column": "amount"})]
        )
        svc = _service(monitored_tables, registry, apply_rules)

        with pytest.raises(SuggestionNotApplicableError):
            svc.apply_suggestion("b1", 0, "user@x")
        registry.match_or_create_approved_rule.assert_not_called()

    def test_non_approved_duplicate_blocks_apply(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        # A same-fingerprint rule exists but isn't approved -> (None, False).
        registry.match_or_create_approved_rule.return_value = (None, False)
        svc = _service(monitored_tables, registry, apply_rules)

        with pytest.raises(SuggestionNotApplicableError):
            svc.apply_suggestion("b1", 0, "user@x")
        apply_rules.apply_rule.assert_not_called()


class TestApplySuggestions:
    """Batch apply (B2-109): apply the selected set in one pass, robust to partial failure."""

    def test_batch_creates_and_binds_selected_set(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile(
            [_check("is_not_null", {"column": "amount"}), _check("is_not_null", {"column": "name"})]
        )
        registry.match_or_create_approved_rule.return_value = (_approved_rule("prof1"), True)
        apply_rules.apply_rule.side_effect = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="prof1", column_mapping=[{"column": "amount"}]),
            AppliedRule(id="ar2", binding_id="b1", rule_id="prof1", column_mapping=[{"column": "name"}]),
        ]
        svc = _service(monitored_tables, registry, apply_rules)

        result = svc.apply_suggestions("b1", [0, 1], "user@x")

        assert len(result.applied) == 2
        assert result.failed == []
        assert registry.match_or_create_approved_rule.call_count == 2
        assert apply_rules.apply_rule.call_count == 2

    def test_batch_dedupes_repeated_index(self, monitored_tables, registry, apply_rules):
        # Idempotency: the same index selected twice never creates or binds twice.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        registry.match_or_create_approved_rule.return_value = (_approved_rule("prof1"), True)
        apply_rules.apply_rule.return_value = AppliedRule(
            id="ar1", binding_id="b1", rule_id="prof1", column_mapping=[{"column": "amount"}]
        )
        svc = _service(monitored_tables, registry, apply_rules)

        result = svc.apply_suggestions("b1", [0, 0], "user@x")

        assert len(result.applied) == 1
        registry.match_or_create_approved_rule.assert_called_once()
        apply_rules.apply_rule.assert_called_once()

    def test_batch_reports_partial_failure(self, monitored_tables, registry, apply_rules):
        # A good suggestion applies; an out-of-range one is reported, not raised.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        registry.match_or_create_approved_rule.return_value = (_approved_rule("prof1"), True)
        apply_rules.apply_rule.return_value = AppliedRule(
            id="ar1", binding_id="b1", rule_id="prof1", column_mapping=[{"column": "amount"}]
        )
        svc = _service(monitored_tables, registry, apply_rules)

        result = svc.apply_suggestions("b1", [0, 9], "user@x")

        assert len(result.applied) == 1
        assert len(result.failed) == 1
        assert result.failed[0].index == 9

    def test_batch_non_approved_duplicate_reported_not_raised(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        registry.match_or_create_approved_rule.return_value = (None, False)
        svc = _service(monitored_tables, registry, apply_rules)

        result = svc.apply_suggestions("b1", [0], "user@x")

        assert result.applied == []
        assert len(result.failed) == 1
        apply_rules.apply_rule.assert_not_called()

    def test_batch_reports_apply_rule_error(self, monitored_tables, registry, apply_rules):
        # An apply-time failure (e.g. rule not published) is captured per-index.
        monitored_tables.get.return_value = _binding_detail()
        monitored_tables.get_latest_profile.return_value = _profile([_check("is_not_null", {"column": "amount"})])
        registry.match_or_create_approved_rule.return_value = (_approved_rule("prof1"), True)
        apply_rules.apply_rule.side_effect = RuleNotPublishedError("not published")
        svc = _service(monitored_tables, registry, apply_rules)

        result = svc.apply_suggestions("b1", [0], "user@x")

        assert result.applied == []
        assert len(result.failed) == 1

    def test_batch_unknown_binding_raises(self, monitored_tables, registry, apply_rules):
        monitored_tables.get.return_value = None
        svc = _service(monitored_tables, registry, apply_rules)
        with pytest.raises(BindingNotFoundError):
            svc.apply_suggestions("missing", [0], "user@x")
