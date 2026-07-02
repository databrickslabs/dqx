"""Tests for the ``/monitored-tables`` route handlers.

Follows ``test_registry_rules_routes.py``'s convention: call the route
functions directly with mocked dependencies (``MonitoredTableService``, OBO
``WorkspaceClient``) rather than spinning up a FastAPI ``TestClient`` — the
routes themselves are thin adapters over ``MonitoredTableService``, whose
behaviour is already covered by ``test_monitored_table_service.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.models import (
    ApplyRuleIn,
    RegisterMonitoredTableIn,
    SetAppliedRulePinIn,
    SetAppliedRuleSeverityOverrideIn,
)
from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import (
    apply_rule_to_table,
    delete_monitored_table,
    get_monitored_table,
    get_monitored_table_profile,
    list_monitored_tables,
    publish_monitored_table,
    register_monitored_table,
    remove_applied_rule,
    set_applied_rule_pin,
    set_applied_rule_severity_override,
    suggest_rules_for_table,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError
from databricks_labs_dqx_app.backend.services.rule_suggester import RuleSuggestion, SuggestRulesResult
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    DuplicateMonitoredTableError,
    LatestProfile,
    MonitoredTableDetail,
    MonitoredTableSummary,
)


def _table(binding_id: str = "b1", table_fqn: str = "cat.schema.tbl", status: str = "draft") -> MonitoredTable:
    return MonitoredTable(binding_id=binding_id, table_fqn=table_fqn, status=status)


def _mock_obo_ws(user_email: str = "alice@x") -> MagicMock:
    obo = MagicMock()
    me = MagicMock()
    me.user_name = user_email
    obo.current_user.me.return_value = me
    return obo


class TestListAndGet:
    def test_list_maps_domain_summaries_to_dto(self):
        svc = MagicMock()
        svc.list_monitored_tables.return_value = [MonitoredTableSummary(table=_table(), applied_rule_count=3)]
        result = list_monitored_tables(svc=svc, status="draft")
        assert len(result) == 1
        assert result[0].table.binding_id == "b1"
        assert result[0].applied_rule_count == 3
        svc.list_monitored_tables.assert_called_once_with(
            status="draft", steward=None, catalog=None, schema=None, name=None
        )

    def test_get_returns_detail_with_applied_rules(self):
        svc = MagicMock()
        applied = AppliedRule(id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "id"}])
        svc.get.return_value = MonitoredTableDetail(
            table=_table(),
            applied_rules=[AppliedRuleSummary(applied_rule=applied, rule_name="Not Null", rule_dimension="Completeness")],
        )
        result = get_monitored_table("b1", svc=svc)
        assert result.table.binding_id == "b1"
        assert len(result.applied_rules) == 1
        assert result.applied_rules[0].rule_name == "Not Null"

    def test_get_missing_binding_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            get_monitored_table("missing", svc=svc)
        assert excinfo.value.status_code == 404


class TestRegister:
    def test_register_success(self):
        svc = MagicMock()
        svc.register.return_value = _table()
        body = RegisterMonitoredTableIn(table_fqn="cat.schema.tbl", steward="bob@x")
        result = register_monitored_table(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert result.table.table_fqn == "cat.schema.tbl"
        assert result.applied_rule_count == 0
        svc.register.assert_called_once_with("cat.schema.tbl", "alice@x", steward="bob@x")

    def test_register_duplicate_raises_409(self):
        svc = MagicMock()
        svc.register.side_effect = DuplicateMonitoredTableError("Table already monitored")
        body = RegisterMonitoredTableIn(table_fqn="cat.schema.tbl")
        with pytest.raises(HTTPException) as excinfo:
            register_monitored_table(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 409

    def test_register_invalid_fqn_raises_400(self):
        svc = MagicMock()
        svc.register.side_effect = ValueError("Invalid fully qualified name")
        body = RegisterMonitoredTableIn(table_fqn="bad-fqn")
        with pytest.raises(HTTPException) as excinfo:
            register_monitored_table(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 400


class TestDelete:
    def test_delete_success(self):
        svc = MagicMock()
        result = delete_monitored_table("b1", svc=svc, obo_ws=_mock_obo_ws())
        assert result == {"status": "deleted", "binding_id": "b1"}
        svc.delete.assert_called_once_with("b1", "alice@x")

    def test_delete_missing_raises_404(self):
        svc = MagicMock()
        svc.delete.side_effect = RuntimeError("Monitored table not found: b1")
        with pytest.raises(HTTPException) as excinfo:
            delete_monitored_table("b1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404


class TestProfile:
    def test_returns_profile(self):
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(), applied_rules=[])
        svc.get_latest_profile.return_value = LatestProfile(
            run_id="run1", source_table_fqn="cat.schema.tbl", rows_profiled=100
        )
        result = get_monitored_table_profile("b1", svc=svc)
        assert result.run_id == "run1"
        assert result.rows_profiled == 100
        svc.get_latest_profile.assert_called_once_with("cat.schema.tbl")

    def test_missing_binding_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            get_monitored_table_profile("missing", svc=svc)
        assert excinfo.value.status_code == 404

    def test_no_profile_results_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(), applied_rules=[])
        svc.get_latest_profile.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            get_monitored_table_profile("b1", svc=svc)
        assert excinfo.value.status_code == 404


class TestApplyRuleToTable:
    def test_apply_success(self):
        svc = MagicMock()
        applied = AppliedRule(id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "id"}])
        svc.apply_rule.return_value = applied
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])
        result = apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert result.id == "ar1"
        svc.apply_rule.assert_called_once_with(
            "b1", "r1", [{"column": "id"}], "alice@x", pinned_version=None, severity_override=None, tags={}
        )

    def test_mapping_incomplete_raises_422(self):
        svc = MagicMock()
        svc.apply_rule.side_effect = MappingIncompleteError("missing slot")
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{}])
        with pytest.raises(HTTPException) as excinfo:
            apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 422

    def test_unpublished_rule_raises_409(self):
        svc = MagicMock()
        svc.apply_rule.side_effect = RuleNotPublishedError("not published")
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])
        with pytest.raises(HTTPException) as excinfo:
            apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 409

    def test_missing_binding_or_rule_raises_404(self):
        svc = MagicMock()
        svc.apply_rule.side_effect = RuntimeError("Monitored table not found: b1")
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])
        with pytest.raises(HTTPException) as excinfo:
            apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404


class TestRemoveAppliedRule:
    def test_remove_success(self):
        svc = MagicMock()
        result = remove_applied_rule("b1", "ar1", svc=svc)
        assert result == {"status": "removed", "binding_id": "b1", "applied_rule_id": "ar1"}
        svc.remove_applied.assert_called_once_with("ar1")

    def test_remove_missing_raises_404(self):
        svc = MagicMock()
        svc.remove_applied.side_effect = RuntimeError("Applied rule not found: ar1")
        with pytest.raises(HTTPException) as excinfo:
            remove_applied_rule("b1", "ar1", svc=svc)
        assert excinfo.value.status_code == 404


class TestSetAppliedRulePin:
    def test_sets_pin(self):
        svc = MagicMock()
        svc.set_pin.return_value = AppliedRule(id="ar1", binding_id="b1", rule_id="r1", pinned_version=2)
        result = set_applied_rule_pin("b1", "ar1", body=SetAppliedRulePinIn(pinned_version=2), svc=svc)
        assert result.pinned_version == 2
        svc.set_pin.assert_called_once_with("ar1", 2)

    def test_missing_raises_404(self):
        svc = MagicMock()
        svc.set_pin.side_effect = RuntimeError("Applied rule not found: ar1")
        with pytest.raises(HTTPException) as excinfo:
            set_applied_rule_pin("b1", "ar1", body=SetAppliedRulePinIn(pinned_version=1), svc=svc)
        assert excinfo.value.status_code == 404


class TestSetAppliedRuleSeverityOverride:
    def test_sets_override(self):
        svc = MagicMock()
        svc.set_severity_override.return_value = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", severity_override="Critical"
        )
        result = set_applied_rule_severity_override(
            "b1", "ar1", body=SetAppliedRuleSeverityOverrideIn(severity="Critical"), svc=svc
        )
        assert result.severity_override == "Critical"
        svc.set_severity_override.assert_called_once_with("ar1", "Critical")

    def test_missing_raises_404(self):
        svc = MagicMock()
        svc.set_severity_override.side_effect = RuntimeError("Applied rule not found: ar1")
        with pytest.raises(HTTPException) as excinfo:
            set_applied_rule_severity_override(
                "b1", "ar1", body=SetAppliedRuleSeverityOverrideIn(severity="Low"), svc=svc
            )
        assert excinfo.value.status_code == 404


class TestPublishMonitoredTable:
    def test_publish_materializes_and_returns_ids(self):
        monitored_tables_svc = MagicMock()
        monitored_tables_svc.publish.return_value = _table(status="published")
        materializer = MagicMock()
        materializer.materialize_binding.return_value = ["ar1-0", "ar2-0"]
        result = publish_monitored_table(
            "b1", monitored_tables_svc=monitored_tables_svc, materializer=materializer, obo_ws=_mock_obo_ws()
        )
        assert result.table.status == "published"
        assert result.materialized_rule_ids == ["ar1-0", "ar2-0"]
        monitored_tables_svc.publish.assert_called_once_with("b1", "alice@x")
        materializer.materialize_binding.assert_called_once_with("b1")

    def test_missing_binding_raises_404(self):
        monitored_tables_svc = MagicMock()
        monitored_tables_svc.publish.side_effect = RuntimeError("Monitored table not found: b1")
        materializer = MagicMock()
        with pytest.raises(HTTPException) as excinfo:
            publish_monitored_table(
                "b1", monitored_tables_svc=monitored_tables_svc, materializer=materializer, obo_ws=_mock_obo_ws()
            )
        assert excinfo.value.status_code == 404

    def test_materialization_error_raises_404(self):
        monitored_tables_svc = MagicMock()
        monitored_tables_svc.publish.return_value = _table(status="published")
        materializer = MagicMock()
        materializer.materialize_binding.side_effect = MaterializationError("Monitored table not found: b1")
        with pytest.raises(HTTPException) as excinfo:
            publish_monitored_table(
                "b1", monitored_tables_svc=monitored_tables_svc, materializer=materializer, obo_ws=_mock_obo_ws()
            )
        assert excinfo.value.status_code == 404


class TestSuggestRulesForTable:
    """The route is a thin async adapter — always returns HTTP 200 (never raises)."""

    async def test_available_suggestions_are_mapped_to_the_response_model(self):
        svc = MagicMock()
        svc.suggest = AsyncMock(
            return_value=SuggestRulesResult(
                available=True,
                suggestions=[
                    RuleSuggestion(
                        rule_id="r1",
                        rule_name="Not Null Check",
                        dimension="Completeness",
                        severity="High",
                        column_mapping={"column": "email"},
                        explanation="email should not be null",
                    )
                ],
            )
        )

        result = await suggest_rules_for_table("b1", svc=svc, obo_ws=_mock_obo_ws())

        assert result.available is True
        assert len(result.suggestions) == 1
        assert result.suggestions[0].rule_id == "r1"
        assert result.suggestions[0].column_mapping == {"column": "email"}
        svc.suggest.assert_called_once_with("b1", "alice@x")

    async def test_unavailable_result_returns_200_with_reason(self):
        svc = MagicMock()
        svc.suggest = AsyncMock(
            return_value=SuggestRulesResult(available=False, reason="Vector Search is not configured.")
        )

        result = await suggest_rules_for_table("b1", svc=svc, obo_ws=_mock_obo_ws())

        assert result.available is False
        assert result.reason == "Vector Search is not configured."
        assert result.suggestions == []
