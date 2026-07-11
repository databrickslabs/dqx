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
    BulkRegisterMonitoredTablesIn,
    DesiredAppliedRuleIn,
    RegisterMonitoredTableIn,
    SaveAppliedRulesIn,
    SetAppliedRulePinIn,
    SetAppliedRuleSeverityOverrideIn,
)
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable, MonitoredTableVersion
from databricks_labs_dqx_app.backend.routes.v1 import monitored_tables as mt_routes
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import (
    apply_rule_to_table,
    approve_monitored_table,
    bulk_register_monitored_tables,
    delete_monitored_table,
    get_monitored_table,
    get_monitored_table_profile,
    list_monitored_table_versions,
    list_monitored_tables,
    register_monitored_table,
    reject_monitored_table,
    remove_applied_rule,
    run_monitored_table,
    save_applied_rules,
    set_applied_rule_pin,
    set_applied_rule_severity_override,
    submit_monitored_table,
    suggest_rules_for_table,
    update_monitored_table_schedule,
)
from databricks_labs_dqx_app.backend.models import RunMonitoredTableIn, UpdateMonitoredTableScheduleIn
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingNotFoundError,
    BindingRunError,
    BindingRunResult,
    MissingSnapshotError,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError
from databricks_labs_dqx_app.backend.services.rule_suggester import RuleSuggestion, SuggestRulesResult
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    BulkRegisterResult,
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


class TestBulkRegister:
    def test_bulk_register_success(self):
        svc = MagicMock()
        svc.bulk_register.return_value = BulkRegisterResult(
            registered=["cat.schema.a", "cat.schema.b"],
            skipped_existing=["cat.schema.existing"],
            invalid=["bad-fqn"],
        )
        body = BulkRegisterMonitoredTablesIn(
            table_fqns=["cat.schema.a", "cat.schema.b", "cat.schema.existing", "bad-fqn"],
            steward="bob@x",
        )
        result = bulk_register_monitored_tables(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert result.registered == ["cat.schema.a", "cat.schema.b"]
        assert result.skipped_existing == ["cat.schema.existing"]
        assert result.invalid == ["bad-fqn"]
        svc.bulk_register.assert_called_once_with(
            ["cat.schema.a", "cat.schema.b", "cat.schema.existing", "bad-fqn"], "alice@x", steward="bob@x"
        )

    def test_bulk_register_empty_list(self):
        svc = MagicMock()
        svc.bulk_register.return_value = BulkRegisterResult()
        body = BulkRegisterMonitoredTablesIn(table_fqns=[])
        result = bulk_register_monitored_tables(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert result.registered == []
        assert result.skipped_existing == []
        assert result.invalid == []

    def test_bulk_register_propagates_unexpected_errors_as_500(self):
        svc = MagicMock()
        svc.bulk_register.side_effect = RuntimeError("boom")
        body = BulkRegisterMonitoredTablesIn(table_fqns=["cat.schema.a"])
        with pytest.raises(HTTPException) as excinfo:
            bulk_register_monitored_tables(body=body, svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 500


class TestDelete:
    def test_delete_success(self):
        svc = MagicMock()
        result = delete_monitored_table("b1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result == {"status": "deleted", "binding_id": "b1"}
        svc.delete.assert_called_once_with("b1", "alice@x")

    def test_delete_missing_raises_404(self):
        svc = MagicMock()
        svc.delete.side_effect = RuntimeError("Monitored table not found: b1")
        with pytest.raises(HTTPException) as excinfo:
            delete_monitored_table("b1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestUpdateSchedule:
    def test_sets_schedule(self):
        svc = MagicMock()
        table = _table(status="approved")
        table.schedule_cron = "0 6 * * *"
        table.schedule_tz = "UTC"
        svc.update_schedule.return_value = table
        body = UpdateMonitoredTableScheduleIn(schedule_cron="0 6 * * *", schedule_tz="UTC")
        result = update_monitored_table_schedule("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.schedule_cron == "0 6 * * *"
        assert result.schedule_tz == "UTC"
        svc.update_schedule.assert_called_once_with("b1", "0 6 * * *", "UTC", "alice@x")

    def test_clears_schedule(self):
        svc = MagicMock()
        svc.update_schedule.return_value = _table(status="approved")
        body = UpdateMonitoredTableScheduleIn(schedule_cron=None, schedule_tz=None)
        result = update_monitored_table_schedule("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.schedule_cron is None
        svc.update_schedule.assert_called_once_with("b1", None, None, "alice@x")

    def test_missing_raises_404(self):
        svc = MagicMock()
        svc.update_schedule.side_effect = RuntimeError("Monitored table not found: b1")
        body = UpdateMonitoredTableScheduleIn(schedule_cron="0 6 * * *", schedule_tz="UTC")
        with pytest.raises(HTTPException) as excinfo:
            update_monitored_table_schedule("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
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
        result = apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.id == "ar1"
        svc.apply_rule.assert_called_once_with(
            "b1", "r1", [{"column": "id"}], "alice@x", pinned_version=None, severity_override=None, tags={}
        )

    def test_mapping_incomplete_raises_422(self):
        svc = MagicMock()
        svc.apply_rule.side_effect = MappingIncompleteError("missing slot")
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{}])
        with pytest.raises(HTTPException) as excinfo:
            apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 422

    def test_unpublished_rule_raises_409(self):
        svc = MagicMock()
        svc.apply_rule.side_effect = RuleNotPublishedError("not published")
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])
        with pytest.raises(HTTPException) as excinfo:
            apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 409

    def test_missing_binding_or_rule_raises_404(self):
        svc = MagicMock()
        svc.apply_rule.side_effect = RuntimeError("Monitored table not found: b1")
        body = ApplyRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])
        with pytest.raises(HTTPException) as excinfo:
            apply_rule_to_table("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestSaveAppliedRules:
    def test_save_success(self):
        svc = MagicMock()
        applied = AppliedRule(id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "id"}])
        svc.save_applied_rules.return_value = [applied]
        body = SaveAppliedRulesIn(
            applications=[DesiredAppliedRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])]
        )
        result = save_applied_rules("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert [r.id for r in result] == ["ar1"]
        (called_binding_id, called_desired, called_user_email), _kwargs = svc.save_applied_rules.call_args
        assert called_binding_id == "b1"
        assert called_user_email == "alice@x"
        assert [d.rule_id for d in called_desired] == ["r1"]
        assert called_desired[0].column_mapping == [{"column": "id"}]

    def test_empty_applications_removes_everything(self):
        svc = MagicMock()
        svc.save_applied_rules.return_value = []
        body = SaveAppliedRulesIn(applications=[])
        result = save_applied_rules("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result == []
        svc.save_applied_rules.assert_called_once_with("b1", [], "alice@x")

    def test_mapping_incomplete_raises_422(self):
        svc = MagicMock()
        svc.save_applied_rules.side_effect = MappingIncompleteError("missing slot")
        body = SaveAppliedRulesIn(applications=[DesiredAppliedRuleIn(rule_id="r1", column_mapping=[{}])])
        with pytest.raises(HTTPException) as excinfo:
            save_applied_rules("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 422

    def test_unpublished_rule_raises_409(self):
        svc = MagicMock()
        svc.save_applied_rules.side_effect = RuleNotPublishedError("not published")
        body = SaveAppliedRulesIn(
            applications=[DesiredAppliedRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])]
        )
        with pytest.raises(HTTPException) as excinfo:
            save_applied_rules("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 409

    def test_missing_binding_or_rule_raises_404(self):
        svc = MagicMock()
        svc.save_applied_rules.side_effect = RuntimeError("Monitored table not found: b1")
        body = SaveAppliedRulesIn(
            applications=[DesiredAppliedRuleIn(rule_id="r1", column_mapping=[{"column": "id"}])]
        )
        with pytest.raises(HTTPException) as excinfo:
            save_applied_rules("b1", body=body, svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestRemoveAppliedRule:
    def test_remove_success(self):
        svc = MagicMock()
        result = remove_applied_rule("b1", "ar1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result == {"status": "removed", "binding_id": "b1", "applied_rule_id": "ar1"}
        svc.remove_applied.assert_called_once_with("ar1")

    def test_remove_missing_raises_404(self):
        svc = MagicMock()
        svc.remove_applied.side_effect = RuntimeError("Applied rule not found: ar1")
        with pytest.raises(HTTPException) as excinfo:
            remove_applied_rule("b1", "ar1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestSetAppliedRulePin:
    def test_sets_pin(self):
        svc = MagicMock()
        svc.set_pin.return_value = AppliedRule(id="ar1", binding_id="b1", rule_id="r1", pinned_version=2)
        result = set_applied_rule_pin("b1", "ar1", body=SetAppliedRulePinIn(pinned_version=2), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.pinned_version == 2
        svc.set_pin.assert_called_once_with("ar1", 2)

    def test_missing_raises_404(self):
        svc = MagicMock()
        svc.set_pin.side_effect = RuntimeError("Applied rule not found: ar1")
        with pytest.raises(HTTPException) as excinfo:
            set_applied_rule_pin("b1", "ar1", body=SetAppliedRulePinIn(pinned_version=1), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestSetAppliedRuleSeverityOverride:
    def test_sets_override(self):
        svc = MagicMock()
        svc.set_severity_override.return_value = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", severity_override="Critical"
        )
        result = set_applied_rule_severity_override(
            "b1", "ar1", body=SetAppliedRuleSeverityOverrideIn(severity="Critical"), svc=svc
        , obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.severity_override == "Critical"
        svc.set_severity_override.assert_called_once_with("ar1", "Critical")

    def test_missing_raises_404(self):
        svc = MagicMock()
        svc.set_severity_override.side_effect = RuntimeError("Applied rule not found: ar1")
        with pytest.raises(HTTPException) as excinfo:
            set_applied_rule_severity_override(
                "b1", "ar1", body=SetAppliedRuleSeverityOverrideIn(severity="Low"), svc=svc
            , obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


def _route_required_roles(operation_id: str) -> set[UserRole]:
    """Extract the ``require_role(...)`` role set declared on a route.

    ``require_role(*roles)`` returns ``Depends(_check)`` where ``_check``
    closes over the ``roles`` tuple; pull it out of the closure so we can
    assert the RBAC gate structurally without spinning up a TestClient
    (matching this file's call-the-handler-directly convention).
    """
    for route in mt_routes.router.routes:
        if getattr(route, "operation_id", None) != operation_id:
            continue
        for dep in route.dependencies:
            for cell in getattr(dep.dependency, "__closure__", None) or ():
                val = cell.cell_contents
                if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                    return set(val)
    raise AssertionError(f"No require_role dependency found for {operation_id}")


class TestSubmitMonitoredTable:
    def test_materializes_transitions_draft_checks_and_rolls_up(self):
        svc = MagicMock()
        svc.list_materialized_rule_statuses.side_effect = [
            [("r1", "draft"), ("r2", "draft")],  # recovery scan (rejected -> draft): none
            [("r1", "draft"), ("r2", "draft")],  # read inside the draft -> pending loop
            [("r1", "pending_approval"), ("r2", "pending_approval")],  # read for the roll-up
        ]
        svc.set_status.return_value = _table(status="pending_approval")
        materializer = MagicMock()
        materializer.materialize_binding.return_value = ["r1", "r2"]
        rules_catalog = MagicMock()
        result = submit_monitored_table(
            "b1",
            monitored_tables_svc=svc,
            materializer=materializer,
            rules_catalog=rules_catalog,
            obo_ws=_mock_obo_ws(),
        )
        assert result.table.status == "pending_approval"
        assert result.affected_check_count == 2
        materializer.materialize_binding.assert_called_once_with("b1")
        rules_catalog.set_status.assert_any_call("r1", "pending_approval", "alice@x")
        rules_catalog.set_status.assert_any_call("r2", "pending_approval", "alice@x")
        svc.set_status.assert_called_once_with("b1", "pending_approval", "alice@x")

    def test_only_draft_checks_are_submitted(self):
        svc = MagicMock()
        svc.list_materialized_rule_statuses.side_effect = [
            [("r1", "draft"), ("r2", "approved")],  # recovery scan: no rejected
            [("r1", "draft"), ("r2", "approved")],  # draft -> pending loop
            [("r1", "pending_approval"), ("r2", "approved")],  # roll-up
        ]
        svc.set_status.return_value = _table(status="pending_approval")
        rules_catalog = MagicMock()
        result = submit_monitored_table(
            "b1",
            monitored_tables_svc=svc,
            materializer=MagicMock(),
            rules_catalog=rules_catalog,
            obo_ws=_mock_obo_ws(),
        )
        assert result.affected_check_count == 1
        rules_catalog.set_status.assert_called_once_with("r1", "pending_approval", "alice@x")

    def test_rejected_checks_are_recovered_on_unchanged_resubmit(self):
        """Regression (P16-H): after a reject, an unchanged re-submit must walk
        rejected checks back through ``rejected -> draft -> pending_approval``,
        count them, and roll the binding up to ``pending_approval`` — not leave
        them stuck at ``rejected`` with a false-success ``affected_check_count=0``
        and the binding flipping down to ``draft``.
        """
        svc = MagicMock()
        svc.list_materialized_rule_statuses.side_effect = [
            [("r1", "rejected"), ("r2", "rejected")],  # recovery scan: rejected -> draft
            [("r1", "draft"), ("r2", "draft")],  # draft -> pending_approval loop
            [("r1", "pending_approval"), ("r2", "pending_approval")],  # roll-up
        ]
        svc.set_status.return_value = _table(status="pending_approval")
        materializer = MagicMock()
        materializer.materialize_binding.return_value = []  # unchanged: nothing re-drafted
        rules_catalog = MagicMock()
        result = submit_monitored_table(
            "b1",
            monitored_tables_svc=svc,
            materializer=materializer,
            rules_catalog=rules_catalog,
            obo_ws=_mock_obo_ws(),
        )
        assert result.table.status == "pending_approval"
        assert result.affected_check_count == 2
        # rejected -> draft recovery hop, then draft -> pending_approval hop.
        rules_catalog.set_status.assert_any_call("r1", "draft", "alice@x")
        rules_catalog.set_status.assert_any_call("r2", "draft", "alice@x")
        rules_catalog.set_status.assert_any_call("r1", "pending_approval", "alice@x")
        rules_catalog.set_status.assert_any_call("r2", "pending_approval", "alice@x")
        svc.set_status.assert_called_once_with("b1", "pending_approval", "alice@x")

    def test_row_transition_failure_is_skipped_not_fatal(self):
        svc = MagicMock()
        svc.list_materialized_rule_statuses.side_effect = [
            [("r1", "draft"), ("r2", "draft")],  # recovery scan: no rejected
            [("r1", "draft"), ("r2", "draft")],  # draft -> pending loop
            [("r1", "draft"), ("r2", "pending_approval")],  # roll-up
        ]
        svc.set_status.return_value = _table(status="pending_approval")
        rules_catalog = MagicMock()
        rules_catalog.set_status.side_effect = [ValueError("duplicate pending"), MagicMock()]
        result = submit_monitored_table(
            "b1",
            monitored_tables_svc=svc,
            materializer=MagicMock(),
            rules_catalog=rules_catalog,
            obo_ws=_mock_obo_ws(),
        )
        # r1 failed and was skipped; r2 counted.
        assert result.affected_check_count == 1

    def test_materialization_error_raises_404(self):
        svc = MagicMock()
        materializer = MagicMock()
        materializer.materialize_binding.side_effect = MaterializationError("Monitored table not found: b1")
        with pytest.raises(HTTPException) as excinfo:
            submit_monitored_table(
                "b1",
                monitored_tables_svc=svc,
                materializer=materializer,
                rules_catalog=MagicMock(),
                obo_ws=_mock_obo_ws(),
            )
        assert excinfo.value.status_code == 404


class TestListMonitoredTableVersions:
    def test_maps_domain_versions_to_dto_newest_first(self):
        version_svc = MagicMock()
        version_svc.list_versions.return_value = [
            MonitoredTableVersion(id="v2", binding_id="b1", version=2, state_json={"applied_rules": []}),
            MonitoredTableVersion(id="v1", binding_id="b1", version=1),
        ]
        result = list_monitored_table_versions("b1", version_svc=version_svc)
        assert [v.version for v in result] == [2, 1]
        # checks_json is never surfaced by this listing endpoint.
        assert not hasattr(result[0], "checks_json")
        version_svc.list_versions.assert_called_once_with("b1")

    def test_empty_when_never_approved(self):
        version_svc = MagicMock()
        version_svc.list_versions.return_value = []
        assert list_monitored_table_versions("b1", version_svc=version_svc) == []

    def test_service_error_raises_500(self):
        version_svc = MagicMock()
        version_svc.list_versions.side_effect = RuntimeError("boom")
        with pytest.raises(HTTPException) as excinfo:
            list_monitored_table_versions("b1", version_svc=version_svc)
        assert excinfo.value.status_code == 500


class TestApproveMonitoredTable:
    def test_approves_pending_checks_and_rolls_up(self):
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(status="pending_approval"), applied_rules=[])
        svc.list_materialized_rule_statuses.side_effect = [
            [("r1", "pending_approval"), ("r2", "pending_approval")],
            [("r1", "approved"), ("r2", "approved")],
        ]
        svc.set_status.return_value = _table(status="approved")
        rules_catalog = MagicMock()
        version_svc = MagicMock()
        version_svc.freeze_new_version.return_value = 1
        result = approve_monitored_table(
            "b1", monitored_tables_svc=svc, rules_catalog=rules_catalog, version_svc=version_svc, obo_ws=_mock_obo_ws()
        )
        assert result.table.status == "approved"
        assert result.affected_check_count == 2
        rules_catalog.set_status.assert_any_call("r1", "approved", "alice@x")
        svc.set_status.assert_called_once_with("b1", "approved", "alice@x")

    def test_approve_freezes_a_new_version_and_returns_it(self):
        """Table approval bumps + freezes the version; the response carries it."""
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(status="pending_approval"), applied_rules=[])
        svc.list_materialized_rule_statuses.side_effect = [
            [("r1", "pending_approval")],
            [("r1", "approved")],
        ]
        svc.set_status.return_value = _table(status="approved")
        version_svc = MagicMock()
        version_svc.freeze_new_version.return_value = 2  # 1 -> 2 bump
        result = approve_monitored_table(
            "b1", monitored_tables_svc=svc, rules_catalog=MagicMock(), version_svc=version_svc, obo_ws=_mock_obo_ws()
        )
        version_svc.freeze_new_version.assert_called_once_with("b1", "alice@x")
        assert result.new_version == 2

    def test_missing_binding_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = None
        version_svc = MagicMock()
        with pytest.raises(HTTPException) as excinfo:
            approve_monitored_table(
                "b1", monitored_tables_svc=svc, rules_catalog=MagicMock(), version_svc=version_svc, obo_ws=_mock_obo_ws()
            )
        assert excinfo.value.status_code == 404
        svc.set_status.assert_not_called()
        version_svc.freeze_new_version.assert_not_called()

    def test_approve_on_draft_binding_raises_409_and_state_unchanged(self):
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(status="draft"), applied_rules=[])
        rules_catalog = MagicMock()
        version_svc = MagicMock()
        with pytest.raises(HTTPException) as excinfo:
            approve_monitored_table(
                "b1", monitored_tables_svc=svc, rules_catalog=rules_catalog, version_svc=version_svc, obo_ws=_mock_obo_ws()
            )
        assert excinfo.value.status_code == 409
        # Neither the checks, the binding's own status, nor the version should be touched.
        rules_catalog.set_status.assert_not_called()
        svc.set_status.assert_not_called()
        version_svc.freeze_new_version.assert_not_called()


class TestRejectMonitoredTable:
    def test_rejects_pending_checks_and_flips_binding(self):
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(status="pending_approval"), applied_rules=[])
        svc.list_materialized_rule_statuses.return_value = [("r1", "pending_approval")]
        svc.set_status.return_value = _table(status="rejected")
        rules_catalog = MagicMock()
        result = reject_monitored_table(
            "b1", monitored_tables_svc=svc, rules_catalog=rules_catalog, obo_ws=_mock_obo_ws()
        )
        assert result.table.status == "rejected"
        assert result.affected_check_count == 1
        rules_catalog.set_status.assert_called_once_with("r1", "rejected", "alice@x")
        svc.set_status.assert_called_once_with("b1", "rejected", "alice@x")

    def test_missing_binding_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            reject_monitored_table("b1", monitored_tables_svc=svc, rules_catalog=MagicMock(), obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404
        svc.set_status.assert_not_called()

    def test_reject_on_approved_binding_raises_409_and_state_unchanged(self):
        """Regression: rejecting an already-approved binding must not flip its
        status to 'rejected' while its materialized checks stay 'approved' and
        keep executing in the scheduler (per-rule transitions only move
        'pending_approval' rows — VALID_TRANSITIONS['approved'] = {'draft'})."""
        svc = MagicMock()
        svc.get.return_value = MonitoredTableDetail(table=_table(status="approved"), applied_rules=[])
        rules_catalog = MagicMock()
        with pytest.raises(HTTPException) as excinfo:
            reject_monitored_table(
                "b1", monitored_tables_svc=svc, rules_catalog=rules_catalog, obo_ws=_mock_obo_ws()
            )
        assert excinfo.value.status_code == 409
        # Neither the materialized checks nor the binding's own status should be touched.
        rules_catalog.set_status.assert_not_called()
        svc.set_status.assert_not_called()


class TestLifecycleRbac:
    """RBAC is declared on the routes via ``require_role`` — authors submit,
    only approvers/admins approve or reject (mirrors ``routes/v1/rules.py``)."""

    def test_submit_allows_authors_and_above(self):
        assert _route_required_roles("submitMonitoredTable") == {
            UserRole.ADMIN,
            UserRole.RULE_APPROVER,
            UserRole.RULE_AUTHOR,
        }

    def test_approve_is_approvers_only_author_excluded(self):
        roles = _route_required_roles("approveMonitoredTable")
        assert roles == {UserRole.ADMIN, UserRole.RULE_APPROVER}
        assert UserRole.RULE_AUTHOR not in roles

    def test_reject_is_approvers_only_author_excluded(self):
        roles = _route_required_roles("rejectMonitoredTable")
        assert roles == {UserRole.ADMIN, UserRole.RULE_APPROVER}
        assert UserRole.RULE_AUTHOR not in roles


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


class TestRunMonitoredTable:
    """``POST /monitored-tables/{binding_id}/run`` — resolution matrix + RBAC gate."""

    def test_gated_by_require_runner_not_a_primary_role(self):
        """The route must use the orthogonal RUNNER gate (matching batch_run_from_catalog),
        not a plain ``require_role(...)`` primary-role check — non-runner authors/approvers
        must still be rejected even though they'd pass every ``require_role`` check.
        """
        for route in mt_routes.router.routes:
            if getattr(route, "operation_id", None) != "runMonitoredTable":
                continue
            qualnames = {getattr(dep.dependency, "__qualname__", "") for dep in route.dependencies}
            assert any("require_runner" in q for q in qualnames)
            return
        raise AssertionError("No route found for operation_id=runMonitoredTable")

    def test_success_maps_result_to_response_model(self):
        svc = MagicMock()
        svc.run_binding.return_value = BindingRunResult(
            run_set_id="rs-1", run_id="run-1", job_run_id=42, view_fqn="dqx_studio_tmp.tmp_1"
        )
        result = run_monitored_table(
            "b1",
            body=RunMonitoredTableIn(source="approved", version=None),
            obo_ws=_mock_obo_ws(),
            run_svc=svc,
        )
        assert result.run_set_id == "rs-1"
        assert result.run_id == "run-1"
        assert result.job_run_id == 42
        assert result.view_fqn == "dqx_studio_tmp.tmp_1"
        # Exact-match assert: NO sample_size kwarg. Sampling is not a
        # per-request choice — approved runs always scan the whole table
        # and draft runs are capped by the admin setting, both resolved
        # inside run_binding itself.
        svc.run_binding.assert_called_once_with(
            "b1", source="approved", version=None, user_email="alice@x", trigger="manual"
        )

    def test_binding_not_found_maps_to_404(self):
        svc = MagicMock()
        svc.run_binding.side_effect = BindingNotFoundError("Monitored table not found: b1")
        with pytest.raises(HTTPException) as excinfo:
            run_monitored_table(
                "b1", body=RunMonitoredTableIn(source="draft"), obo_ws=_mock_obo_ws(), run_svc=svc
            )
        assert excinfo.value.status_code == 404

    def test_never_approved_maps_to_409(self):
        svc = MagicMock()
        svc.run_binding.side_effect = NeverApprovedError("never approved")
        with pytest.raises(HTTPException) as excinfo:
            run_monitored_table(
                "b1", body=RunMonitoredTableIn(source="approved"), obo_ws=_mock_obo_ws(), run_svc=svc
            )
        assert excinfo.value.status_code == 409

    def test_missing_snapshot_maps_to_422(self):
        svc = MagicMock()
        svc.run_binding.side_effect = MissingSnapshotError("no snapshot")
        with pytest.raises(HTTPException) as excinfo:
            run_monitored_table(
                "b1", body=RunMonitoredTableIn(source="approved", version=9), obo_ws=_mock_obo_ws(), run_svc=svc
            )
        assert excinfo.value.status_code == 422

    def test_generic_binding_run_error_maps_to_400(self):
        svc = MagicMock()
        svc.run_binding.side_effect = BindingRunError("missing sql_query")
        with pytest.raises(HTTPException) as excinfo:
            run_monitored_table(
                "b1", body=RunMonitoredTableIn(source="approved", version=1), obo_ws=_mock_obo_ws(), run_svc=svc
            )
        assert excinfo.value.status_code == 400

    def test_unexpected_error_maps_to_500(self):
        svc = MagicMock()
        svc.run_binding.side_effect = RuntimeError("boom")
        with pytest.raises(HTTPException) as excinfo:
            run_monitored_table(
                "b1", body=RunMonitoredTableIn(source="draft"), obo_ws=_mock_obo_ws(), run_svc=svc
            )
        assert excinfo.value.status_code == 500


class TestRunMonitoredTableInHasNoSamplingKnob:
    """Pin that ``RunMonitoredTableIn`` exposes NO sample_size field.

    Approved/published runs must never sample (they always scan the
    whole table) and draft runs are capped by the admin setting
    ``draft_run_sample_limit`` — sampling is deliberately not a
    per-request choice. Reintroducing the field would let API callers
    silently turn monitoring runs into sample scans.
    """

    def test_sample_size_field_removed(self):
        assert "sample_size" not in RunMonitoredTableIn.model_fields

    def test_stray_sample_size_is_ignored_not_accepted(self):
        body = RunMonitoredTableIn.model_validate({"source": "approved", "sample_size": 50})
        assert not hasattr(body, "sample_size")


# ---------------------------------------------------------------------------
# getMonitoredTableVersionChecks — change-diff backing
# ---------------------------------------------------------------------------


class TestVersionChecks:
    def test_returns_frozen_checks(self):
        version_svc = MagicMock()
        version_svc.get_checks.return_value = [{"check": {"function": "is_not_null"}}]
        out = mt_routes.get_monitored_table_version_checks("b1", 2, version_svc)
        assert out.binding_id == "b1"
        assert out.version == 2
        assert out.checks == [{"check": {"function": "is_not_null"}}]
        version_svc.get_checks.assert_called_once_with("b1", 2)

    def test_missing_snapshot_returns_empty(self):
        version_svc = MagicMock()
        version_svc.get_checks.side_effect = LookupError("no snapshot")
        out = mt_routes.get_monitored_table_version_checks("b1", 9, version_svc)
        assert out.checks == []

    def test_unexpected_error_maps_to_500(self):
        version_svc = MagicMock()
        version_svc.get_checks.side_effect = RuntimeError("boom")
        with pytest.raises(HTTPException) as exc:
            mt_routes.get_monitored_table_version_checks("b1", 1, version_svc)
        assert exc.value.status_code == 500
