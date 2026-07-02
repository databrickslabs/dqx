"""Tests for the ``/monitored-tables`` route handlers.

Follows ``test_registry_rules_routes.py``'s convention: call the route
functions directly with mocked dependencies (``MonitoredTableService``, OBO
``WorkspaceClient``) rather than spinning up a FastAPI ``TestClient`` — the
routes themselves are thin adapters over ``MonitoredTableService``, whose
behaviour is already covered by ``test_monitored_table_service.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.models import RegisterMonitoredTableIn
from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import (
    delete_monitored_table,
    get_monitored_table,
    get_monitored_table_profile,
    list_monitored_tables,
    register_monitored_table,
)
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
