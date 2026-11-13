"""Tests for the shared monitored-table registration helper."""

from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.registry_models import MonitoredTable
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.services.monitored_table_registration import (
    can_register_tables,
    register_table,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService


def _deps(uc_owner: str | None) -> tuple[MonitoredTableService, DiscoveryService]:
    svc = create_autospec(MonitoredTableService, instance=True)
    svc.register.return_value = MonitoredTable(binding_id="b1", table_fqn="c.s.t")
    discovery = create_autospec(DiscoveryService, instance=True)
    discovery.get_table_owner.return_value = uc_owner
    return svc, discovery


def test_defaults_owner_to_uc_owner():
    svc, discovery = _deps("uc@x")
    table = register_table("c.s.t", "alice@x", monitored_tables=svc, discovery=discovery)
    assert table.binding_id == "b1"
    svc.register.assert_called_once_with("c.s.t", "alice@x", owner="uc@x", owner_display_name=None)


def test_explicit_owner_skips_uc_lookup():
    svc, discovery = _deps("uc@x")
    register_table(
        "c.s.t", "alice@x", monitored_tables=svc, discovery=discovery, owner="bob@x", owner_display_name="Bob"
    )
    discovery.get_table_owner.assert_not_called()
    svc.register.assert_called_once_with("c.s.t", "alice@x", owner="bob@x", owner_display_name="Bob")


def test_can_register_tables_matches_register_route_roles():
    assert can_register_tables(UserRole.ADMIN)
    assert can_register_tables(UserRole.RULE_APPROVER)
    assert can_register_tables(UserRole.RULE_AUTHOR)
    assert not can_register_tables(UserRole.VIEWER)
