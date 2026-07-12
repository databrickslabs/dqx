"""Tests for the approvals-mode setting and its submit-path wiring (issue #94).

Covers, in order:

* the pure ``backend.common.approvals`` decision helpers (no I/O);
* ``AppSettingsService`` get/save of the mode;
* the ``/config/approvals-mode`` route (GET default + PUT validation);
* ``PermissionsService.can_edit_and_approve`` (the auto-bypass predicate);
* the four submit routes (rules / registry rules / monitored tables / table
  spaces) across all three modes × (admin / approver / plain author), asserting
  who gets auto-approved vs left pending and that the auto approver is recorded
  with an ``(auto)`` marker.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.approvals import (
    AUTO_APPROVER_SUFFIX,
    ApprovalMode,
    mark_auto_approver,
    normalize_approvals_mode,
    should_auto_approve,
)
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType
from databricks_labs_dqx_app.backend.registry_models import (
    DataProduct,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductDetail
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RuleCatalogEntry
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol


def _registry_rule(status: str = "pending_approval", version: int = 0) -> RegistryRule:
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            "slots": [{"name": "column", "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )
    return RegistryRule(rule_id="rr1", mode="dqx_native", status=status, version=version, definition=definition)


# ---------------------------------------------------------------------------
# Pure decision helpers
# ---------------------------------------------------------------------------


class TestNormalizeApprovalsMode:
    @pytest.mark.parametrize("raw", ["enabled", "auto_bypass", "disabled"])
    def test_valid_values_pass_through(self, raw):
        assert normalize_approvals_mode(raw) == raw

    def test_case_and_whitespace_insensitive(self):
        assert normalize_approvals_mode("  Auto_Bypass ") == ApprovalMode.AUTO_BYPASS

    @pytest.mark.parametrize("raw", [None, "", "nonsense", "off"])
    def test_unset_or_unknown_falls_back_to_enabled(self, raw):
        assert normalize_approvals_mode(raw) == ApprovalMode.ENABLED


class TestShouldAutoApprove:
    def test_enabled_never_auto_approves(self):
        assert should_auto_approve(ApprovalMode.ENABLED, can_edit_and_approve=True) is False
        assert should_auto_approve(ApprovalMode.ENABLED, can_edit_and_approve=False) is False

    def test_disabled_always_auto_approves(self):
        assert should_auto_approve(ApprovalMode.DISABLED, can_edit_and_approve=True) is True
        assert should_auto_approve(ApprovalMode.DISABLED, can_edit_and_approve=False) is True

    def test_auto_bypass_depends_on_predicate(self):
        assert should_auto_approve(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=True) is True
        assert should_auto_approve(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=False) is False

    def test_corrupt_mode_treated_as_enabled(self):
        assert should_auto_approve("garbage", can_edit_and_approve=True) is False


class TestMarkAutoApprover:
    def test_appends_marker(self):
        assert mark_auto_approver("alice@x") == f"alice@x{AUTO_APPROVER_SUFFIX}"

    def test_is_idempotent(self):
        once = mark_auto_approver("alice@x")
        assert mark_auto_approver(once) == once


# ---------------------------------------------------------------------------
# AppSettingsService
# ---------------------------------------------------------------------------


def _wire_stateful_store(sql_executor_mock) -> dict[str, str]:
    store: dict[str, str] = {}

    def _upsert(_table, *, key_cols, value_cols, **_kwargs):
        store[key_cols["setting_key"]] = value_cols["setting_value"]

    def _query(sql):
        for key, value in store.items():
            if f"'{key}'" in sql:
                return [(value,)]
        return []

    sql_executor_mock.upsert.side_effect = _upsert
    sql_executor_mock.query.side_effect = _query
    return store


@pytest.fixture
def settings_svc(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


class TestAppSettingsApprovalsMode:
    def test_defaults_to_enabled(self, settings_svc):
        assert settings_svc.get_approvals_mode() == ApprovalMode.ENABLED

    @pytest.mark.parametrize("mode", ["enabled", "auto_bypass", "disabled"])
    def test_round_trip(self, settings_svc, sql_executor_mock, mode):
        _wire_stateful_store(sql_executor_mock)
        assert settings_svc.save_approvals_mode(mode, user_email="admin@x") == mode
        assert settings_svc.get_approvals_mode() == mode

    def test_invalid_mode_raises(self, settings_svc):
        with pytest.raises(ValueError):
            settings_svc.save_approvals_mode("sometimes")

    def test_corrupt_stored_value_reads_as_enabled(self, settings_svc, sql_executor_mock):
        store = _wire_stateful_store(sql_executor_mock)
        store["approvals_mode"] = "banana"
        assert settings_svc.get_approvals_mode() == ApprovalMode.ENABLED


# ---------------------------------------------------------------------------
# /config/approvals-mode route
# ---------------------------------------------------------------------------


class TestApprovalsModeRoute:
    def test_get_returns_default(self, settings_svc):
        from databricks_labs_dqx_app.backend.routes.v1.config import get_approvals_mode

        assert get_approvals_mode(settings_svc).mode == ApprovalMode.ENABLED

    def test_put_saves_valid_mode(self, settings_svc, sql_executor_mock):
        from databricks_labs_dqx_app.backend.routes.v1.config import ApprovalsModeIn, save_approvals_mode

        _wire_stateful_store(sql_executor_mock)
        out = save_approvals_mode(ApprovalsModeIn(mode="disabled"), settings_svc, "admin@x")
        assert out.mode == "disabled"

    def test_put_rejects_invalid_mode(self, settings_svc):
        from databricks_labs_dqx_app.backend.routes.v1.config import ApprovalsModeIn, save_approvals_mode

        with pytest.raises(HTTPException) as exc:
            save_approvals_mode(ApprovalsModeIn(mode="whenever"), settings_svc, "admin@x")
        assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# PermissionsService.can_edit_and_approve — the auto-bypass predicate
# ---------------------------------------------------------------------------


def _perms_service() -> tuple[PermissionsService, MagicMock]:
    """Build a PermissionsService over a stubbed SQL executor.

    Returns the service together with its ``sql`` mock so tests can assert on
    the executor's call surface directly, rather than reaching into the
    service's private ``_sql`` attribute.
    """
    sql = create_autospec(OltpExecutorProtocol, instance=True)
    sql.fqn.side_effect = lambda t: t
    sql.query.return_value = []  # no object grants stored
    return PermissionsService(sql=sql, app_settings=MagicMock()), sql


class TestCanEditAndApprove:
    @pytest.mark.parametrize(
        ("role", "expected"),
        [
            (UserRole.ADMIN, True),
            (UserRole.RULE_APPROVER, True),
            (UserRole.RULE_AUTHOR, False),
            (UserRole.VIEWER, False),
        ],
    )
    def test_predicate_by_role(self, role, expected):
        perms, _sql = _perms_service()
        result = perms.can_edit_and_approve(
            ObjectType.REGISTRY_RULE.value,
            "obj-1",
            role=role,
            principal_ids={"me"},
            owner_email="someone@x",
            principal_email="me@x",
        )
        assert result is expected

    def test_admin_short_circuits_without_grant_lookup(self):
        perms, sql = _perms_service()
        perms.can_edit_and_approve(
            ObjectType.MONITORED_TABLE.value,
            "b1",
            role=UserRole.ADMIN,
            principal_ids=set(),
        )
        # ADMIN returns True before any privilege resolution query.
        sql.query.assert_not_called()


# ---------------------------------------------------------------------------
# Route wiring — rules.py submit_for_approval
# ---------------------------------------------------------------------------


def _mock_obo_ws(user_email: str) -> MagicMock:
    obo = MagicMock()
    me = MagicMock()
    me.user_name = user_email
    obo.current_user.me.return_value = me
    return obo


def _app_settings(mode: str) -> MagicMock:
    svc = MagicMock()
    svc.get_approvals_mode.return_value = mode
    return svc


class TestRulesSubmitWiring:
    """The per-table rule submit route auto-approves per the mode + role."""

    @staticmethod
    def _call(mode: str, role: UserRole):
        from databricks_labs_dqx_app.backend.routes.v1.rules import submit_for_approval

        svc = MagicMock()
        # Owned by alice so the author-role ownership gate passes.
        svc.get_by_rule_id.return_value = RuleCatalogEntry(
            table_fqn="c.s.t", checks=[], created_by="alice@x", rule_id="r1"
        )
        svc.set_status.return_value = RuleCatalogEntry(
            table_fqn="c.s.t", checks=[], version=1, status="approved", source="ui",
            created_by="alice@x", rule_id="r1",
        )
        version_svc = MagicMock()
        submit_for_approval(
            rule_id="r1",
            svc=svc,
            version_svc=version_svc,
            app_settings=_app_settings(mode),
            draft_run_gate=MagicMock(),
            obo_ws=_mock_obo_ws("alice@x"),
            user_role=role,
            body=None,
        )
        return svc

    def test_enabled_never_auto_approves(self):
        svc = self._call(ApprovalMode.ENABLED, UserRole.ADMIN)
        # Only the pending_approval transition happened.
        statuses = [c.args[1] for c in svc.set_status.call_args_list]
        assert statuses == ["pending_approval"]

    def test_disabled_auto_approves_even_for_author(self):
        svc = self._call(ApprovalMode.DISABLED, UserRole.RULE_AUTHOR)
        statuses = [c.args[1] for c in svc.set_status.call_args_list]
        assert statuses == ["pending_approval", "approved"]
        # Auto approver recorded with the (auto) marker.
        approver = svc.set_status.call_args_list[-1].args[2]
        assert approver.endswith(AUTO_APPROVER_SUFFIX)
        assert approver.startswith("alice@x")

    def test_auto_bypass_approves_for_approver(self):
        svc = self._call(ApprovalMode.AUTO_BYPASS, UserRole.RULE_APPROVER)
        statuses = [c.args[1] for c in svc.set_status.call_args_list]
        assert statuses == ["pending_approval", "approved"]

    def test_auto_bypass_leaves_author_pending(self):
        svc = self._call(ApprovalMode.AUTO_BYPASS, UserRole.RULE_AUTHOR)
        statuses = [c.args[1] for c in svc.set_status.call_args_list]
        assert statuses == ["pending_approval"]


# ---------------------------------------------------------------------------
# Route wiring — registry_rules.py submit_registry_rule
# ---------------------------------------------------------------------------


class TestRegistrySubmitWiring:
    @staticmethod
    def _call(mode: str, can_edit_and_approve: bool):
        from databricks_labs_dqx_app.backend.routes.v1.registry_rules import submit_registry_rule

        svc = MagicMock()
        svc.submit.return_value = _registry_rule(status="pending_approval")
        svc.approve.return_value = _registry_rule(status="approved", version=1)
        materializer = MagicMock()
        materializer.rematerialize_for_rule.return_value = []
        perms = MagicMock()
        perms.can_edit_and_approve.return_value = can_edit_and_approve
        perms.get_object_owner.return_value = "owner@x"
        app_settings = _app_settings(mode)
        app_settings.get_auto_upgrade_without_approval.return_value = False
        submit_registry_rule(
            rule_id="rr1",
            svc=svc,
            embeddings=MagicMock(),
            materializer=materializer,
            version_svc=MagicMock(),
            monitored_tables=MagicMock(),
            app_settings=app_settings,
            perms=perms,
            role=UserRole.RULE_APPROVER,
            principal_ids=frozenset({"me"}),
            user_email="alice@x",
        )
        return svc

    def test_enabled_only_submits(self):
        svc = self._call(ApprovalMode.ENABLED, can_edit_and_approve=True)
        svc.submit.assert_called_once()
        svc.approve.assert_not_called()

    def test_disabled_auto_publishes(self):
        svc = self._call(ApprovalMode.DISABLED, can_edit_and_approve=False)
        svc.approve.assert_called_once()
        assert svc.approve.call_args.args[1].endswith(AUTO_APPROVER_SUFFIX)

    def test_auto_bypass_publishes_when_predicate_true(self):
        svc = self._call(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=True)
        svc.approve.assert_called_once()

    def test_auto_bypass_pending_when_predicate_false(self):
        svc = self._call(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=False)
        svc.approve.assert_not_called()


# ---------------------------------------------------------------------------
# Route wiring — monitored_tables.py submit_monitored_table
# ---------------------------------------------------------------------------


class TestMonitoredTableSubmitWiring:
    @staticmethod
    def _call(mode: str, can_edit_and_approve: bool):
        from databricks_labs_dqx_app.backend.routes.v1 import monitored_tables as mt

        mt_svc = MagicMock()
        # Two checks: after submit both are pending_approval; the approve half
        # then transitions them. list_materialized_rule_statuses is called by
        # both _transition_binding_checks and _rollup_binding_status, so vary
        # the return over successive calls.
        mt_svc.list_materialized_rule_statuses.side_effect = _statuses_sequence()
        mt_svc.set_status.return_value = MonitoredTable(
            binding_id="b1", table_fqn="cat.schema.tbl", status="approved"
        )
        rules_catalog = MagicMock()
        version_svc = MagicMock()
        version_svc.freeze_new_version.return_value = 2
        perms = MagicMock()
        perms.can_edit_and_approve.return_value = can_edit_and_approve
        perms.get_object_owner.return_value = "owner@x"
        mt.submit_monitored_table(
            binding_id="b1",
            monitored_tables_svc=mt_svc,
            materializer=MagicMock(),
            rules_catalog=rules_catalog,
            version_svc=version_svc,
            app_settings=_app_settings(mode),
            draft_run_gate=MagicMock(),
            perms=perms,
            role=UserRole.RULE_APPROVER,
            principal_ids=frozenset({"me"}),
            obo_ws=_mock_obo_ws("alice@x"),
        )
        return version_svc, rules_catalog

    def test_enabled_does_not_freeze_version(self):
        version_svc, _ = self._call(ApprovalMode.ENABLED, can_edit_and_approve=True)
        version_svc.freeze_new_version.assert_not_called()

    def test_disabled_freezes_version_with_auto_marker(self):
        version_svc, _ = self._call(ApprovalMode.DISABLED, can_edit_and_approve=False)
        version_svc.freeze_new_version.assert_called_once()
        assert version_svc.freeze_new_version.call_args.args[1].endswith(AUTO_APPROVER_SUFFIX)

    def test_auto_bypass_freezes_when_predicate_true(self):
        version_svc, _ = self._call(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=True)
        version_svc.freeze_new_version.assert_called_once()

    def test_auto_bypass_pending_when_predicate_false(self):
        version_svc, _ = self._call(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=False)
        version_svc.freeze_new_version.assert_not_called()


def _statuses_sequence():
    """Yield materialized-check status lists for successive service calls.

    submit path: recover-rejected pass (none), draft->pending pass (2 drafts),
    rollup after submit (2 pending). If auto-approve fires: approve pass
    (2 pending), rollup after approve (2 approved). Extra trailing values keep
    the iterator from exhausting if the mode doesn't auto-approve.
    """
    return iter(
        [
            [],  # recover rejected: nothing to recover
            [("r1", "draft"), ("r2", "draft")],  # draft -> pending_approval
            [("r1", "pending_approval"), ("r2", "pending_approval")],  # rollup after submit
            [("r1", "pending_approval"), ("r2", "pending_approval")],  # approve pass (if any)
            [("r1", "approved"), ("r2", "approved")],  # rollup after approve
            [("r1", "approved"), ("r2", "approved")],  # spare
        ]
    )


# ---------------------------------------------------------------------------
# Route wiring — data_products.py submit_data_product
# ---------------------------------------------------------------------------


class TestDataProductSubmitWiring:
    @staticmethod
    def _call(mode: str, can_edit_and_approve: bool):
        from databricks_labs_dqx_app.backend.routes.v1.data_products import submit_data_product

        svc = MagicMock()
        svc.get.return_value = DataProductDetail(
            product=DataProduct(product_id="p1", name="Orders", status="approved", version=1)
        )
        perms = MagicMock()
        perms.can_edit_and_approve.return_value = can_edit_and_approve
        perms.get_object_owner.return_value = "owner@x"
        submit_data_product(
            product_id="p1",
            svc=svc,
            app_settings=_app_settings(mode),
            draft_run_gate=MagicMock(),
            perms=perms,
            role=UserRole.RULE_APPROVER,
            principal_ids=frozenset({"me"}),
            obo_ws=_mock_obo_ws("alice@x"),
        )
        return svc

    def test_enabled_only_submits(self):
        svc = self._call(ApprovalMode.ENABLED, can_edit_and_approve=True)
        svc.submit.assert_called_once()
        svc.approve.assert_not_called()

    def test_disabled_auto_approves_with_marker(self):
        svc = self._call(ApprovalMode.DISABLED, can_edit_and_approve=False)
        svc.approve.assert_called_once()
        assert svc.approve.call_args.args[1].endswith(AUTO_APPROVER_SUFFIX)

    def test_auto_bypass_approves_when_predicate_true(self):
        svc = self._call(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=True)
        svc.approve.assert_called_once()

    def test_auto_bypass_pending_when_predicate_false(self):
        svc = self._call(ApprovalMode.AUTO_BYPASS, can_edit_and_approve=False)
        svc.approve.assert_not_called()
