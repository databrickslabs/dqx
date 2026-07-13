"""Tests for the require-draft-run-before-submit gate (issue B2-12).

Covers, in order:

* the pure :class:`DraftRunGateService` (has-run query + ``enforce`` behaviour,
  including the synthetic / registry carve-out);
* ``AppSettingsService`` get/save of the setting (default off, round trip);
* the ``/config/require-draft-run`` route (GET default + PUT);
* the submit routes (per-table rule / monitored table / table space): blocked
  with 409 when the setting is on and no draft run exists; allowed once a draft
  run exists; never blocked when the setting is off; and — the key interaction —
  a submit that WOULD auto-approve (approvals ``disabled`` / ``auto_bypass``) is
  blocked BEFORE any state transition, so no auto-approval leaks through.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.approvals import ApprovalMode
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.registry_models import DataProduct, MonitoredTable
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductDetail
from databricks_labs_dqx_app.backend.services.draft_run_gate_service import (
    DRAFT_RUN_REQUIRED_MESSAGE,
    DRAFT_RUN_STALE_MESSAGE,
    DraftRunGateService,
    DraftRunRequiredError,
)
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RuleCatalogEntry


def _fresh_gate(*, has_run: bool, has_fresh_run: bool) -> DraftRunGateService:
    """Gate whose validation-runs query distinguishes the 'since' (fresh) filter.

    A query carrying the ``created_at >=`` clause reports a run only when
    *has_fresh_run*; an existence-only query (no ``since``) reports one when
    *has_run*. Lets the fresh-run comparison (B2-118) be asserted without a live
    warehouse.
    """
    sql = MagicMock()
    sql.fqn.side_effect = lambda t: t

    def _query(text: str):
        if "created_at >=" in text:
            return [(1,)] if has_fresh_run else []
        return [(1,)] if has_run else []

    sql.query.side_effect = _query
    return DraftRunGateService(validation_sql=sql)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gate(*, has_run: bool) -> DraftRunGateService:
    """A DraftRunGateService whose validation-runs query reports a run or not."""
    sql = MagicMock()
    sql.fqn.side_effect = lambda t: t
    sql.query.return_value = [(1,)] if has_run else []
    return DraftRunGateService(validation_sql=sql)


def _mock_obo_ws(user_email: str) -> MagicMock:
    obo = MagicMock()
    me = MagicMock()
    me.user_name = user_email
    obo.current_user.me.return_value = me
    return obo


def _app_settings(*, require_draft_run: bool, mode: str = ApprovalMode.ENABLED) -> MagicMock:
    svc = MagicMock()
    svc.get_require_draft_run_before_submit.return_value = require_draft_run
    svc.get_approvals_mode.return_value = mode
    return svc


# ---------------------------------------------------------------------------
# DraftRunGateService — pure logic
# ---------------------------------------------------------------------------


class TestDraftRunGateService:
    def test_has_any_run_true_when_query_returns_rows(self):
        assert _gate(has_run=True).has_any_run(["c.s.t"]) is True

    def test_has_any_run_false_when_query_empty(self):
        assert _gate(has_run=False).has_any_run(["c.s.t"]) is False

    def test_has_any_run_skips_query_for_synthetic_only(self):
        gate = _gate(has_run=True)
        assert gate.has_any_run(["__sql_check__/foo"]) is False
        gate._sql.query.assert_not_called()

    def test_enforce_noop_when_disabled(self):
        gate = _gate(has_run=False)
        gate.enforce(enabled=False, table_fqns=["c.s.t"])  # must not raise
        gate._sql.query.assert_not_called()

    def test_enforce_noop_when_only_synthetic_tables(self):
        gate = _gate(has_run=False)
        gate.enforce(enabled=True, table_fqns=["__sql_check__/foo", ""])  # table-agnostic → allow
        gate._sql.query.assert_not_called()

    def test_enforce_noop_when_no_tables(self):
        gate = _gate(has_run=False)
        gate.enforce(enabled=True, table_fqns=[])  # empty space → vacuously allowed

    def test_enforce_raises_when_enabled_and_no_run(self):
        with pytest.raises(DraftRunRequiredError) as exc:
            _gate(has_run=False).enforce(enabled=True, table_fqns=["c.s.t"])
        assert str(exc.value) == DRAFT_RUN_REQUIRED_MESSAGE

    def test_enforce_passes_when_run_exists(self):
        _gate(has_run=True).enforce(enabled=True, table_fqns=["c.s.t"])  # must not raise

    def test_enforce_passes_when_one_of_many_members_has_run(self):
        # any_run semantics: a single member with a recorded run satisfies the gate.
        _gate(has_run=True).enforce(enabled=True, table_fqns=["c.s.a", "c.s.b", "c.s.c"])

    # --- B2-118: fresh-run-since-last-change comparison ---

    def test_has_any_run_adds_since_filter_to_query(self):
        gate = _gate(has_run=True)
        gate.has_any_run(["c.s.t"], since=datetime(2026, 7, 13, tzinfo=timezone.utc))
        assert "created_at >=" in gate._sql.query.call_args[0][0]

    def test_has_any_run_no_since_filter_when_none(self):
        gate = _gate(has_run=True)
        gate.has_any_run(["c.s.t"])
        assert "created_at >=" not in gate._sql.query.call_args[0][0]

    def test_enforce_passes_when_run_is_fresh(self):
        # A run exists AND is newer than the last change → allowed.
        _fresh_gate(has_run=True, has_fresh_run=True).enforce(
            enabled=True, table_fqns=["c.s.t"], last_change_time=datetime(2026, 7, 13, tzinfo=timezone.utc)
        )

    def test_enforce_raises_stale_when_run_predates_last_change(self):
        # A run exists but predates the last change → stale, distinct message.
        with pytest.raises(DraftRunRequiredError) as exc:
            _fresh_gate(has_run=True, has_fresh_run=False).enforce(
                enabled=True, table_fqns=["c.s.t"], last_change_time=datetime(2026, 7, 13, tzinfo=timezone.utc)
            )
        assert str(exc.value) == DRAFT_RUN_STALE_MESSAGE

    def test_enforce_raises_required_when_no_run_at_all(self):
        # No run exists (fresh or otherwise) → the "never tested" message, even
        # with a last_change_time supplied.
        with pytest.raises(DraftRunRequiredError) as exc:
            _fresh_gate(has_run=False, has_fresh_run=False).enforce(
                enabled=True, table_fqns=["c.s.t"], last_change_time=datetime(2026, 7, 13, tzinfo=timezone.utc)
            )
        assert str(exc.value) == DRAFT_RUN_REQUIRED_MESSAGE

    def test_ts_literal_normalises_tz_aware_to_naive_utc(self):
        # A +02:00 instant becomes its UTC wall-clock inside the CAST literal.
        aware = datetime(2026, 7, 13, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
        literal = DraftRunGateService._ts_literal(aware)
        assert literal == "CAST('2026-07-13 10:00:00' AS TIMESTAMP)"


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


class TestAppSettingsRequireDraftRun:
    def test_defaults_to_false(self, settings_svc):
        assert settings_svc.get_require_draft_run_before_submit() is False

    @pytest.mark.parametrize("enabled", [True, False])
    def test_round_trip(self, settings_svc, sql_executor_mock, enabled):
        _wire_stateful_store(sql_executor_mock)
        assert settings_svc.save_require_draft_run_before_submit(enabled, user_email="admin@x") is enabled
        assert settings_svc.get_require_draft_run_before_submit() is enabled

    def test_corrupt_value_reads_as_false(self, settings_svc, sql_executor_mock):
        store = _wire_stateful_store(sql_executor_mock)
        store["require_draft_run_before_submit"] = "banana"
        assert settings_svc.get_require_draft_run_before_submit() is False


# ---------------------------------------------------------------------------
# /config/require-draft-run route
# ---------------------------------------------------------------------------


class TestRequireDraftRunRoute:
    def test_get_returns_default_false(self, settings_svc):
        from databricks_labs_dqx_app.backend.routes.v1.config import get_require_draft_run_settings

        assert get_require_draft_run_settings(settings_svc).require_draft_run_before_submit is False

    def test_put_saves_value(self, settings_svc, sql_executor_mock):
        from databricks_labs_dqx_app.backend.routes.v1.config import (
            RequireDraftRunSettingsIn,
            get_require_draft_run_settings,
            save_require_draft_run_settings,
        )

        _wire_stateful_store(sql_executor_mock)
        out = save_require_draft_run_settings(
            RequireDraftRunSettingsIn(require_draft_run_before_submit=True), settings_svc, "admin@x"
        )
        assert out.require_draft_run_before_submit is True
        assert get_require_draft_run_settings(settings_svc).require_draft_run_before_submit is True


# ---------------------------------------------------------------------------
# rules.py submit_for_approval — full matrix
# ---------------------------------------------------------------------------


def _rules_svc(*, table_fqn: str = "c.s.t") -> MagicMock:
    svc = MagicMock()
    svc.get_by_rule_id.return_value = RuleCatalogEntry(
        table_fqn=table_fqn, checks=[], created_by="alice@x", rule_id="r1"
    )
    svc.set_status.return_value = RuleCatalogEntry(
        table_fqn=table_fqn, checks=[], version=1, status="pending_approval", source="ui",
        created_by="alice@x", rule_id="r1",
    )
    return svc


def _call_rules_submit(*, svc, require_draft_run, has_run, mode=ApprovalMode.ENABLED):
    from databricks_labs_dqx_app.backend.routes.v1.rules import submit_for_approval

    return submit_for_approval(
        rule_id="r1",
        svc=svc,
        version_svc=MagicMock(),
        app_settings=_app_settings(require_draft_run=require_draft_run, mode=mode),
        draft_run_gate=_gate(has_run=has_run),
        obo_ws=_mock_obo_ws("alice@x"),
        user_role=UserRole.ADMIN,
        body=None,
    )


class TestRulesSubmitGate:
    def test_blocked_with_409_when_enabled_and_no_run(self):
        svc = _rules_svc()
        with pytest.raises(HTTPException) as exc:
            _call_rules_submit(svc=svc, require_draft_run=True, has_run=False)
        assert exc.value.status_code == 409
        assert exc.value.detail == DRAFT_RUN_REQUIRED_MESSAGE
        # No state transition happened — the gate fired before set_status.
        svc.set_status.assert_not_called()

    def test_allowed_when_enabled_and_run_exists(self):
        svc = _rules_svc()
        _call_rules_submit(svc=svc, require_draft_run=True, has_run=True)
        statuses = [c.args[1] for c in svc.set_status.call_args_list]
        assert statuses == ["pending_approval"]

    def test_not_blocked_when_setting_off(self):
        svc = _rules_svc()
        _call_rules_submit(svc=svc, require_draft_run=False, has_run=False)
        svc.set_status.assert_called_once()

    def test_synthetic_rule_never_blocked(self):
        svc = _rules_svc(table_fqn="__sql_check__/cross")
        _call_rules_submit(svc=svc, require_draft_run=True, has_run=False)
        svc.set_status.assert_called_once()

    @pytest.mark.parametrize("mode", [ApprovalMode.ENABLED, ApprovalMode.AUTO_BYPASS, ApprovalMode.DISABLED])
    def test_auto_approving_modes_blocked_before_approval(self, mode):
        # Even in modes that would auto-approve an admin's submit, the gate
        # fires first — nothing (not even pending_approval) is written.
        svc = _rules_svc()
        with pytest.raises(HTTPException) as exc:
            _call_rules_submit(svc=svc, require_draft_run=True, has_run=False, mode=mode)
        assert exc.value.status_code == 409
        svc.set_status.assert_not_called()


# ---------------------------------------------------------------------------
# monitored_tables.py submit_monitored_table
# ---------------------------------------------------------------------------


def _statuses_sequence():
    return iter(
        [
            [],  # recover rejected: nothing
            [("r1", "draft")],  # draft -> pending_approval
            [("r1", "pending_approval")],  # rollup after submit
            [("r1", "pending_approval")],  # spare
            [("r1", "approved")],  # spare
        ]
    )


def _call_mt_submit(*, require_draft_run, has_run, mode=ApprovalMode.ENABLED):
    from databricks_labs_dqx_app.backend.routes.v1 import monitored_tables as mt

    mt_svc = MagicMock()
    mt_svc.get.return_value = SimpleNamespace(table=SimpleNamespace(table_fqn="cat.schema.tbl", updated_at=None))
    mt_svc.list_materialized_rule_statuses.side_effect = _statuses_sequence()
    mt_svc.set_status.return_value = MonitoredTable(
        binding_id="b1", table_fqn="cat.schema.tbl", status="pending_approval"
    )
    materializer = MagicMock()
    mt.submit_monitored_table(
        binding_id="b1",
        monitored_tables_svc=mt_svc,
        materializer=materializer,
        rules_catalog=MagicMock(),
        version_svc=MagicMock(),
        app_settings=_app_settings(require_draft_run=require_draft_run, mode=mode),
        draft_run_gate=_gate(has_run=has_run),
        perms=MagicMock(),
        role=UserRole.RULE_APPROVER,
        principal_ids=frozenset({"me"}),
        obo_ws=_mock_obo_ws("alice@x"),
    )
    return materializer


class TestMonitoredTableSubmitGate:
    def test_blocked_with_409_when_enabled_and_no_run(self):
        from databricks_labs_dqx_app.backend.routes.v1 import monitored_tables as mt

        mt_svc = MagicMock()
        mt_svc.get.return_value = SimpleNamespace(table=SimpleNamespace(table_fqn="cat.schema.tbl", updated_at=None))
        materializer = MagicMock()
        with pytest.raises(HTTPException) as exc:
            mt.submit_monitored_table(
                binding_id="b1",
                monitored_tables_svc=mt_svc,
                materializer=materializer,
                rules_catalog=MagicMock(),
                version_svc=MagicMock(),
                app_settings=_app_settings(require_draft_run=True, mode=ApprovalMode.DISABLED),
                draft_run_gate=_gate(has_run=False),
                perms=MagicMock(),
                role=UserRole.RULE_APPROVER,
                principal_ids=frozenset({"me"}),
                obo_ws=_mock_obo_ws("alice@x"),
            )
        assert exc.value.status_code == 409
        # Gate fired before any materialization / state change.
        materializer.materialize_binding.assert_not_called()

    def test_allowed_when_run_exists(self):
        materializer = _call_mt_submit(require_draft_run=True, has_run=True)
        materializer.materialize_binding.assert_called_once()

    def test_not_blocked_when_setting_off(self):
        materializer = _call_mt_submit(require_draft_run=False, has_run=False)
        materializer.materialize_binding.assert_called_once()

    def test_blocked_409_stale_when_run_predates_last_edit(self):
        # B2-118: a run exists but predates the binding's updated_at (last edit)
        # → 409 with the stale message, before any materialization.
        from databricks_labs_dqx_app.backend.routes.v1 import monitored_tables as mt

        mt_svc = MagicMock()
        mt_svc.get.return_value = SimpleNamespace(
            table=SimpleNamespace(table_fqn="cat.schema.tbl", updated_at=datetime(2026, 7, 13, tzinfo=timezone.utc))
        )
        materializer = MagicMock()
        with pytest.raises(HTTPException) as exc:
            mt.submit_monitored_table(
                binding_id="b1",
                monitored_tables_svc=mt_svc,
                materializer=materializer,
                rules_catalog=MagicMock(),
                version_svc=MagicMock(),
                app_settings=_app_settings(require_draft_run=True),
                draft_run_gate=_fresh_gate(has_run=True, has_fresh_run=False),
                perms=MagicMock(),
                role=UserRole.RULE_APPROVER,
                principal_ids=frozenset({"me"}),
                obo_ws=_mock_obo_ws("alice@x"),
            )
        assert exc.value.status_code == 409
        assert exc.value.detail == DRAFT_RUN_STALE_MESSAGE
        materializer.materialize_binding.assert_not_called()


# ---------------------------------------------------------------------------
# data_products.py submit_data_product
# ---------------------------------------------------------------------------


class TestDataProductSubmitGate:
    def test_blocked_with_409_when_enabled_and_no_run(self):
        from databricks_labs_dqx_app.backend.routes.v1.data_products import submit_data_product

        svc = MagicMock()
        svc.get.return_value = SimpleNamespace(
            members=[SimpleNamespace(table_fqn="c.s.t")], product=SimpleNamespace(updated_at=None)
        )
        with pytest.raises(HTTPException) as exc:
            submit_data_product(
                product_id="p1",
                svc=svc,
                app_settings=_app_settings(require_draft_run=True, mode=ApprovalMode.AUTO_BYPASS),
                draft_run_gate=_gate(has_run=False),
                perms=MagicMock(),
                role=UserRole.RULE_APPROVER,
                principal_ids=frozenset({"me"}),
                obo_ws=_mock_obo_ws("alice@x"),
            )
        assert exc.value.status_code == 409
        svc.submit.assert_not_called()

    def test_allowed_when_run_exists(self):
        from databricks_labs_dqx_app.backend.routes.v1.data_products import submit_data_product

        svc = MagicMock()
        detail = DataProductDetail(
            product=DataProduct(product_id="p1", name="Orders", status="approved", version=1),
            members=[],
        )
        # First get() (gate) returns a members-bearing namespace; the final
        # get() (response build) returns a real detail for from_domain.
        svc.get.side_effect = [
            SimpleNamespace(members=[SimpleNamespace(table_fqn="c.s.t")], product=SimpleNamespace(updated_at=None)),
            detail,
        ]
        submit_data_product(
            product_id="p1",
            svc=svc,
            app_settings=_app_settings(require_draft_run=True),
            draft_run_gate=_gate(has_run=True),
            perms=MagicMock(),
            role=UserRole.RULE_APPROVER,
            principal_ids=frozenset({"me"}),
            obo_ws=_mock_obo_ws("alice@x"),
        )
        svc.submit.assert_called_once()

    def test_empty_space_not_blocked(self):
        from databricks_labs_dqx_app.backend.routes.v1.data_products import submit_data_product

        svc = MagicMock()
        detail = DataProductDetail(
            product=DataProduct(product_id="p1", name="Orders", status="draft", version=0),
            members=[],
        )
        svc.get.side_effect = [
            SimpleNamespace(members=[], product=SimpleNamespace(updated_at=None)),
            detail,
        ]
        submit_data_product(
            product_id="p1",
            svc=svc,
            app_settings=_app_settings(require_draft_run=True),
            draft_run_gate=_gate(has_run=False),
            perms=MagicMock(),
            role=UserRole.RULE_APPROVER,
            principal_ids=frozenset({"me"}),
            obo_ws=_mock_obo_ws("alice@x"),
        )
        svc.submit.assert_called_once()
