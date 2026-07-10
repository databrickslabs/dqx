"""Tests for :class:`DataProductService` — the Data Products Task 4 service.

Drives the service against a spec-bound ``SqlExecutor`` mock (per-test
``query`` dispatchers, assertions on ``execute`` call SQL) plus mocked
``MonitoredTableService`` / ``RunSetService`` / ``BindingRunService`` — no
Spark or workspace needed. Covers CRUD + name uniqueness, the
published->draft/modified display-status transitions, member upsert-by-
binding_id, publish version bump, and the run fan-out resolution matrix
(draft always runs, approved skips never-approved unless pinned, 409 on
zero-runnable, per-member submission failures collected into ``skipped``).
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import DataProduct, MonitoredTable
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingRunResult,
    BindingRunService,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    BindingNotApprovedError,
    DataProductService,
    DuplicateDataProductNameError,
    InvalidStatusTransitionError,
    NoRunnableMembersError,
    display_status,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService, RunSetSummary
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_PRODUCTS = "dqx_test.dqx_app_test.dq_data_products"
_MEMBERS = "dqx_test.dqx_app_test.dq_data_product_members"


def _mock_sql() -> SqlExecutor:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.dialect = "delta"
    mock.fqn.side_effect = lambda t: {"dq_data_products": _PRODUCTS, "dq_data_product_members": _MEMBERS}.get(
        t, f"dqx_test.dqx_app_test.{t}"
    )
    mock.q.side_effect = lambda i: f"`{i}`"
    mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    mock.query.return_value = []
    return mock


def _product_row(
    product_id: str = "p1",
    name: str = "Orders",
    description: str | None = None,
    steward: str | None = "alice@x",
    schedule_cron: str | None = None,
    schedule_tz: str | None = None,
    status: str = "draft",
    version: str = "0",
    score: str | None = None,
    failed_tests: str | None = None,
    total_tests: str | None = None,
    score_computed_at: str | None = None,
) -> list[str | None]:
    # The trailing 4 cells are the dq_score_cache LEFT-JOIN columns the
    # list/get read paths select (P3.4) — all None when the product has
    # never been scored. The other read paths select only the first 12
    # columns; the extra cells are ignored by _row_to_product.
    return [
        product_id,
        name,
        description,
        steward,
        schedule_cron,
        schedule_tz,
        status,
        version,
        "alice@x",
        "2026-07-07T00:00:00",
        "alice@x",
        "2026-07-07T00:00:00",
        score,
        failed_tests,
        total_tests,
        score_computed_at,
    ]


def _member_row(member_id: str, binding_id: str, pinned_version: str | None = None) -> list[str | None]:
    return [member_id, binding_id, pinned_version]


def _table_summary(
    binding_id: str = "b1",
    table_fqn: str = "cat.schema.tbl",
    status: str = "approved",
    version: int = 2,
    applied_rule_count: int = 3,
    check_count: int = 5,
) -> MonitoredTableSummary:
    return MonitoredTableSummary(
        table=MonitoredTable(binding_id=binding_id, table_fqn=table_fqn, status=status, version=version),
        applied_rule_count=applied_rule_count,
        check_count=check_count,
    )


def _monitored_table_detail(
    binding_id: str = "b1", version: int = 2, status: str = "approved"
) -> MonitoredTableDetail:
    return MonitoredTableDetail(
        table=MonitoredTable(binding_id=binding_id, table_fqn="cat.schema.tbl", status=status, version=version)
    )


@pytest.fixture
def sql():
    return _mock_sql()


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def run_set_service():
    mock = create_autospec(RunSetService, instance=True)
    mock.list_for_product.return_value = []
    mock.create.return_value = "rs-new"
    return mock


@pytest.fixture
def binding_run_service():
    return create_autospec(BindingRunService, instance=True)


@pytest.fixture
def version_service():
    mock = create_autospec(MonitoredTableVersionService, instance=True)
    # Default: no frozen snapshot resolves, so members fall back to live counts.
    mock.snapshot_counts.return_value = None
    return mock


@pytest.fixture
def app_settings():
    mock = create_autospec(AppSettingsService, instance=True)
    # Default matches production default: new members follow latest unless
    # the caller explicitly pins.
    mock.get_default_auto_upgrade.return_value = True
    mock.resolve_pinned_version_for_new_attachment.side_effect = (
        lambda explicit, current: explicit if explicit is not None else (None if mock.get_default_auto_upgrade() else current)
    )
    return mock


@pytest.fixture
def service(sql, monitored_tables, run_set_service, binding_run_service, version_service, app_settings):
    return DataProductService(
        sql=sql,
        monitored_tables=monitored_tables,
        run_set_service=run_set_service,
        binding_run_service=binding_run_service,
        version_service=version_service,
        app_settings=app_settings,
    )


class TestDisplayStatus:
    def test_approved(self):
        p = DataProduct(product_id="p1", name="x", status="approved", version=1)
        assert display_status(p) == "approved"

    def test_pending_approval(self):
        p = DataProduct(product_id="p1", name="x", status="pending_approval", version=0)
        assert display_status(p) == "pending_approval"

    def test_rejected(self):
        p = DataProduct(product_id="p1", name="x", status="rejected", version=0)
        assert display_status(p) == "rejected"

    def test_modified_since_approval(self):
        p = DataProduct(product_id="p1", name="x", status="draft", version=1)
        assert display_status(p) == "modified"

    def test_never_approved_is_draft(self):
        p = DataProduct(product_id="p1", name="x", status="draft", version=0)
        assert display_status(p) == "draft"


class TestListAndGet:
    def test_list_products_resolves_members_and_counters(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="draft", version="0")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2)
        ]
        run_set_service.list_for_product.return_value = []

        result = service.list_products()
        assert len(result) == 1
        detail = result[0]
        assert detail.product.product_id == "p1"
        assert detail.member_count == 1
        assert detail.runnable_count == 1
        assert detail.members[0].binding_status == "approved"
        assert detail.members[0].runnable is True

    def test_list_products_empty(self, service, sql):
        sql.query.return_value = []
        assert service.list_products() == []

    def test_list_products_left_joins_score_cache_in_same_round_trip(self, service, sql, monitored_tables):
        """P3.4: the cached score columns ride along the products query —
        no extra round trip and NEVER a warehouse recompute on page load."""
        sql.query.side_effect = [
            [_product_row(product_id="p1", score="0.9876", failed_tests="12", total_tests="1000",
                          score_computed_at="2026-07-10T00:00:00")],
            [],  # members
        ]
        monitored_tables.list_monitored_tables.return_value = []
        result = service.list_products()
        products_query = sql.query.call_args_list[0][0][0]
        assert "LEFT JOIN dqx_test.dqx_app_test.dq_score_cache" in products_query
        assert "sc.scope_type = 'product'" in products_query
        assert "sc.scope_key = p.product_id" in products_query
        detail = result[0]
        assert detail.score == 0.9876
        assert detail.failed_tests == 12
        assert detail.total_tests == 1000
        assert detail.score_computed_at == "2026-07-10T00:00:00"

    def test_list_products_score_fields_none_when_never_scored(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # members
        ]
        monitored_tables.list_monitored_tables.return_value = []
        detail = service.list_products()[0]
        assert detail.score is None
        assert detail.failed_tests is None
        assert detail.total_tests is None
        assert detail.score_computed_at is None

    def test_get_carries_cached_score(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1", score="0.5", failed_tests="1", total_tests="2",
                          score_computed_at="2026-07-10T00:00:00")],
            [],  # members
        ]
        monitored_tables.list_monitored_tables.return_value = []
        detail = service.get("p1")
        assert detail is not None
        assert detail.score == 0.5
        assert detail.failed_tests == 1
        assert detail.total_tests == 2

    def test_member_referencing_missing_binding_is_skipped(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b-deleted")],
        ]
        monitored_tables.list_monitored_tables.return_value = []
        result = service.list_products()
        assert result[0].member_count == 0

    def test_get_missing_returns_none(self, service, sql):
        sql.query.return_value = []
        assert service.get("missing") is None

    def test_get_not_runnable_when_status_not_approved(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="pending_approval", version=1)
        ]
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].runnable is False

    def test_unpinned_member_reports_live_counts(self, service, sql, monitored_tables, version_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version=None)],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=3, check_count=5)
        ]
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].rules_count == 3
        assert detail.members[0].checks_count == 5
        # No pin -> never consults the frozen snapshot.
        version_service.snapshot_counts.assert_not_called()

    def test_pinned_member_reports_pinned_snapshot_counts(self, service, sql, monitored_tables, version_service):
        # Live binding has moved on to v2 and its live counts differ from the
        # pinned v1 snapshot (regression: pinned member used to show the live
        # count — 0 here — instead of the frozen snapshot's real 2 checks).
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=0, check_count=0)
        ]
        version_service.snapshot_counts.return_value = (1, 2)
        detail = service.get("p1")
        assert detail is not None
        version_service.snapshot_counts.assert_called_once_with("b1", 1)
        assert detail.members[0].rules_count == 1
        assert detail.members[0].checks_count == 2
        # The pin itself is still surfaced unchanged.
        assert detail.members[0].pinned_version == 1

    def test_pinned_member_falls_back_to_live_counts_when_snapshot_missing(
        self, service, sql, monitored_tables, version_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=3, check_count=5)
        ]
        version_service.snapshot_counts.return_value = None  # snapshot not found
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].rules_count == 3
        assert detail.members[0].checks_count == 5


class TestCreate:
    def test_create_success(self, service, sql):
        sql.query.return_value = []  # name available
        product = service.create("Orders", "desc", None, "alice@x")
        assert product.name == "Orders"
        assert product.status == "draft"
        assert product.version == 0
        assert product.steward == "alice@x"  # defaults to creator
        insert_sql = sql.execute.call_args[0][0]
        assert f"INSERT INTO {_PRODUCTS}" in insert_sql
        assert "'Orders'" in insert_sql

    def test_create_duplicate_name_raises(self, service, sql):
        sql.query.return_value = [["p-existing"]]
        with pytest.raises(DuplicateDataProductNameError):
            service.create("Orders", None, None, "alice@x")

    def test_create_empty_name_raises(self, service):
        with pytest.raises(ValueError):
            service.create("   ", None, None, "alice@x")


class TestUpdate:
    def test_update_flips_published_to_draft_without_bumping_version(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="published", version="3")],
        ]
        updated = service.update("p1", {"description": "new desc"}, "bob@x")
        assert updated.status == "draft"
        assert updated.version == 3  # unchanged
        assert updated.description == "new desc"
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'draft'" in update_sql
        assert "version =" not in update_sql  # PATCH never bumps version

    def test_update_missing_raises_lookup_error(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.update("missing", {"description": "x"}, "bob@x")

    def test_update_rename_to_existing_name_raises_409(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", name="Orders")],
            [["p-other"]],  # name check finds a different product
        ]
        with pytest.raises(DuplicateDataProductNameError):
            service.update("p1", {"name": "Taken"}, "bob@x")

    def test_update_rename_to_same_name_is_a_noop_check(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", name="Orders")],
        ]
        updated = service.update("p1", {"name": "Orders"}, "bob@x")
        assert updated.name == "Orders"

    def test_update_empty_name_raises_value_error(self, service, sql):
        sql.query.side_effect = [[_product_row(product_id="p1")]]
        with pytest.raises(ValueError):
            service.update("p1", {"name": ""}, "bob@x")

    def test_update_clears_schedule_when_explicitly_null(self, service, sql):
        sql.query.side_effect = [[_product_row(product_id="p1", schedule_cron="0 0 * * *")]]
        service.update("p1", {"schedule_cron": None}, "bob@x")
        update_sql = sql.execute.call_args[0][0]
        assert "schedule_cron = NULL" in update_sql


class TestDelete:
    def test_delete_success(self, service, sql):
        sql.query.return_value = [["p1"]]
        service.delete("p1")
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"DELETE FROM {_MEMBERS}" in c for c in calls)
        assert any(f"DELETE FROM {_PRODUCTS}" in c for c in calls)

    def test_delete_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.delete("missing")


class TestMembers:
    def test_add_member_inserts_new_and_flips_to_draft(self, service, sql, monitored_tables):
        monitored_tables.get.return_value = _monitored_table_detail()
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="published")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", 2, "bob@x")
        assert member.binding_id == "b1"
        assert member.pinned_version == 2
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"INSERT INTO {_MEMBERS}" in c for c in calls)
        assert any("status = 'draft'" in c for c in calls)

    def test_add_member_updates_pin_in_place_for_existing_binding(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [["m-existing"]],  # already a member
        ]
        member = service.add_member("p1", "b1", 5, "bob@x")
        assert member.id == "m-existing"
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_MEMBERS}" in c and "pinned_version = 5" in c for c in calls)
        assert not any(f"INSERT INTO {_MEMBERS}" in c for c in calls)

    def test_add_member_missing_product_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.add_member("missing", "b1", None, "bob@x")

    def test_add_member_unspecified_pin_follows_latest_when_auto_upgrade_on(
        self, service, sql, monitored_tables, app_settings
    ):
        # default_auto_upgrade ON (default): a brand-new member with no
        # explicit pin stays unpinned (follow latest) — unchanged behaviour.
        app_settings.get_default_auto_upgrade.return_value = True
        monitored_tables.get.return_value = _monitored_table_detail(version=3)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", None, "bob@x")
        assert member.pinned_version is None
        insert_sql = next(c[0][0] for c in sql.execute.call_args_list if f"INSERT INTO {_MEMBERS}" in c[0][0])
        assert "NULL" in insert_sql

    def test_add_member_unspecified_pin_freezes_current_version_when_auto_upgrade_off(
        self, service, sql, monitored_tables, app_settings
    ):
        # default_auto_upgrade OFF: a brand-new member with no explicit pin
        # freezes to the binding's current published version at attach time.
        app_settings.get_default_auto_upgrade.return_value = False
        monitored_tables.get.return_value = _monitored_table_detail(version=3)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", None, "bob@x")
        assert member.pinned_version == 3
        insert_sql = next(c[0][0] for c in sql.execute.call_args_list if f"INSERT INTO {_MEMBERS}" in c[0][0])
        assert ", 3)" in insert_sql

    def test_add_member_existing_binding_explicit_none_unpins_regardless_of_setting(
        self, service, sql, monitored_tables, app_settings
    ):
        # Attach-time-only: default_auto_upgrade must NOT be consulted when
        # updating an EXISTING member — an explicit None here means the
        # steward chose to unpin, not "unspecified".
        app_settings.get_default_auto_upgrade.return_value = False
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [["m-existing"]],  # already a member
        ]
        member = service.add_member("p1", "b1", None, "bob@x")
        assert member.pinned_version is None
        monitored_tables.get.assert_not_called()
        app_settings.resolve_pinned_version_for_new_attachment.assert_not_called()
        update_sql = next(c[0][0] for c in sql.execute.call_args_list if f"UPDATE {_MEMBERS}" in c[0][0])
        assert "pinned_version = NULL" in update_sql

    def test_add_member_invalid_binding_id_raises(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row — triggers the binding validation
        ]
        # Simulate binding_id not found in monitored_tables
        monitored_tables.get.return_value = None
        with pytest.raises(RuntimeError, match="Monitored table not found"):
            service.add_member("p1", "invalid_binding", None, "bob@x")

    def test_add_member_valid_binding_id_succeeds(self, service, sql, monitored_tables, app_settings):
        # Ensure binding_id exists in monitored_tables
        monitored_tables.get.return_value = _monitored_table_detail(binding_id="b1", version=2)
        app_settings.get_default_auto_upgrade.return_value = True
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="draft")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", 2, "bob@x")
        assert member.binding_id == "b1"
        assert member.pinned_version == 2

    @pytest.mark.parametrize(
        ("status", "version"),
        [
            ("draft", 0),  # brand-new, never approved
            ("pending_approval", 0),  # submitted, not yet approved
            ("rejected", 0),  # rejected without any prior approval
            ("pending_approval", 2),  # previously approved, resubmitted — status alone blocks
            ("rejected", 2),  # previously approved, later rejected — status alone blocks
            ("approved", 0),  # rollup edge: status flag approved but never binding-approved
        ],
    )
    def test_add_member_non_approved_binding_raises_400(self, service, sql, monitored_tables, status, version):
        """P3.2: only bindings passing _is_runnable (approved AND version > 0) can JOIN a space."""
        monitored_tables.get.return_value = _monitored_table_detail(status=status, version=version)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row — new-member branch
        ]
        with pytest.raises(BindingNotApprovedError, match="cat.schema.tbl"):
            service.add_member("p1", "b1", None, "bob@x")
        # Nothing was inserted and the space's status was left untouched.
        sql.execute.assert_not_called()

    def test_add_member_never_approved_message_names_table_and_reason(self, service, sql, monitored_tables):
        monitored_tables.get.return_value = _monitored_table_detail(status="draft", version=0)
        sql.query.side_effect = [[_product_row(product_id="p1")], []]
        with pytest.raises(BindingNotApprovedError, match="never been approved"):
            service.add_member("p1", "b1", None, "bob@x")

    def test_add_member_non_approved_status_message_names_status(self, service, sql, monitored_tables):
        monitored_tables.get.return_value = _monitored_table_detail(status="pending_approval", version=2)
        sql.query.side_effect = [[_product_row(product_id="p1")], []]
        with pytest.raises(BindingNotApprovedError, match="'pending_approval'"):
            service.add_member("p1", "b1", None, "bob@x")

    def test_add_member_modified_underneath_binding_stays_eligible(self, service, sql, monitored_tables):
        """A binding shown as "modified" in the UI (approved rules with
        unapproved edits) still has persisted status 'approved' and version > 0
        — it has a frozen approved snapshot, so it remains eligible (P3.2)."""
        monitored_tables.get.return_value = _monitored_table_detail(status="approved", version=3)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", 3, "bob@x")
        assert member.binding_id == "b1"
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"INSERT INTO {_MEMBERS}" in c for c in calls)

    def test_add_member_pin_change_on_existing_member_skips_approval_check(self, service, sql, monitored_tables):
        """No retroactive eviction (P3.2): a binding that left 'approved' after
        joining can still have its pin changed — the UPDATE branch never
        consults the binding's status (run() already skips it under
        source='approved')."""
        monitored_tables.get.return_value = _monitored_table_detail(status="draft", version=0)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [["m-existing"]],  # already a member
        ]
        member = service.add_member("p1", "b1", 4, "bob@x")
        assert member.id == "m-existing"
        monitored_tables.get.assert_not_called()
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_MEMBERS}" in c and "pinned_version = 4" in c for c in calls)

    def test_remove_member_success_flips_to_draft(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="published")],
            [["m1"]],
        ]
        service.remove_member("p1", "m1", "bob@x")
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"DELETE FROM {_MEMBERS}" in c for c in calls)
        assert any("status = 'draft'" in c for c in calls)

    def test_remove_member_missing_raises(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],
        ]
        with pytest.raises(LookupError):
            service.remove_member("p1", "missing", "bob@x")


class TestSubmit:
    def test_submit_draft_to_pending_without_version_bump(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="draft", version="1")]
        product = service.submit("p1", "bob@x")
        assert product.status == "pending_approval"
        assert product.version == 1  # unchanged — only approve bumps
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'pending_approval'" in update_sql
        assert "version =" not in update_sql

    def test_submit_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.submit("missing", "bob@x")

    def test_submit_rejected_to_pending(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="rejected", version="1")]
        product = service.submit("p1", "bob@x")
        assert product.status == "pending_approval"

    def test_submit_approved_unchanged_raises_409(self, service, sql):
        """Minor fix: an approved, unchanged space submitted via direct API
        must be rejected — mirrors the P20 registry-rule "no changes to
        submit" guard. Every ``update``/``add_member``/``remove_member`` call
        already flips the space to ``draft`` (even a no-op save), so a space
        still at ``approved`` has zero unpublished changes by construction.
        Without this guard, submitting it would move it to
        ``pending_approval`` — which is not itself runnable — silently
        pausing its scheduled runs.
        """
        sql.query.return_value = [_product_row(product_id="p1", status="approved", version="2")]
        with pytest.raises(InvalidStatusTransitionError):
            service.submit("p1", "bob@x")
        sql.execute.assert_not_called()


class TestApprove:
    def test_approve_bumps_version_and_sets_approved(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="pending_approval", version="1")]
        product = service.approve("p1", "bob@x")
        assert product.status == "approved"
        assert product.version == 2
        update_sql = sql.execute.call_args[0][0]
        assert "version = 2" in update_sql
        assert "status = 'approved'" in update_sql

    def test_approve_non_pending_raises_409(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="draft", version="0")]
        with pytest.raises(InvalidStatusTransitionError):
            service.approve("p1", "bob@x")

    def test_approve_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.approve("missing", "bob@x")


class TestReject:
    def test_reject_pending_to_rejected_without_version_bump(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="pending_approval", version="2")]
        product = service.reject("p1", "bob@x")
        assert product.status == "rejected"
        assert product.version == 2
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'rejected'" in update_sql
        assert "version =" not in update_sql

    def test_reject_non_pending_raises_409(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="approved", version="1")]
        with pytest.raises(InvalidStatusTransitionError):
            service.reject("p1", "bob@x")

    def test_reject_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.reject("missing", "bob@x")


class TestRun:
    def test_draft_source_runs_never_approved_members_instead_of_skipping(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="draft", version=0)
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-1", job_run_id=1, view_fqn="tmp.v1"
        )
        result = service.run("p1", "draft", "bob@x")
        assert result.run_set_id == "rs-new"
        assert len(result.submitted) == 1
        assert result.skipped == []
        binding_run_service.run_binding.assert_called_once_with(
            "b1", source="draft", version=None, user_email="bob@x", trigger="manual", run_set_id="rs-new"
        )

    def test_approved_source_skips_never_approved_unpinned_member(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1"), _member_row("m2", "b2")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="cat.s.never", status="draft", version=0),
            _table_summary(binding_id="b2", table_fqn="cat.s.ready", status="approved", version=3),
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-2", job_run_id=2, view_fqn="tmp.v2"
        )
        result = service.run("p1", "approved", "bob@x")
        assert len(result.submitted) == 1
        assert result.submitted[0].binding_id == "b2"
        assert len(result.skipped) == 1
        assert "never approved" in result.skipped[0]
        binding_run_service.run_binding.assert_called_once_with(
            "b2", source="approved", version=None, user_email="bob@x", trigger="manual", run_set_id="rs-new"
        )

    def test_pinned_member_attempted_even_when_never_approved(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="2")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="draft", version=0)
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-3", job_run_id=3, view_fqn="tmp.v3"
        )
        result = service.run("p1", "approved", "bob@x")
        assert len(result.submitted) == 1
        binding_run_service.run_binding.assert_called_once_with(
            "b1", source="approved", version=2, user_email="bob@x", trigger="manual", run_set_id="rs-new"
        )

    def test_zero_runnable_raises(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="draft", version=0)
        ]
        with pytest.raises(NoRunnableMembersError):
            service.run("p1", "approved", "bob@x")

    def test_per_member_submission_failure_collected_and_continues(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1"), _member_row("m2", "b2")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="cat.s.fails", status="approved", version=1),
            _table_summary(binding_id="b2", table_fqn="cat.s.ok", status="approved", version=1),
        ]
        binding_run_service.run_binding.side_effect = [
            NeverApprovedError("boom"),
            BindingRunResult(run_set_id="rs-new", run_id="run-4", job_run_id=4, view_fqn="tmp.v4"),
        ]
        result = service.run("p1", "approved", "bob@x")
        assert len(result.submitted) == 1
        assert result.submitted[0].binding_id == "b2"
        assert len(result.skipped) == 1
        assert "cat.s.fails" in result.skipped[0]

    def test_all_members_fail_rolls_back_empty_run_set(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=1)
        ]
        binding_run_service.run_binding.side_effect = NeverApprovedError("boom")
        result = service.run("p1", "approved", "bob@x")
        assert result.submitted == []
        run_set_service.delete_empty.assert_called_once_with("rs-new")

    def test_missing_product_raises_lookup_error(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.run("missing", "approved", "bob@x")

    def test_shared_run_set_created_with_product_id_and_version(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1", version="4")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=1)
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-5", job_run_id=5, view_fqn="tmp.v5"
        )
        service.run("p1", "approved", "bob@x")
        run_set_service.create.assert_called_once_with(
            product_id="p1", product_version=4, source="approved", trigger="manual", created_by="bob@x"
        )
