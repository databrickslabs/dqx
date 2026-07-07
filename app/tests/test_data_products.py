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
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingRunResult,
    BindingRunService,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductService,
    DuplicateDataProductNameError,
    NoRunnableMembersError,
    display_status,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService, MonitoredTableSummary
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
) -> list[str | None]:
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
def service(sql, monitored_tables, run_set_service, binding_run_service):
    return DataProductService(
        sql=sql,
        monitored_tables=monitored_tables,
        run_set_service=run_set_service,
        binding_run_service=binding_run_service,
    )


class TestDisplayStatus:
    def test_published(self):
        p = DataProduct(product_id="p1", name="x", status="published", version=1)
        assert display_status(p) == "published"

    def test_modified_since_publish(self):
        p = DataProduct(product_id="p1", name="x", status="draft", version=1)
        assert display_status(p) == "modified"

    def test_never_published_is_draft(self):
        p = DataProduct(product_id="p1", name="x", status="draft", version=0)
        assert display_status(p) == "draft"


class TestListAndGet:
    def test_list_products_resolves_members_and_counters(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="draft", version="0")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [_table_summary(binding_id="b1", status="approved", version=2)]
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
    def test_add_member_inserts_new_and_flips_to_draft(self, service, sql):
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


class TestPublish:
    def test_publish_bumps_version_and_sets_published(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="draft", version="1")]
        product = service.publish("p1", "bob@x")
        assert product.status == "published"
        assert product.version == 2
        update_sql = sql.execute.call_args[0][0]
        assert "version = 2" in update_sql
        assert "status = 'published'" in update_sql

    def test_publish_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.publish("missing", "bob@x")


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
