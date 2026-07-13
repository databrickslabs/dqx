"""Tests for :class:`BindingRunService` — the Data Products Task 3 run endpoint.

Drives ``run_binding`` against mocked collaborators (``MonitoredTableService``,
``MonitoredTableVersionService``, ``Materializer``, ``ViewService``,
``JobService``, ``RunSetService``, ``AppSettingsService``) so no Spark or
workspace is needed. Covers the design spec §4.1 resolution matrix (draft
render / pinned snapshot / latest-approved snapshot / never-approved 409 /
missing-snapshot 422), the synthetic ``__sql_check__/`` cross-table
dispatch, run-set minting vs. joining, and that the submitted config
carries EXACTLY the resolved checks.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import MonitoredTable
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingNotFoundError,
    BindingRunError,
    BindingRunService,
    MissingSnapshotError,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableDetail, MonitoredTableService
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.services.view_service import ViewService

_RUNS_TABLE = "dqx_test.dqx_app_test.dq_validation_runs"
_CHECKS = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]


def _detail(binding_id: str = "b1", table_fqn: str = "cat.schema.tbl", version: int = 2) -> MonitoredTableDetail:
    return MonitoredTableDetail(table=MonitoredTable(binding_id=binding_id, table_fqn=table_fqn, version=version))


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def version_service():
    return create_autospec(MonitoredTableVersionService, instance=True)


@pytest.fixture
def materializer():
    return create_autospec(Materializer, instance=True)


@pytest.fixture
def view_service():
    mock = create_autospec(ViewService, instance=True)
    mock.create_view.return_value = "dqx_studio_tmp.tmp_view_1"
    mock.create_view_from_sql.return_value = "dqx_studio_tmp.tmp_view_2"
    return mock


@pytest.fixture
def job_service():
    mock = create_autospec(JobService, instance=True)
    mock.submit_run.return_value = 555
    return mock


@pytest.fixture
def run_set_service():
    mock = create_autospec(RunSetService, instance=True)
    mock.create.return_value = "rs-new"
    return mock


@pytest.fixture
def settings_service():
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_custom_metrics.return_value = []
    return mock


@pytest.fixture
def service(monitored_tables, version_service, materializer, view_service, job_service, run_set_service, settings_service):
    return BindingRunService(
        monitored_tables=monitored_tables,
        version_service=version_service,
        materializer=materializer,
        view_service=view_service,
        job_service=job_service,
        run_set_service=run_set_service,
        settings_service=settings_service,
        runs_table=_RUNS_TABLE,
    )


class TestResolutionMatrix:
    def test_binding_not_found_raises(self, service, monitored_tables):
        monitored_tables.get.return_value = None
        with pytest.raises(BindingNotFoundError):
            service.run_binding("missing", source="draft", version=None, user_email="alice@x")

    def test_draft_source_renders_live_checks_with_null_binding_version(
        self, service, monitored_tables, materializer, run_set_service
    ):
        monitored_tables.get.return_value = _detail(version=0)  # never approved — draft still works
        materializer.render_binding_checks.return_value = _CHECKS

        result = service.run_binding("b1", source="draft", version=None, user_email="alice@x")

        materializer.render_binding_checks.assert_called_once_with("b1")
        run_set_service.add_member.assert_called_once_with("rs-new", result.run_id, "b1", None)

    def test_pinned_version_resolves_that_snapshot(self, service, monitored_tables, version_service, run_set_service):
        monitored_tables.get.return_value = _detail(version=5)
        version_service.get_checks.return_value = _CHECKS

        result = service.run_binding("b1", source="approved", version=3, user_email="alice@x")

        version_service.get_checks.assert_called_once_with("b1", 3)
        run_set_service.add_member.assert_called_once_with("rs-new", result.run_id, "b1", 3)

    def test_latest_approved_uses_binding_version(self, service, monitored_tables, version_service, run_set_service):
        monitored_tables.get.return_value = _detail(version=4)
        version_service.get_checks.return_value = _CHECKS

        result = service.run_binding("b1", source="approved", version=None, user_email="alice@x")

        version_service.get_checks.assert_called_once_with("b1", 4)
        run_set_service.add_member.assert_called_once_with("rs-new", result.run_id, "b1", 4)

    def test_never_approved_latest_raises_409_error(self, service, monitored_tables):
        monitored_tables.get.return_value = _detail(version=0)
        with pytest.raises(NeverApprovedError):
            service.run_binding("b1", source="approved", version=None, user_email="alice@x")

    def test_missing_snapshot_raises_422_error(self, service, monitored_tables, version_service):
        monitored_tables.get.return_value = _detail(version=5)
        version_service.get_checks.side_effect = LookupError("no snapshot")
        with pytest.raises(MissingSnapshotError):
            service.run_binding("b1", source="approved", version=9, user_email="alice@x")

    def test_empty_resolved_checks_raises(self, service, monitored_tables, version_service):
        monitored_tables.get.return_value = _detail(version=2)
        version_service.get_checks.return_value = []
        with pytest.raises(BindingRunError):
            service.run_binding("b1", source="approved", version=2, user_email="alice@x")


class TestSubmission:
    def test_submits_exactly_the_resolved_checks(self, service, monitored_tables, version_service, job_service):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS

        service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        _, kwargs = job_service.submit_run.call_args
        assert kwargs["config"]["checks"] == _CHECKS
        assert kwargs["config"]["source_table_fqn"] == "cat.schema.tbl"
        assert kwargs["config"]["is_sql_check"] is False
        assert kwargs["requesting_user"] == "alice@x"

    def test_creates_view_and_returns_view_fqn(self, service, monitored_tables, version_service, view_service):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS

        result = service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        view_service.create_view.assert_called_once_with("cat.schema.tbl")
        assert result.view_fqn == "dqx_studio_tmp.tmp_view_1"
        assert result.job_run_id == 555

    def test_mints_run_set_when_none_supplied(self, service, monitored_tables, version_service, run_set_service):
        monitored_tables.get.return_value = _detail(version=2)
        version_service.get_checks.return_value = _CHECKS

        result = service.run_binding("b1", source="approved", version=2, user_email="alice@x", trigger="manual")

        run_set_service.create.assert_called_once_with(
            product_id=None, product_version=None, source="approved", trigger="manual", created_by="alice@x"
        )
        assert result.run_set_id == "rs-new"

    def test_joins_supplied_run_set_without_minting(self, service, monitored_tables, version_service, run_set_service):
        monitored_tables.get.return_value = _detail(version=2)
        version_service.get_checks.return_value = _CHECKS

        result = service.run_binding(
            "b1", source="approved", version=2, user_email="alice@x", run_set_id="rs-existing"
        )

        run_set_service.create.assert_not_called()
        assert result.run_set_id == "rs-existing"
        run_set_service.add_member.assert_called_once_with("rs-existing", result.run_id, "b1", 2)

    def test_records_dryrun_started(self, service, monitored_tables, version_service, job_service):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS

        result = service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        job_service.record_dryrun_started.assert_called_once_with(
            table=_RUNS_TABLE,
            run_id=result.run_id,
            requesting_user="alice@x",
            source_table_fqn="cat.schema.tbl",
            view_fqn="dqx_studio_tmp.tmp_view_1",
            sample_size=1000,
            job_run_id=555,
        )

    def test_submit_failure_drops_the_temp_view(self, service, monitored_tables, version_service, job_service, view_service):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS
        job_service.submit_run.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError):
            service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        view_service.drop_view.assert_called_once_with("dqx_studio_tmp.tmp_view_1")

    def test_uses_custom_sample_size(self, service, monitored_tables, version_service, job_service):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS

        service.run_binding("b1", source="approved", version=2, user_email="alice@x", sample_size=250)

        _, submit_kwargs = job_service.submit_run.call_args
        assert submit_kwargs["config"]["sample_size"] == 250
        job_service.record_dryrun_started.assert_called_once()
        _, started_kwargs = job_service.record_dryrun_started.call_args
        assert started_kwargs["sample_size"] == 250

    def test_defaults_sample_size_when_not_supplied(self, service, monitored_tables, version_service, job_service):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS

        service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        _, submit_kwargs = job_service.submit_run.call_args
        assert submit_kwargs["config"]["sample_size"] == 1000


class TestFailClosedOrdering:
    """Regression coverage for the fail-closed write order in ``run_binding``.

    A ``dq_run_set_members`` row must never be persisted unless it points
    at an existing ``dq_validation_runs`` row. ``record_dryrun_started``
    (which inserts that row) therefore has to run BEFORE the run set is
    minted/joined and BEFORE the member is added — these tests pin that
    ordering by asserting what does and does not get written when each
    step fails.
    """

    def test_record_dryrun_started_failure_persists_no_run_set_member(
        self, service, monitored_tables, version_service, job_service, run_set_service, view_service
    ):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS
        job_service.record_dryrun_started.side_effect = RuntimeError("db unavailable")

        with pytest.raises(RuntimeError, match="db unavailable"):
            service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        # The fail-closed invariant: no member (and no run set) was ever
        # created, because record_dryrun_started — which must succeed
        # first — never did.
        run_set_service.create.assert_not_called()
        run_set_service.add_member.assert_not_called()
        # The temp view is still cleaned up on this failure path.
        view_service.drop_view.assert_called_once_with("dqx_studio_tmp.tmp_view_1")

    def test_run_set_create_failure_after_dryrun_started_leaves_validation_run_orphaned_but_no_member(
        self, service, monitored_tables, version_service, job_service, run_set_service, view_service
    ):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS
        run_set_service.create.side_effect = RuntimeError("run set insert failed")

        with pytest.raises(RuntimeError, match="run set insert failed"):
            service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        # record_dryrun_started already succeeded — the validation-run row
        # exists — but no run-set member was (or could be) written.
        job_service.record_dryrun_started.assert_called_once()
        run_set_service.add_member.assert_not_called()
        # The job run is already live and reading the view; a run-set
        # bookkeeping failure must NOT drop it out from under that run.
        view_service.drop_view.assert_not_called()

    def test_add_member_failure_rolls_back_minted_run_set(
        self, service, monitored_tables, version_service, job_service, run_set_service, view_service
    ):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS
        run_set_service.add_member.side_effect = RuntimeError("member insert failed")

        with pytest.raises(RuntimeError, match="member insert failed"):
            service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        # record_dryrun_started already succeeded (validation run exists),
        # a run set was minted, add_member failed on it — the freshly
        # minted, now-empty run set must be rolled back so it never
        # aggregates as a phantom "success" with zero members.
        job_service.record_dryrun_started.assert_called_once()
        run_set_service.create.assert_called_once()
        run_set_service.delete_empty.assert_called_once_with("rs-new")
        # The job run is already live and reading the view; a run-set
        # bookkeeping failure must NOT drop it out from under that run.
        view_service.drop_view.assert_not_called()

    def test_add_member_failure_on_caller_supplied_run_set_does_not_delete_it(
        self, service, monitored_tables, version_service, job_service, run_set_service, view_service
    ):
        monitored_tables.get.return_value = _detail(table_fqn="cat.schema.tbl", version=2)
        version_service.get_checks.return_value = _CHECKS
        run_set_service.add_member.side_effect = RuntimeError("member insert failed")

        with pytest.raises(RuntimeError, match="member insert failed"):
            service.run_binding(
                "b1", source="approved", version=2, user_email="alice@x", run_set_id="rs-existing"
            )

        # A caller-supplied run set (product fan-out) may already have
        # other members — it must never be deleted just because one
        # binding's add_member failed.
        run_set_service.create.assert_not_called()
        run_set_service.delete_empty.assert_not_called()
        # The job run is already live and reading the view; a run-set
        # bookkeeping failure must NOT drop it out from under that run.
        view_service.drop_view.assert_not_called()


class TestSyntheticCrossTableDispatch:
    def test_synthetic_binding_builds_view_from_sql_query(self, service, monitored_tables, version_service, view_service):
        monitored_tables.get.return_value = _detail(table_fqn="__sql_check__/my_rule", version=2)
        checks = [
            {"check": {"function": "sql_query", "arguments": {"query": "SELECT 1"}}},
        ]
        version_service.get_checks.return_value = checks

        result = service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        view_service.create_view_from_sql.assert_called_once_with("SELECT 1")
        view_service.create_view.assert_not_called()
        assert result.view_fqn == "dqx_studio_tmp.tmp_view_2"

    def test_synthetic_binding_missing_sql_query_raises(self, service, monitored_tables, version_service):
        monitored_tables.get.return_value = _detail(table_fqn="__sql_check__/my_rule", version=2)
        version_service.get_checks.return_value = _CHECKS  # no sql_query function present

        with pytest.raises(BindingRunError):
            service.run_binding("b1", source="approved", version=2, user_email="alice@x")


class TestCustomMetrics:
    def test_custom_metrics_included_when_configured(self, service, monitored_tables, version_service, job_service, settings_service):
        monitored_tables.get.return_value = _detail(version=2)
        version_service.get_checks.return_value = _CHECKS
        settings_service.get_custom_metrics.return_value = ["metric_a"]

        service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        _, kwargs = job_service.submit_run.call_args
        assert kwargs["config"]["custom_metrics"] == ["metric_a"]

    def test_custom_metrics_omitted_when_empty(self, service, monitored_tables, version_service, job_service, settings_service):
        monitored_tables.get.return_value = _detail(version=2)
        version_service.get_checks.return_value = _CHECKS
        settings_service.get_custom_metrics.return_value = []

        service.run_binding("b1", source="approved", version=2, user_email="alice@x")

        _, kwargs = job_service.submit_run.call_args
        assert "custom_metrics" not in kwargs["config"]
