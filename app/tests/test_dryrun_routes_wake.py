"""Run-start routes in ``dryrun.py`` wake the scheduler from its idle sleep.

Route functions are called directly with autospec'd collaborators, following
``test_monitored_tables_routes.py``.
"""

from unittest.mock import MagicMock, create_autospec

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.models import BatchRunFromCatalogIn, DryRunIn
from databricks_labs_dqx_app.backend.routes.v1.dryrun import batch_run_from_catalog, submit_dry_run
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
from databricks_labs_dqx_app.backend.services.scheduler_wake import SchedulerWakeMarker
from databricks_labs_dqx_app.backend.services.view_service import ViewService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_CHECK = {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}


def _obo_ws() -> MagicMock:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.current_user.me.return_value.user_name = "alice@x"
    return ws


def _collaborators() -> dict[str, MagicMock]:
    view_svc = create_autospec(ViewService, instance=True)
    view_svc.create_view.return_value = "main.dqx_tmp.tmp_view_1"
    job_svc = create_autospec(JobService, instance=True)
    job_svc.submit_run.return_value = 42
    settings_svc = create_autospec(AppSettingsService, instance=True)
    settings_svc.get_custom_metrics.return_value = []
    sql = create_autospec(SqlExecutor, instance=True)
    sql.fqn.side_effect = lambda name: f"main.dqx.{name}"
    return {
        "obo_ws": _obo_ws(),
        "view_svc": view_svc,
        "job_svc": job_svc,
        "settings_svc": settings_svc,
        "sql": sql,
        "wake": create_autospec(SchedulerWakeMarker, instance=True),
    }


def _rules_svc(checks: list[dict[str, object]]) -> MagicMock:
    rules = create_autospec(RulesCatalogService, instance=True)
    rules.get_approved_checks_for_table.return_value = checks
    return rules


class TestBatchRunFromCatalogWakesScheduler:
    def test_submitted_batch_wakes_the_scheduler(self) -> None:
        deps = _collaborators()
        out = batch_run_from_catalog(
            BatchRunFromCatalogIn(table_fqns=["main.sales.orders"]), rules_svc=_rules_svc([_CHECK]), **deps
        )
        assert len(out.submitted) == 1
        deps["wake"].signal.assert_called_once_with()

    def test_batch_that_submitted_nothing_does_not_wake(self) -> None:
        deps = _collaborators()
        out = batch_run_from_catalog(
            BatchRunFromCatalogIn(table_fqns=["main.sales.orders"]), rules_svc=_rules_svc([]), **deps
        )
        assert out.submitted == []
        deps["wake"].signal.assert_not_called()


class TestSubmitDryRunWakesScheduler:
    @staticmethod
    def _validator() -> MagicMock:
        return MagicMock(return_value=ChecksValidationStatus())

    def test_recorded_run_wakes_the_scheduler(self) -> None:
        deps = _collaborators()
        submit_dry_run(
            DryRunIn(table_fqn="main.sales.orders", checks=[_CHECK]), validate_checks_fn=self._validator(), **deps
        )
        deps["wake"].signal.assert_called_once_with()

    def test_preview_without_history_does_not_wake(self) -> None:
        deps = _collaborators()
        submit_dry_run(
            DryRunIn(table_fqn="main.sales.orders", checks=[_CHECK], skip_history=True),
            validate_checks_fn=self._validator(),
            **deps,
        )
        deps["wake"].signal.assert_not_called()
