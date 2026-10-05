"""Exercise scheduled profiling permissions through the scheduler lifecycle."""

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job, JobRunAs, JobSettings
from databricks.sdk.service.sql import StatementResponse, StatementState, StatementStatus

from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService
from databricks_labs_dqx_app.backend.services.scheduler_service import logger as scheduler_logger

RUNNER = "11111111-1111-4111-8111-111111111111"
ProfileScheduler = tuple[SchedulerService, MagicMock, asyncio.Event]


@pytest.fixture
async def scheduled_profile(
    sql_executor_mock: MagicMock, caplog: pytest.LogCaptureFixture
) -> AsyncIterator[ProfileScheduler]:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name=RUNNER)))
    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )
    sql_executor_mock.fqn.side_effect = lambda name: f"dqx_test.dqx_app_test.{name}"
    sql_executor_mock.ts_text.side_effect = lambda name: name
    finished = asyncio.Event()
    loop = asyncio.get_running_loop()

    def query(statement: str, *, timeout_seconds: int = 120) -> list[list[str]]:
        if "FROM dqx_test.dqx_app_test.dq_monitored_tables" in statement:
            return [["binding1", "* * * * *", "UTC", "source.schema.table", "profiling_only"]]
        if "FROM dqx_test.dqx_app_test.dq_schedule_runs" in statement:
            return [
                [
                    "table:binding1",
                    "2000-01-01T00:00:00+00:00",
                    "2000-01-01T00:01:00+00:00",
                    "previous",
                    "success",
                    "false",
                ]
            ]
        return []

    def upsert(table: str, key_cols: dict[str, object], value_cols: dict[str, object]) -> None:
        loop.call_soon_threadsafe(finished.set)

    sql_executor_mock.query.side_effect = query
    sql_executor_mock.upsert.side_effect = upsert
    service = SchedulerService(
        ws=ws,
        warehouse_id="test-warehouse",
        catalog="dqx_test",
        schema="dqx_app_test",
        tmp_schema="dqx_app_test_tmp",
        job_id="123",
        oltp_sql=sql_executor_mock,
        binding_run_service=create_autospec(BindingRunService, instance=True),
    )
    scheduler_logger.addHandler(caplog.handler)
    try:
        yield service, ws, finished
    finally:
        await service.stop()
        scheduler_logger.removeHandler(caplog.handler)


@pytest.mark.parametrize("run_as", [JobRunAs(service_principal_name=RUNNER), JobRunAs(user_name="runner@example.com")])
async def test_scheduled_profile_grants_actual_job_runner(
    scheduled_profile: ProfileScheduler, run_as: JobRunAs
) -> None:
    service, ws, finished = scheduled_profile
    ws.jobs.get.return_value = Job(settings=JobSettings(run_as=run_as))

    service.start()
    await asyncio.wait_for(finished.wait(), timeout=5)

    ws.jobs.get.assert_called_once_with(job_id=123)
    statements = [entry.kwargs["statement"] for entry in ws.statement_execution.execute_statement.call_args_list]
    grants = [statement for statement in statements if statement.startswith("GRANT")]
    principal = run_as.service_principal_name or run_as.user_name
    assert len(grants) == 1
    assert grants[0].startswith("GRANT SELECT ON VIEW `dqx_test`.`dqx_app_test_tmp`.`tmp_view_")
    assert grants[0].endswith(f" TO `{principal}`")
    assert all("account users" not in statement for statement in statements)
    ws.jobs.run_now.assert_called_once()


@pytest.mark.parametrize(
    "job",
    [
        Job(),
        Job(settings=JobSettings(run_as=JobRunAs())),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner\n"))),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner\x85"))),
        Job(settings=JobSettings(run_as=JobRunAs(user_name="account users"))),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name=RUNNER, user_name="other@example.com"))),
    ],
)
async def test_invalid_runner_prevents_scheduled_view_creation(scheduled_profile: ProfileScheduler, job: Job) -> None:
    service, ws, finished = scheduled_profile
    ws.jobs.get.return_value = job

    service.start()
    await asyncio.wait_for(finished.wait(), timeout=5)

    statements = [entry.kwargs["statement"] for entry in ws.statement_execution.execute_statement.call_args_list]
    assert not any(statement.startswith("CREATE OR REPLACE VIEW") for statement in statements)
    ws.jobs.run_now.assert_not_called()


async def test_runner_lookup_failure_prevents_creation(
    scheduled_profile: ProfileScheduler, caplog: pytest.LogCaptureFixture
) -> None:
    service, ws, finished = scheduled_profile
    ws.jobs.get.side_effect = RuntimeError("sensitive lookup token")

    service.start()
    await asyncio.wait_for(finished.wait(), timeout=5)

    ws.jobs.run_now.assert_not_called()
    statements = [entry.kwargs["statement"] for entry in ws.statement_execution.execute_statement.call_args_list]
    assert not any(statement.startswith("CREATE OR REPLACE VIEW") for statement in statements)
    assert "sensitive lookup token" not in caplog.text


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_failed_scheduler_grant_drops_view_and_never_submits(
    scheduled_profile: ProfileScheduler, cleanup_fails: bool, caplog: pytest.LogCaptureFixture
) -> None:
    service, ws, finished = scheduled_profile

    def execute_statement(**kwargs: object) -> StatementResponse:
        statement = str(kwargs["statement"])
        if statement.startswith("GRANT") or (cleanup_fails and statement.startswith("DROP VIEW")):
            raise RuntimeError("sensitive grant token")
        return StatementResponse(status=StatementStatus(state=StatementState.SUCCEEDED))

    ws.statement_execution.execute_statement.side_effect = execute_statement

    service.start()
    await asyncio.wait_for(finished.wait(), timeout=5)

    statements = [entry.kwargs["statement"] for entry in ws.statement_execution.execute_statement.call_args_list]
    created = next(statement for statement in statements if statement.startswith("CREATE OR REPLACE VIEW"))
    quoted_view = created.split(" AS ", 1)[0].removeprefix("CREATE OR REPLACE VIEW ")
    assert f"DROP VIEW IF EXISTS {quoted_view}" in statements
    assert not any(statement.startswith("DESCRIBE") for statement in statements)
    ws.jobs.run_now.assert_not_called()
    assert "sensitive grant token" not in caplog.text
