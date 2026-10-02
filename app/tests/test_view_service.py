"""Temporary views expose data only to the runner and remain app-cleanable."""

from collections.abc import Callable, Iterator
from unittest.mock import MagicMock, call

import pytest

from databricks_labs_dqx_app.backend.services.view_service import (
    ViewService,
    mark_tmp_schema_ready,
    reset_tmp_schema_ready,
)

RUNNER = "11111111-1111-4111-8111-111111111111"
CLEANUP = "22222222-2222-4222-8222-222222222222"


@pytest.fixture(autouse=True)
def ready_tmp_schema() -> Iterator[None]:
    mark_tmp_schema_ready()
    try:
        yield
    finally:
        reset_tmp_schema_ready()


@pytest.fixture(params=["table", "sql"])
def create_view(request: pytest.FixtureRequest) -> Callable[[ViewService], str]:
    if request.param == "table":
        return lambda service: service.create_view("source.schema.table")
    return lambda service: service.create_view_from_sql("SELECT * FROM source.schema.table")


def test_view_grants_only_runner_select_and_app_manage(
    sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]
) -> None:
    service = ViewService(sql_executor_mock, runner_principal=RUNNER, cleanup_principal=CLEANUP)

    view = create_view(service)

    quoted = ".".join(f"`{part}`" for part in view.split("."))
    statements = [entry.args[0] for entry in sql_executor_mock.execute.call_args_list]
    assert statements[0] in (
        f"CREATE OR REPLACE VIEW {quoted} AS SELECT * FROM `source`.`schema`.`table`",
        f"CREATE OR REPLACE VIEW {quoted} AS SELECT * FROM source.schema.table",
    )
    assert statements[1:] == [
        f"GRANT MANAGE ON VIEW {quoted} TO `{CLEANUP}`",
        f"GRANT SELECT ON VIEW {quoted} TO `{RUNNER}`",
        f"DESCRIBE TABLE {quoted}",
    ]
    assert all("account users" not in statement for statement in statements)
    assert all("OWNER" not in statement for statement in statements)


@pytest.mark.parametrize("field", ["runner_principal", "cleanup_principal"])
@pytest.mark.parametrize(
    "principal",
    [
        "",
        " ",
        "``",
        "runner\n",
        "runner\r",
        "runner\x00",
        "runner\x7f",
        "runner\x85",
        "users",
        "`UsErS`",
        "account users",
        "`Account Users`",
    ],
)
def test_invalid_principal_fails_before_creating(
    sql_executor_mock: MagicMock,
    create_view: Callable[[ViewService], str],
    field: str,
    principal: str,
) -> None:
    identities = {"runner_principal": RUNNER, "cleanup_principal": CLEANUP, field: principal}
    reset_tmp_schema_ready()

    with pytest.raises(RuntimeError):
        service = ViewService(sql_executor_mock, sp_sql=sql_executor_mock, **identities)
        create_view(service)

    sql_executor_mock.execute.assert_not_called()
    sql_executor_mock.execute_no_schema.assert_not_called()
    sql_executor_mock.query.assert_not_called()


def test_missing_default_runner_fails_closed(
    sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]
) -> None:
    with pytest.raises(RuntimeError):
        create_view(ViewService(sql_executor_mock))
    sql_executor_mock.execute.assert_not_called()


def test_missing_cleanup_identity_fails_closed(
    sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]
) -> None:
    with pytest.raises(RuntimeError):
        create_view(ViewService(sql_executor_mock, runner_principal=RUNNER))
    sql_executor_mock.execute.assert_not_called()


def test_principal_is_quoted_as_one_identifier(
    sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]
) -> None:
    service = ViewService(sql_executor_mock, runner_principal="runner`name@example.com", cleanup_principal=CLEANUP)
    create_view(service)
    grants = [
        entry.args[0] for entry in sql_executor_mock.execute.call_args_list if entry.args[0].startswith("GRANT SELECT")
    ]
    assert len(grants) == 1
    assert grants[0].endswith(" TO `runner``name@example.com`")


@pytest.mark.parametrize("failed_privilege", ["MANAGE", "SELECT"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_failed_grant_cleans_partial_view_and_redacts_errors(
    sql_executor_mock: MagicMock,
    create_view: Callable[[ViewService], str],
    failed_privilege: str,
    cleanup_fails: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive = "sensitive-source token=do-not-log"

    def execute(statement: str, *, timeout_seconds: int = 120) -> None:
        if statement.startswith(f"GRANT {failed_privilege}") or (cleanup_fails and statement.startswith("DROP VIEW")):
            raise RuntimeError(sensitive)

    sql_executor_mock.execute.side_effect = execute
    service = ViewService(sql_executor_mock, runner_principal=RUNNER, cleanup_principal=CLEANUP)

    with pytest.raises(RuntimeError) as error:
        create_view(service)

    statements = [entry.args[0] for entry in sql_executor_mock.execute.call_args_list]
    quoted_view = statements[0].split(" AS ", 1)[0].removeprefix("CREATE OR REPLACE VIEW ")
    assert statements[-1] == f"DROP VIEW IF EXISTS {quoted_view}"
    assert not any(statement.startswith("DESCRIBE") for statement in statements)
    assert sensitive not in str(error.value)
    assert sensitive not in caplog.text
    assert error.value.__suppress_context__


def test_drop_falls_back_to_app_executor(sql_executor_mock: MagicMock) -> None:
    from unittest.mock import create_autospec

    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

    sp_sql = create_autospec(SqlExecutor, instance=True)
    service = ViewService(sql_executor_mock, sp_sql=sp_sql, runner_principal=RUNNER, cleanup_principal=CLEANUP)
    view = service.create_view("source.schema.table")
    sql_executor_mock.execute.side_effect = RuntimeError("OBO expired")

    service.drop_view(view)

    quoted = ".".join(f"`{part}`" for part in view.split("."))
    sp_sql.execute.assert_has_calls([call(f"DROP VIEW IF EXISTS {quoted}")])
