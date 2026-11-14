"""Temporary views grant only app cleanup rights; runner reads via schema-level SELECT."""

from collections.abc import Callable, Iterator
from unittest.mock import MagicMock, call

import pytest

from databricks_labs_dqx_app.backend.services.view_service import (
    ViewService,
    mark_tmp_schema_ready,
    reset_tmp_schema_ready,
)

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


def test_view_grants_only_app_manage(sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]) -> None:
    service = ViewService(sql_executor_mock, cleanup_principal=CLEANUP)

    view = create_view(service)

    quoted = ".".join(f"`{part}`" for part in view.split("."))
    statements = [entry.args[0] for entry in sql_executor_mock.execute.call_args_list]
    assert statements[0] in (
        f"CREATE OR REPLACE VIEW {quoted} AS SELECT * FROM `source`.`schema`.`table`",
        f"CREATE OR REPLACE VIEW {quoted} AS SELECT * FROM source.schema.table",
    )
    assert statements[1:] == [
        f"GRANT MANAGE ON VIEW {quoted} TO `{CLEANUP}`",
        f"DESCRIBE TABLE {quoted}",
    ]
    assert all("account users" not in statement for statement in statements)
    assert all("OWNER" not in statement for statement in statements)
    assert not any(statement.startswith("GRANT SELECT") for statement in statements)


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
    principal: str,
) -> None:
    reset_tmp_schema_ready()

    with pytest.raises(RuntimeError):
        service = ViewService(sql_executor_mock, sp_sql=sql_executor_mock, cleanup_principal=principal)
        create_view(service)

    sql_executor_mock.execute.assert_not_called()
    sql_executor_mock.execute_no_schema.assert_not_called()
    sql_executor_mock.query.assert_not_called()


def test_missing_cleanup_identity_fails_closed(
    sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]
) -> None:
    with pytest.raises(RuntimeError):
        create_view(ViewService(sql_executor_mock))
    sql_executor_mock.execute.assert_not_called()


def test_principal_is_quoted_as_one_identifier(
    sql_executor_mock: MagicMock, create_view: Callable[[ViewService], str]
) -> None:
    service = ViewService(sql_executor_mock, cleanup_principal="app`name@example.com")
    create_view(service)
    grants = [
        entry.args[0] for entry in sql_executor_mock.execute.call_args_list if entry.args[0].startswith("GRANT MANAGE")
    ]
    assert len(grants) == 1
    assert grants[0].endswith(" TO `app``name@example.com`")


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_failed_grant_cleans_partial_view_and_redacts_errors(
    sql_executor_mock: MagicMock,
    create_view: Callable[[ViewService], str],
    cleanup_fails: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive = "sensitive-source token=do-not-log"

    def execute(statement: str, *, timeout_seconds: int = 120) -> None:
        if statement.startswith("GRANT MANAGE") or (cleanup_fails and statement.startswith("DROP VIEW")):
            raise RuntimeError(sensitive)

    sql_executor_mock.execute.side_effect = execute
    service = ViewService(sql_executor_mock, cleanup_principal=CLEANUP)

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
    service = ViewService(sql_executor_mock, sp_sql=sp_sql, cleanup_principal=CLEANUP)
    view = service.create_view("source.schema.table")
    sql_executor_mock.execute.side_effect = RuntimeError("OBO expired")

    service.drop_view(view)

    quoted = ".".join(f"`{part}`" for part in view.split("."))
    sp_sql.execute.assert_has_calls([call(f"DROP VIEW IF EXISTS {quoted}")])
