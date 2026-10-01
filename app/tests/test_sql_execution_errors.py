"""SQL execution diagnostics at the SDK boundary."""

from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ColumnInfo,
    ResultData,
    ResultManifest,
    ResultSchema,
    ServiceError,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


@pytest.fixture
def workspace() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


@pytest.mark.parametrize("method", ["execute", "execute_no_schema", "query", "query_dicts"])
@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("message", [None, "sensitive diagnostic"])
@pytest.mark.parametrize(
    "sqlstate, expected",
    [("42501", "42501"), ("42P01", "42P01"), (None, None), ("42501\nforged", None), ("secret", None)],
)
def test_sql_failure_preserves_only_validated_sqlstate(
    workspace: MagicMock, method: str, pending: bool, message: str | None, sqlstate: str | None, expected: str | None
) -> None:
    failed = StatementResponse(
        statement_id="statement-id",
        status=StatementStatus(
            state=StatementState.FAILED,
            sql_state=sqlstate,
            error=ServiceError(message=message) if message is not None else None,
        ),
    )
    workspace.statement_execution.execute_statement.return_value = (
        StatementResponse(statement_id="statement-id", status=StatementStatus(state=StatementState.PENDING))
        if pending
        else failed
    )
    workspace.statement_execution.get_statement.return_value = failed
    executor = SqlExecutor(workspace, "warehouse", "main", "studio")

    with pytest.raises(RuntimeError) as caught:
        getattr(executor, method)("SELECT 1")

    assert getattr(caught.value, "sqlstate", "missing") == expected
    prefix = "SQL execution failed" if method in {"execute", "execute_no_schema"} else "SQL query failed"
    assert str(caught.value) == f"{prefix}: {message if message is not None else 'Unknown error'}\nSQL: SELECT 1"


@pytest.mark.parametrize(
    "method, expected",
    [("execute", None), ("execute_no_schema", None), ("query", [["1"]]), ("query_dicts", [{"value": "1"}])],
)
def test_successful_polling_preserves_query_results(workspace: MagicMock, method: str, expected: object) -> None:
    workspace.statement_execution.execute_statement.return_value = StatementResponse(
        statement_id="statement-id", status=StatementStatus(state=StatementState.PENDING)
    )
    workspace.statement_execution.get_statement.return_value = StatementResponse(
        statement_id="statement-id",
        status=StatementStatus(state=StatementState.SUCCEEDED),
        result=ResultData(data_array=[["1"]]),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="value")])),
    )
    executor = SqlExecutor(workspace, "warehouse", "main", "studio")

    assert getattr(executor, method)("SELECT 1 AS value") == expected
