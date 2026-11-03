"""Running setup GRANT statements as the signed-in administrator."""

from unittest.mock import call, create_autospec

import pytest
from databricks.sdk.errors import (
    BadRequest,
    DeadlineExceeded,
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
)

from databricks_labs_dqx_app.backend.setup.grant_application import grant_error_code, run_grant_statements
from databricks_labs_dqx_app.backend.setup.models import SetupGrantFailure
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor, SqlStatementError

_STATEMENTS = (
    "GRANT USE_CATALOG ON CATALOG `main` TO `data-team`;",
    "GRANT USE_SCHEMA ON SCHEMA `main`.`dqx_studio` TO `runner`;",
    "GRANT READ_VOLUME ON VOLUME `main`.`dqx_studio`.`wheels` TO `runner`;",
)


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (PermissionDenied("User does not have MANAGE on catalog main"), "permission_denied"),
        (
            SqlStatementError("SQL execution failed: [INSUFFICIENT_PERMISSIONS] nope\nSQL: GRANT", "42501"),
            "permission_denied",
        ),
        (SqlStatementError("SQL execution failed: PERMISSION_DENIED: nope\nSQL: GRANT", None), "permission_denied"),
        # Only a 404 that names the warehouse means the warehouse itself is gone.
        (NotFound("Warehouse abc123 does not exist."), "warehouse_unavailable"),
        (ResourceDoesNotExist("SQL warehouse abc123 was not found"), "warehouse_unavailable"),
        (NotFound("Warehouse 'abc123' not found"), "warehouse_unavailable"),
        (NotFound("gone"), "not_found"),
        (ResourceDoesNotExist("Principal runner does not exist"), "not_found"),
        # A securable whose name only contains "warehouse" is still an ordinary 404.
        (NotFound("Catalog 'data_warehouse' does not exist."), "not_found"),
        (NotFound("Table sales_warehouse.orders.daily was not found"), "not_found"),
        (ResourceDoesNotExist("Schema `main`.`warehouse` does not exist"), "not_found"),
        (NotFound("Principal warehouse-admins does not exist"), "not_found"),
        (TooManyRequests("slow down"), "rate_limited"),
        (ResourceExhausted("quota"), "rate_limited"),
        (SqlStatementError("SQL execution failed: [SCHEMA_NOT_FOUND] x\nSQL: GRANT", "42704"), "not_found"),
        (SqlStatementError("SQL execution failed: [PRINCIPAL_DOES_NOT_EXIST] x\nSQL: GRANT", None), "not_found"),
        (TemporarilyUnavailable("warehouse starting"), "warehouse_unavailable"),
        (DeadlineExceeded("slow"), "warehouse_unavailable"),
        (RuntimeError("SQL statement abc timed out after 120s"), "warehouse_unavailable"),
        (SqlStatementError("SQL execution failed: [PARSE_SYNTAX_ERROR] x\nSQL: GRANT", "42601"), "unknown"),
        (BadRequest("bad"), "unknown"),
        (ValueError("boom"), "unknown"),
    ],
)
def test_grant_errors_map_to_a_small_set_of_codes(error: Exception, code: str) -> None:
    assert grant_error_code(error) == code


def test_identifier_text_in_the_statement_does_not_decide_the_code() -> None:
    """A securable named like an error marker must not change the classification."""
    error = SqlStatementError("SQL execution failed: [PARSE_SYNTAX_ERROR] x\nSQL: GRANT ON `not_found`", None)

    assert grant_error_code(error) == "unknown"


def test_every_statement_runs_once_without_its_trailing_semicolon() -> None:
    executor = create_autospec(SqlExecutor, instance=True)

    result = run_grant_statements(_STATEMENTS, executor)

    assert executor.execute_no_schema.call_args_list == [call(statement[:-1]) for statement in _STATEMENTS]
    assert result.applied == 3
    assert result.failed == ()


def test_a_failed_statement_does_not_stop_the_rest() -> None:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.execute_no_schema.side_effect = [None, PermissionDenied("secret detail"), None]

    result = run_grant_statements(_STATEMENTS, executor)

    assert executor.execute_no_schema.call_count == 3
    assert result.applied == 2
    assert result.failed == (SetupGrantFailure(statement=_STATEMENTS[1], error_code="permission_denied"),)
    assert "secret detail" not in repr(result)


def test_only_grant_statements_are_run() -> None:
    executor = create_autospec(SqlExecutor, instance=True)

    result = run_grant_statements(("DROP SCHEMA `main`.`x`;", _STATEMENTS[0], "grant all"), executor)

    executor.execute_no_schema.assert_called_once_with(_STATEMENTS[0][:-1])
    assert result.applied == 1
    assert result.failed == ()


def test_without_a_sql_executor_every_statement_reports_the_warehouse() -> None:
    result = run_grant_statements(_STATEMENTS, None)

    assert result.applied == 0
    assert [failure.error_code for failure in result.failed] == ["warehouse_unavailable"] * 3


def test_once_the_warehouse_is_unavailable_the_remaining_statements_are_not_attempted() -> None:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.execute_no_schema.side_effect = [None, TemporarilyUnavailable("warehouse starting")]

    result = run_grant_statements(_STATEMENTS, executor)

    assert executor.execute_no_schema.call_count == 2
    assert result.applied == 1
    assert result.failed == (
        SetupGrantFailure(statement=_STATEMENTS[1], error_code="warehouse_unavailable"),
        SetupGrantFailure(statement=_STATEMENTS[2], error_code="warehouse_unavailable"),
    )


def test_a_missing_securable_does_not_stop_the_rest() -> None:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.execute_no_schema.side_effect = [NotFound("Principal runner does not exist"), None, None]

    result = run_grant_statements(_STATEMENTS, executor)

    assert executor.execute_no_schema.call_count == 3
    assert result.failed == (SetupGrantFailure(statement=_STATEMENTS[0], error_code="not_found"),)


def test_a_rate_limited_statement_is_retried_once_after_a_pause() -> None:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.execute_no_schema.side_effect = [None, TooManyRequests("slow down"), None, None]
    pauses: list[float] = []

    result = run_grant_statements(_STATEMENTS, executor, pause=pauses.append)

    assert executor.execute_no_schema.call_args_list == [
        call(_STATEMENTS[0][:-1]),
        call(_STATEMENTS[1][:-1]),
        call(_STATEMENTS[1][:-1]),
        call(_STATEMENTS[2][:-1]),
    ]
    assert len(pauses) == 1 and pauses[0] > 0
    assert (result.applied, result.failed) == (3, ())


def test_a_statement_still_rate_limited_after_its_retry_fails_without_stopping_the_rest() -> None:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.execute_no_schema.side_effect = [TooManyRequests("slow down"), TooManyRequests("slow down"), None, None]

    result = run_grant_statements(_STATEMENTS, executor, pause=lambda seconds: None)

    assert executor.execute_no_schema.call_count == 4
    assert result.applied == 2
    assert result.failed == (SetupGrantFailure(statement=_STATEMENTS[0], error_code="rate_limited"),)
