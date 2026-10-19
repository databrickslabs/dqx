"""Run a setup step's GRANT statements as the signed-in administrator.

The statements come only from the setup checks that display them, never from the
client. Each one runs on its own so a failure doesn't stop the rest, and failures are
reported as a small set of codes; raw error text is never returned or logged.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from databricks.sdk.errors import (
    DeadlineExceeded,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
)

from databricks_labs_dqx_app.backend.setup.models import GrantErrorCode, SetupGrantFailure
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor, SqlStatementError

logger = logging.getLogger(__name__)

GRANT_PREFIX = "GRANT "
_PERMISSION_SQLSTATES = frozenset({"42501"})
_NOT_FOUND_SQLSTATES = frozenset({"42704", "42P01", "42S02", "42883"})
_PERMISSION_MARKERS = ("PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS", "INSUFFICIENT_PRIVILEGES")
_NOT_FOUND_MARKERS = ("_NOT_FOUND", "DOES_NOT_EXIST")
_WAREHOUSE_ERRORS = (TemporarilyUnavailable, DeadlineExceeded, ResourceExhausted, TooManyRequests, TimeoutError)


@dataclass(frozen=True)
class GrantRun:
    """Outcome of running a step's GRANT statements.

    Args:
        applied: Number of statements that ran successfully.
        total: Number of GRANT statements attempted.
        failed: Statements that failed, each with a sanitized error code.
    """

    applied: int
    total: int
    failed: tuple[SetupGrantFailure, ...]


def grant_error_code(error: Exception) -> GrantErrorCode:
    """Map a GRANT failure to a sanitized error code.

    Args:
        error: Exception raised while running a GRANT statement.

    Returns:
        *permission_denied*, *not_found*, *warehouse_unavailable* or *unknown*.
    """
    if isinstance(error, PermissionDenied):
        return "permission_denied"
    if isinstance(error, NotFound):
        return "not_found"
    if isinstance(error, _WAREHOUSE_ERRORS):
        return "warehouse_unavailable"
    if isinstance(error, SqlStatementError):
        if error.sqlstate in _PERMISSION_SQLSTATES:
            return "permission_denied"
        if error.sqlstate in _NOT_FOUND_SQLSTATES:
            return "not_found"
        # Only the error part is inspected: the statement text after it names securables.
        message = str(error).split("\nSQL:", 1)[0].upper()
        if any(marker in message for marker in _PERMISSION_MARKERS):
            return "permission_denied"
        if any(marker in message for marker in _NOT_FOUND_MARKERS):
            return "not_found"
        return "unknown"
    if type(error) is RuntimeError:
        # The executor raises a plain RuntimeError when a statement times out or never finishes.
        return "warehouse_unavailable"
    return "unknown"


def run_grant_statements(statements: Sequence[str], executor: SqlExecutor | None) -> GrantRun:
    """Run each GRANT statement once through *executor*, continuing past failures.

    Args:
        statements: Statements built by the setup checks; anything that isn't a GRANT is skipped.
        executor: The signed-in administrator's SQL executor, or None when none could be built.

    Returns:
        How many statements ran and which ones failed, with sanitized error codes.
    """
    grants = [statement for statement in statements if statement.startswith(GRANT_PREFIX)]
    failed: list[SetupGrantFailure] = []
    for statement in grants:
        if executor is None:
            failed.append(SetupGrantFailure(statement=statement, error_code="warehouse_unavailable"))
            continue
        try:
            executor.execute_no_schema(statement.rstrip().removesuffix(";"))
        except Exception as error:
            code = grant_error_code(error)
            logger.warning(f"A setup GRANT statement could not be run ({type(error).__name__}, {code})")
            failed.append(SetupGrantFailure(statement=statement, error_code=code))
    return GrantRun(applied=len(grants) - len(failed), total=len(grants), failed=tuple(failed))
