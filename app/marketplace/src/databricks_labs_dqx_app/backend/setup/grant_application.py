"""Run GRANT statements as the signed-in user.

Used by setup steps and by the schedule preflight. The statements come only from the
server-side checks that display them, never from the client. Each one runs on its own
so a failure doesn't stop the rest, and failures are reported as a small set of codes;
raw error text is never returned or logged.
"""

import logging
import re
import time
from collections.abc import Callable, Sequence
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
# The Statement Execution API's 404 for a missing warehouse starts by naming the warehouse,
# such as "Warehouse abc123 does not exist." A securable whose name only contains "warehouse"
# (a catalog called data_warehouse, say) is an ordinary 404.
_MISSING_WAREHOUSE = re.compile(
    r"\A\s*(?:(?:sql\s+)?warehouse\s+\S+\s+(?:does\s+not\s+exist|(?:was\s+)?not\s+found)"
    r"|no\s+(?:sql\s+)?warehouse\s+(?:was\s+)?found)",
    re.IGNORECASE,
)
_WAREHOUSE_ERRORS = (TemporarilyUnavailable, DeadlineExceeded, TimeoutError)
_RATE_LIMIT_ERRORS = (TooManyRequests, ResourceExhausted)
# How long to wait before running a rate-limited statement once more.
RATE_LIMIT_PAUSE_SECONDS = 2.0


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

    Only real warehouse unavailability is *warehouse_unavailable*, which stops the run:
    the warehouse not answering, a statement that never finishes, or the Statement
    Execution API's 404 for a missing warehouse. Other 404s, including one about a
    securable whose name contains "warehouse", are *not_found*, and 429s are *rate_limited*.

    Args:
        error: Exception raised while running a GRANT statement.

    Returns:
        *permission_denied*, *not_found*, *warehouse_unavailable*, *rate_limited* or *unknown*.
    """
    if isinstance(error, PermissionDenied):
        return "permission_denied"
    if isinstance(error, _RATE_LIMIT_ERRORS):
        return "rate_limited"
    if isinstance(error, _WAREHOUSE_ERRORS):
        return "warehouse_unavailable"
    if isinstance(error, NotFound):
        return "warehouse_unavailable" if _MISSING_WAREHOUSE.match(str(error)) else "not_found"
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


def run_grant_statements(
    statements: Sequence[str],
    executor: SqlExecutor | None,
    *,
    pause: Callable[[float], None] = time.sleep,
) -> GrantRun:
    """Run each GRANT statement once through *executor*, continuing past failures.

    A statement Databricks turns away as rate limited runs once more after a short
    pause, then counts as failed. Once the warehouse is unavailable, the remaining
    statements are reported as *warehouse_unavailable* without being attempted, rather
    than each waiting out the warehouse again.

    Args:
        statements: Statements built by the server-side checks; anything that isn't a GRANT is skipped.
        executor: The signed-in user's SQL executor, or None when none could be built.
        pause: Waits the given number of seconds before a rate-limited statement's retry.

    Returns:
        How many statements ran and which ones failed, with sanitized error codes.
    """
    grants = [statement for statement in statements if statement.startswith(GRANT_PREFIX)]
    failed: list[SetupGrantFailure] = []
    warehouse_unavailable = executor is None
    for statement in grants:
        if executor is None or warehouse_unavailable:
            failed.append(SetupGrantFailure(statement=statement, error_code="warehouse_unavailable"))
            continue
        code = _run_grant(statement, executor, pause)
        if code is not None:
            failed.append(SetupGrantFailure(statement=statement, error_code=code))
            warehouse_unavailable = code == "warehouse_unavailable"
    return GrantRun(applied=len(grants) - len(failed), total=len(grants), failed=tuple(failed))


def _run_grant(statement: str, executor: SqlExecutor, pause: Callable[[float], None]) -> GrantErrorCode | None:
    """Run one GRANT statement, retrying it once when rate limited; return its error code, or None."""
    sql = statement.rstrip().removesuffix(";")
    for attempt in range(2):
        try:
            executor.execute_no_schema(sql)
            return None
        except Exception as error:
            code = grant_error_code(error)
            logger.warning(f"A GRANT statement could not be run ({type(error).__name__}, {code})")
            if code != "rate_limited" or attempt == 1:
                return code
        pause(RATE_LIMIT_PAUSE_SECONDS)
    return "rate_limited"
