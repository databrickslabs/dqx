"""Unit tests for the dataset-level rule dispatch helpers.

The synthetic ``__sql_check__/<name>`` table_fqn is used by the catalog
to group *cross-table SQL* checks. ``_extract_sql_query`` pulls the
embedded query so the runner can build a view from it and take the SQL
fast-path.

Reference checks such as ``has_valid_schema`` / ``foreign_key`` are NOT
dataset-synthetic: they carry a real target-table FQN and flow through
the normal ``create_view(table_fqn)`` path, where the row-level engine
handles them. They therefore have no ``sql_query`` — a synthetic rule
that lacks one is a malformed cross-table rule, not a schema rule.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.routes.v1.dryrun import _extract_sql_query
from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService


def _sql_check() -> dict[str, object]:
    return {
        "name": "cross_table_orders_have_customers",
        "criticality": "error",
        "check": {
            "function": "sql_query",
            "arguments": {"query": "SELECT 1 FROM main.shop.orders LIMIT 1"},
        },
    }


def _schema_check() -> dict[str, object]:
    """A ``has_valid_schema`` check — has no ``sql_query`` by design."""
    return {
        "name": "valid_customer_schema",
        "criticality": "error",
        "check": {
            "function": "has_valid_schema",
            "arguments": {"expected_schema": "id BIGINT, email STRING"},
        },
    }


# ---------------------------------------------------------------------------
# _extract_sql_query (route helper)
# ---------------------------------------------------------------------------


def test_extract_sql_query_returns_query_for_sql_check() -> None:
    assert _extract_sql_query([_sql_check()]) == "SELECT 1 FROM main.shop.orders LIMIT 1"


def test_extract_sql_query_returns_none_for_schema_check() -> None:
    # has_valid_schema is not a SQL check, so it never yields a query.
    assert _extract_sql_query([_schema_check()]) is None


def test_extract_sql_query_handles_empty_list() -> None:
    assert _extract_sql_query([]) is None


def test_extract_sql_query_handles_missing_arguments() -> None:
    bare = {"check": {"function": "sql_query"}}
    assert _extract_sql_query([bare]) is None


# The route module and the scheduler must stay in lock-step or scheduled
# cross-table SQL runs would silently regress.
def test_scheduler_sql_query_helper_matches_route_helper() -> None:
    checks = [_sql_check()]
    assert SchedulerService._extract_sql_query(checks) == _extract_sql_query(checks)
