"""Unit tests for the dataset-level rule dispatch helpers.

These helpers decide *how* to materialise the input dataset when a rule
lives under the synthetic ``__sql_check__/<name>`` table_fqn used by the
catalog to group dataset-level checks:

  * ``sql_query`` checks       → build a view from the embedded query.
  * ``has_valid_schema`` checks → build a view from the real target
    table carried on ``user_metadata.target_table`` and run the regular
    row-level engine.

Regression coverage for the bug where the runner reported
``"SQL check has no query"`` for schema-validation rules.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.routes.v1.dryrun import (
    _extract_schema_check_target,
    _extract_sql_query,
)
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


def _schema_check(target: str = "samples.bakehouse.sales_customers") -> dict[str, object]:
    return {
        "name": "valid_customer_schema",
        "criticality": "error",
        "check": {
            "function": "has_valid_schema",
            "arguments": {"expected_schema": "id BIGINT, email STRING"},
        },
        "user_metadata": {
            "rule_type": "schema_validation",
            "strictness": "compatible",
            "target_table": target,
        },
    }


# ---------------------------------------------------------------------------
# _extract_sql_query (route helper)
# ---------------------------------------------------------------------------


def test_extract_sql_query_returns_query_for_sql_check() -> None:
    assert _extract_sql_query([_sql_check()]) == "SELECT 1 FROM main.shop.orders LIMIT 1"


def test_extract_sql_query_returns_none_for_schema_check() -> None:
    assert _extract_sql_query([_schema_check()]) is None


def test_extract_sql_query_handles_empty_list() -> None:
    assert _extract_sql_query([]) is None


def test_extract_sql_query_handles_missing_arguments() -> None:
    bare = {"check": {"function": "sql_query"}}
    assert _extract_sql_query([bare]) is None


# ---------------------------------------------------------------------------
# _extract_schema_check_target (route + scheduler helpers must agree)
# ---------------------------------------------------------------------------


def test_extract_schema_target_pulls_from_user_metadata() -> None:
    assert _extract_schema_check_target([_schema_check()]) == "samples.bakehouse.sales_customers"


def test_extract_schema_target_strips_whitespace() -> None:
    chk = _schema_check(target="  samples.bakehouse.sales_customers  ")
    assert _extract_schema_check_target([chk]) == "samples.bakehouse.sales_customers"


def test_extract_schema_target_returns_none_for_sql_check() -> None:
    assert _extract_schema_check_target([_sql_check()]) is None


def test_extract_schema_target_skips_blank_target() -> None:
    chk = _schema_check(target="   ")
    assert _extract_schema_check_target([chk]) is None


def test_extract_schema_target_picks_first_schema_check() -> None:
    first = _schema_check(target="cat.sch.first")
    second = _schema_check(target="cat.sch.second")
    assert _extract_schema_check_target([first, second]) == "cat.sch.first"


def test_extract_schema_target_ignores_non_schema_checks() -> None:
    not_schema = {"check": {"function": "is_not_null"}, "user_metadata": {"target_table": "x.y.z"}}
    chk = _schema_check()
    assert _extract_schema_check_target([not_schema, chk]) == "samples.bakehouse.sales_customers"


def test_extract_schema_target_handles_missing_user_metadata() -> None:
    chk = {"check": {"function": "has_valid_schema", "arguments": {}}}
    assert _extract_schema_check_target([chk]) is None


# Same helper on the scheduler — the route module and the scheduler must
# stay in lock-step or scheduled schema-rule runs would silently regress.
def test_scheduler_schema_target_helper_matches_route_helper() -> None:
    checks = [_schema_check()]
    assert SchedulerService._extract_schema_check_target(checks) == _extract_schema_check_target(checks)


def test_scheduler_sql_query_helper_matches_route_helper() -> None:
    checks = [_sql_check()]
    assert SchedulerService._extract_sql_query(checks) == _extract_sql_query(checks)
