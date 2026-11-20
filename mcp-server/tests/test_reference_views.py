"""Unit tests for reference-table temp-view exposure (no Spark, no workspace)."""

from typing import Any

import pytest
from server.reference_views import (
    build_reference_views,
    collect_reference_tables,
    rewrite_reference_tables,
)


def _fk_check(ref_table: str, name: str = "fk") -> dict[str, Any]:
    return {
        "name": name,
        "criticality": "error",
        "check": {
            "function": "foreign_key",
            "arguments": {"columns": ["cust_id"], "ref_table": ref_table, "ref_columns": ["id"]},
        },
    }


def test_collect_dedupes_and_preserves_order() -> None:
    checks = [
        _fk_check("cat.sch.customers", "a"),
        _fk_check("cat.sch.regions", "b"),
        _fk_check("cat.sch.customers", "c"),  # duplicate
    ]
    assert collect_reference_tables(checks) == ["cat.sch.customers", "cat.sch.regions"]


def test_collect_ignores_checks_without_ref_table() -> None:
    checks: list[dict[str, Any]] = [
        {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        {"check": {"function": "compare_datasets", "arguments": {"ref_df_name": "ref"}}},  # ref_df_name, not ref_table
        {"check": {"function": "foreign_key", "arguments": {"ref_table": "  "}}},  # blank
        {"check": {"function": "has_valid_schema"}},  # no arguments
        {"not_a_check": True},  # malformed
    ]
    assert collect_reference_tables(checks) == []


def test_rewrite_swaps_and_does_not_mutate_input() -> None:
    checks = [_fk_check("cat.sch.customers")]
    rewritten = rewrite_reference_tables(checks, {"cat.sch.customers": "cat.tmp.v_abc"})
    assert rewritten[0]["check"]["arguments"]["ref_table"] == "cat.tmp.v_abc"
    assert checks[0]["check"]["arguments"]["ref_table"] == "cat.sch.customers"  # input untouched


def test_rewrite_leaves_unmapped_refs_unchanged() -> None:
    checks = [_fk_check("cat.sch.customers"), _fk_check("cat.sch.regions", "b")]
    rewritten = rewrite_reference_tables(checks, {"cat.sch.customers": "cat.tmp.v1"})
    assert rewritten[0]["check"]["arguments"]["ref_table"] == "cat.tmp.v1"
    assert rewritten[1]["check"]["arguments"]["ref_table"] == "cat.sch.regions"


def test_build_creates_views_and_rewrites() -> None:
    requested: list[str] = []

    def create_view(ref: str) -> str:
        view = f"dqx_mcp.dqx_mcp_tmp.v_{len(requested)}"
        requested.append(ref)
        return view

    checks = [_fk_check("cat.sch.customers", "a"), _fk_check("cat.sch.regions", "b")]
    result = build_reference_views(checks, create_view)

    assert requested == ["cat.sch.customers", "cat.sch.regions"]  # one view per distinct ref table
    assert result.created_view_fqns == ["dqx_mcp.dqx_mcp_tmp.v_0", "dqx_mcp.dqx_mcp_tmp.v_1"]
    assert result.checks[0]["check"]["arguments"]["ref_table"] == "dqx_mcp.dqx_mcp_tmp.v_0"
    assert result.checks[1]["check"]["arguments"]["ref_table"] == "dqx_mcp.dqx_mcp_tmp.v_1"


def test_build_is_noop_without_reference_tables() -> None:
    def create_view(ref: str) -> str:
        raise AssertionError("no reference tables -> no view creation")

    checks = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    result = build_reference_views(checks, create_view)

    assert result.created_view_fqns == []
    assert result.checks is checks  # untouched, same object


def test_build_reuses_monitored_view_for_self_reference() -> None:
    def create_view(ref: str) -> str:
        raise AssertionError("self-reference must reuse the monitored view, not create a new one")

    checks = [_fk_check("cat.sch.orders")]  # references the monitored table itself
    result = build_reference_views(
        checks,
        create_view,
        monitored_table_fqn="cat.sch.orders",
        monitored_view_fqn="dqx_mcp.dqx_mcp_tmp.v_monitored",
    )

    assert result.created_view_fqns == []
    assert result.checks[0]["check"]["arguments"]["ref_table"] == "dqx_mcp.dqx_mcp_tmp.v_monitored"


def test_build_drops_created_views_when_a_later_creation_fails() -> None:
    dropped: list[str] = []

    def create_view(ref: str) -> str:
        if ref == "cat.sch.regions":
            raise RuntimeError("no access")
        return f"view_of_{ref}"

    checks = [_fk_check("cat.sch.customers", "a"), _fk_check("cat.sch.regions", "b")]

    with pytest.raises(RuntimeError, match="no access"):
        build_reference_views(checks, create_view, drop_view=dropped.append)

    assert dropped == ["view_of_cat.sch.customers"]  # the first (successful) view is cleaned up
