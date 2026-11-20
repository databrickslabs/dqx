"""Unit tests for reference-table temp-view exposure (no Spark, no workspace)."""

from typing import Any

import pytest
from server.reference_views import (
    build_reference_views,
    collect_reference_tables,
    rewrite_to_ref_df_name,
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
        {"check": {"function": "compare_datasets", "arguments": {"ref_df_name": "ref"}}},  # already a ref_df_name
        {"check": {"function": "foreign_key", "arguments": {"ref_table": "  "}}},  # blank
        {"check": {"function": "has_valid_schema"}},  # no arguments
        {"not_a_check": True},  # malformed
    ]
    assert collect_reference_tables(checks) == []


def test_rewrite_swaps_ref_table_to_ref_df_name_without_mutating_input() -> None:
    checks = [_fk_check("cat.sch.customers")]
    rewritten = rewrite_to_ref_df_name(checks, {"cat.sch.customers"})

    args = rewritten[0]["check"]["arguments"]
    assert "ref_table" not in args  # swapped out (DQX rejects both together)
    assert args["ref_df_name"] == "cat.sch.customers"  # stable original name
    # Input untouched.
    assert checks[0]["check"]["arguments"]["ref_table"] == "cat.sch.customers"
    assert "ref_df_name" not in checks[0]["check"]["arguments"]


def test_rewrite_leaves_unhandled_refs_unchanged() -> None:
    checks = [_fk_check("cat.sch.customers"), _fk_check("cat.sch.regions", "b")]
    rewritten = rewrite_to_ref_df_name(checks, {"cat.sch.customers"})
    assert rewritten[0]["check"]["arguments"]["ref_df_name"] == "cat.sch.customers"
    assert rewritten[1]["check"]["arguments"]["ref_table"] == "cat.sch.regions"  # not handled
    assert "ref_df_name" not in rewritten[1]["check"]["arguments"]


def test_build_creates_views_and_rewrites_to_ref_df_name() -> None:
    requested: list[str] = []

    def create_view(ref: str) -> str:
        view = f"dqx_mcp.dqx_mcp_tmp.v_{len(requested)}"
        requested.append(ref)
        return view

    checks = [_fk_check("cat.sch.customers", "a"), _fk_check("cat.sch.regions", "b")]
    result = build_reference_views(checks, create_view)

    assert requested == ["cat.sch.customers", "cat.sch.regions"]  # one view per distinct ref table
    assert result.ref_views == {
        "cat.sch.customers": "dqx_mcp.dqx_mcp_tmp.v_0",
        "cat.sch.regions": "dqx_mcp.dqx_mcp_tmp.v_1",
    }
    assert result.created_view_fqns == ["dqx_mcp.dqx_mcp_tmp.v_0", "dqx_mcp.dqx_mcp_tmp.v_1"]
    # Checks now carry the ORIGINAL table name as ref_df_name, not the view FQN.
    assert result.checks[0]["check"]["arguments"]["ref_df_name"] == "cat.sch.customers"
    assert result.checks[1]["check"]["arguments"]["ref_df_name"] == "cat.sch.regions"
    assert all("ref_table" not in c["check"]["arguments"] for c in result.checks)


def test_build_is_noop_without_reference_tables() -> None:
    def create_view(ref: str) -> str:
        raise AssertionError("no reference tables -> no view creation")

    checks = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    result = build_reference_views(checks, create_view)

    assert result.ref_views == {}
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

    assert result.created_view_fqns == []  # nothing newly created
    assert result.ref_views == {"cat.sch.orders": "dqx_mcp.dqx_mcp_tmp.v_monitored"}
    assert result.checks[0]["check"]["arguments"]["ref_df_name"] == "cat.sch.orders"


def test_build_raises_clear_error_naming_table_and_drops_created_on_failure() -> None:
    dropped: list[str] = []

    def create_view(ref: str) -> str:
        if ref == "cat.sch.regions":
            raise RuntimeError("PERMISSION_DENIED")
        return f"view_of_{ref}"

    checks = [_fk_check("cat.sch.customers", "a"), _fk_check("cat.sch.regions", "b")]

    with pytest.raises(RuntimeError, match=r"cat\.sch\.regions"):
        build_reference_views(checks, create_view, drop_view=dropped.append)

    assert dropped == ["view_of_cat.sch.customers"]  # the first (successful) view is cleaned up
