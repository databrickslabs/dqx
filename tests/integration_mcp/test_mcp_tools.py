"""Deterministic, protocol-level integration tests for every DQX MCP tool.

Unlike ``test_mcp_agent.py`` (which drives the server through an LLM and asserts on the tool-use
trajectory), these call each tool directly over the MCP protocol against the deployed app and
assert on concrete results — so they cover all 12 tools without LLM nondeterminism. They reuse
the session-scoped ``deployed_mcp`` app and the ``demo_data`` sample table (see conftest.py),
and run the same way locally and in CI.

The flow mirrors the documented workflow: discover → describe → profile → generate (from a
profile and from a contract) → validate → run → save → load → apply. ``run_checks`` / ``apply``
use a small set of explicit, error-level rules that target the known-dirty rows so failure
counts are exact (profiler-generated rules learn from the data and would mostly pass it).
"""

# The 10-row sample table (conftest._CUSTOMERS_ROWS) has exactly these error-level failures:
#   - customer_id NULL ....... 1 row  (is_not_null)
#   - age outside [0, 120] ... 2 rows (-3 and 210)
#   - name NULL .............. 1 row  (is_not_null_and_not_empty)
# Across 4 distinct rows, so 4 invalid / 6 valid out of 10.
EXPLICIT_CHECKS = [
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "customer_id"}}},
    {
        "criticality": "error",
        "check": {"function": "is_in_range", "arguments": {"column": "age", "min_limit": 0, "max_limit": 120}},
    },
    {"criticality": "error", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}}},
]
EXPECTED_TOTAL_ROWS = 10
EXPECTED_INVALID_ROWS = 4

ALL_TOOLS = {
    "get_workflow",
    "get_table_schema",
    "profile_table",
    "generate_rules",
    "generate_rules_from_contract",
    "list_available_checks",
    "validate_checks",
    "run_checks",
    "save_checks",
    "load_checks",
    "apply_checks_and_save_to_table",
    "get_run_result",
}


# --- discovery / synchronous tools (no sample data needed) -----------------------------------


def test_tools_list_exposes_all_tools(mcp):
    names = {t["name"] for t in mcp.list_tools()}
    missing = ALL_TOOLS - names
    assert not missing, f"missing tools: {missing}"


def test_get_workflow_returns_steps(mcp):
    workflow = mcp.call("get_workflow")
    assert workflow.get("steps"), "get_workflow should describe the recommended steps"
    assert any(step.get("tool") == "get_table_schema" for step in workflow["steps"])


def test_list_available_checks_includes_builtins(mcp):
    result = mcp.call("list_available_checks")
    assert result["count"] > 0
    names = {c["name"] for c in result["checks"]}
    assert {"is_not_null", "is_in_range"} <= names


def test_validate_checks_accepts_valid_and_rejects_invalid(mcp):
    assert mcp.call("validate_checks", {"checks": EXPLICIT_CHECKS})["valid"] is True

    bad = [{"criticality": "error", "check": {"function": "not_a_real_check", "arguments": {}}}]
    invalid = mcp.call("validate_checks", {"checks": bad})
    assert invalid["valid"] is False
    assert invalid["errors"], "an unknown check function should produce validation errors"


# --- data-backed tools (use the shared dirty 'customers' table) ------------------------------


def test_get_table_schema_lists_columns(mcp, demo_data):
    schema = mcp.call("get_table_schema", {"table_name": demo_data["table"]})
    columns = {c["name"] for c in schema["columns"]}
    assert {"customer_id", "name", "email", "age", "country", "signup_date", "amount"} <= columns


def test_profile_then_generate_rules(mcp, demo_data):
    profile = mcp.call("profile_table", {"table_name": demo_data["table"], "options": {"sample_fraction": 1.0}})
    assert profile["profiles"], "profiling should return per-column profiles"

    generated = mcp.call("generate_rules", {"profiles": profile["profiles"], "criticality": "error"})
    assert generated["count"] > 0
    # Generated rules must themselves be valid DQX metadata.
    assert mcp.call("validate_checks", {"checks": generated["rules"]})["valid"] is True


def test_generate_rules_from_contract(mcp, demo_data):
    generated = mcp.call("generate_rules_from_contract", {"contract_file": demo_data["contract"]})
    assert generated["count"] > 0
    assert mcp.call("validate_checks", {"checks": generated["rules"]})["valid"] is True


def test_run_checks_flags_the_dirty_rows(mcp, demo_data):
    result = mcp.call("run_checks", {"table_name": demo_data["table"], "checks": EXPLICIT_CHECKS})
    assert result["total_rows"] == EXPECTED_TOTAL_ROWS
    assert result["invalid_rows"] == EXPECTED_INVALID_ROWS
    assert result["valid_rows"] == EXPECTED_TOTAL_ROWS - EXPECTED_INVALID_ROWS
    assert result["error_sample"], "failing rows should be sampled"
    assert result["rule_summary"], "per-rule error counts should be reported"


def test_save_then_load_checks_round_trip(mcp, demo_data):
    saved = mcp.call(
        "save_checks",
        {"checks": EXPLICIT_CHECKS, "location": demo_data["checks_table"], "mode": "overwrite"},
    )
    assert saved["saved"] is True
    assert saved["count"] == len(EXPLICIT_CHECKS)

    loaded = mcp.call("load_checks", {"location": demo_data["checks_table"]})
    assert loaded["count"] == len(EXPLICIT_CHECKS)
    loaded_functions = {c["check"]["function"] for c in loaded["checks"]}
    assert loaded_functions == {c["check"]["function"] for c in EXPLICIT_CHECKS}


def test_apply_checks_writes_clean_and_quarantine(mcp, demo_data):
    result = mcp.call(
        "apply_checks_and_save_to_table",
        {
            "table_name": demo_data["table"],
            "checks": EXPLICIT_CHECKS,
            "output_table": demo_data["clean_table"],
            "quarantine_table": demo_data["quarantine_table"],
            "mode": "overwrite",
        },
    )
    assert result["quarantine_rows"] == EXPECTED_INVALID_ROWS
    assert result["output_rows"] == EXPECTED_TOTAL_ROWS - EXPECTED_INVALID_ROWS
    assert result["output_rows"] + result["quarantine_rows"] == EXPECTED_TOTAL_ROWS
