"""End-to-end test script for the DQX MCP Server tools.

Exercises the full workflow: list checks -> get schema -> profile -> generate -> validate -> run.
Requires a running MCP server and a Databricks workspace with an accessible table.
"""

import json

from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient

TABLE_NAME = "users.nehme_tohme.otel_logs"

ws = WorkspaceClient(profile="e2-demo-west")
client = DatabricksMCPClient(
    server_url="https://dqx-mcp-server-2556758628403379.aws.databricksapps.com/mcp",
    workspace_client=ws,
)


def parse_result(result) -> dict | list:
    """Extract and parse the JSON payload from an MCP tool result."""
    text = result.content[0].text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"ERROR from server: {text}")
        raise SystemExit(1)


# 0. List available tools
tools = client.list_tools()
print("Available tools:", [t.name for t in tools])
print()

# 1. List available check functions
print("=" * 60)
print("STEP 1: list_available_checks")
print("=" * 60)
result = parse_result(client.call_tool("list_available_checks", {}))
print(f"Available checks count: {result['count']}")
print()

# 2. Get table schema
print("=" * 60)
print(f"STEP 2: get_table_schema({TABLE_NAME})")
print("=" * 60)
schema_result = parse_result(client.call_tool("get_table_schema", {"table_name": TABLE_NAME}))
print(f"Columns: {len(schema_result['columns'])}, Rows: {schema_result['row_count']}")
for col in schema_result["columns"]:
    print(f"  - {col['name']}: {col['type']} (nullable={col['nullable']})")
print()

# 3. Profile the table
print("=" * 60)
print(f"STEP 3: profile_table({TABLE_NAME})")
print("=" * 60)
profile_result = parse_result(
    client.call_tool(
        "profile_table",
        {"table_name": TABLE_NAME, "options": {"sample_fraction": 0.1, "limit": 500}},
    )
)
profiles = profile_result["profiles"]
print(f"Generated {len(profiles)} profiles:")
for p in profiles:
    print(f"  - {p['column']}: {p['name']} {p.get('parameters') or ''}")
print()

# 4. Generate rules from profiles
print("=" * 60)
print("STEP 4: generate_rules")
print("=" * 60)
gen_result = parse_result(client.call_tool("generate_rules", {"profiles": profiles, "criticality": "error"}))
rules = gen_result["rules"]
print(f"Generated {gen_result['count']} rules:")
for r in rules:
    func = r.get("check", {}).get("function", "?")
    cols = r.get("check", {}).get("arguments", {}).get("column", "?")
    print(f"  - {r.get('name', '?')}: {func}({cols})")
print()

# 5. Validate the generated rules
print("=" * 60)
print("STEP 5: validate_checks")
print("=" * 60)
val_result = parse_result(client.call_tool("validate_checks", {"checks": rules}))
print(f"Valid: {val_result['valid']}, Errors: {val_result['errors']}")
print()

# 6. Run checks against the table
print("=" * 60)
print(f"STEP 6: run_checks({TABLE_NAME})")
print("=" * 60)
run_result = parse_result(
    client.call_tool("run_checks", {"table_name": TABLE_NAME, "checks": rules, "sample_size": 10})
)
print(f"Total rows:   {run_result['total_rows']}")
print(f"Valid rows:   {run_result['valid_rows']}")
print(f"Invalid rows: {run_result['invalid_rows']}")
if run_result["rule_summary"]:
    print("Rule summary:")
    for s in run_result["rule_summary"]:
        print(f"  - {s['rule_name']}: errors={s['error_count']}, warnings={s['warning_count']}")
if run_result["error_sample"]:
    print(f"Error sample ({len(run_result['error_sample'])} rows):")
    for row in run_result["error_sample"][:3]:
        print(f"  {row}")
