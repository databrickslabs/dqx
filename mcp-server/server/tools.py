import logging
import os

from server import utils

logger = logging.getLogger(__name__)


def _get_tmp_view_config() -> tuple[str, str]:
    """Get catalog and schema for temporary views from environment."""
    catalog = os.environ.get("DQX_CATALOG", "")
    schema = os.environ.get("DQX_TMP_SCHEMA", "tmp")
    if not catalog:
        raise RuntimeError("DQX_CATALOG not set. Deploy the bundle first.")
    return catalog, schema


def load_tools(mcp_server):
    """Register all DQX MCP tools with the server.

    All DQX operations run in a notebook job — the app has no pyspark dependency.
    Tools that access tables create a temporary view via the user's OBO token
    (enforcing UC governance), then the SP job reads through the view.
    """

    @mcp_server.tool
    def get_table_schema(table_name: str):
        """Retrieve the schema and basic metadata for a Databricks table.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').

        Returns a dict with:
            - 'table_name': the input table name
            - 'columns': list of {name, type, comment} for each column
        """
        logger.info(f"Getting schema for table: {table_name}")
        obo_ws = utils.get_obo_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)

        rows = utils.execute_sql(
            obo_ws,
            f"DESCRIBE TABLE {table_name}",
            warehouse_id=warehouse_id,
        )

        columns = [
            {"name": row["col_name"], "type": row["data_type"], "comment": row.get("comment", "")}
            for row in rows
            if row.get("col_name") and not row["col_name"].startswith("#")
        ]

        return {"table_name": table_name, "columns": columns}

    @mcp_server.tool
    def profile_table(
        table_name: str,
        columns: list[str] | None = None,
        options: dict | None = None,
    ):
        """Profile a Databricks table to get summary statistics and auto-generated data quality profiles.

        The profiles describe patterns found in the data (nullability, value ranges, allowed
        values, uniqueness, etc.) and can be fed directly into `generate_rules`.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            columns: Optional list of column names to profile. Profiles all columns if omitted.
            options: Optional profiling options.

        Returns a dict with:
            - 'table_name': the input table name
            - 'summary_stats': per-column summary statistics
            - 'profiles': list of profile dicts
        """
        logger.info(f"Profiling table: {table_name}")
        obo_ws = utils.get_obo_client()
        sp_ws = utils._get_sp_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)
        try:
            result = utils.submit_notebook_job("profile_table", {
                "view_name": view_fqn,
                "columns": columns,
                "options": options,
            })
            result["table_name"] = table_name
            return result
        finally:
            utils.drop_view(sp_ws, view_fqn, warehouse_id=warehouse_id)

    @mcp_server.tool
    def generate_rules(profiles: list[dict], criticality: str = "error"):
        """Generate DQX data quality check definitions from profiling output.

        Args:
            profiles: List of profile dicts from `profile_table`.
            criticality: Default criticality: 'error' or 'warn' (default 'error').

        Returns a dict with:
            - 'rules': list of DQX check definitions (metadata format)
            - 'count': number of rules generated
        """
        logger.info(f"Generating rules from {len(profiles)} profiles, criticality={criticality}")
        return utils.submit_notebook_job("generate_rules", {
            "profiles": profiles,
            "criticality": criticality,
        })

    @mcp_server.tool
    def validate_checks(checks: list[dict]):
        """Validate a list of DQX data quality check definitions for correctness.

        Returns a dict with 'valid' (bool) and 'errors' (list of error strings).
        """
        logger.info(f"Validating {len(checks)} check(s)")
        return utils.submit_notebook_job("validate_checks", {"checks": checks})

    @mcp_server.tool
    def run_checks(table_name: str, checks: list[dict], sample_size: int = 50):
        """Execute DQX data quality checks against a Databricks table and return results.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            checks: List of DQX check definitions (metadata format).
            sample_size: Max number of invalid rows to include in the sample (default 50).

        Returns a dict with:
            - 'table_name': the input table name
            - 'total_rows', 'valid_rows', 'invalid_rows': row counts
            - 'error_sample': list of dicts, each representing an invalid row
            - 'rule_summary': per-rule counts of errors and warnings
        """
        logger.info(f"Running {len(checks)} checks on table: {table_name}")
        obo_ws = utils.get_obo_client()
        sp_ws = utils._get_sp_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)
        try:
            result = utils.submit_notebook_job("run_checks", {
                "view_name": view_fqn,
                "checks": checks,
                "sample_size": sample_size,
            })
            result["table_name"] = table_name
            return result
        finally:
            utils.drop_view(sp_ws, view_fqn, warehouse_id=warehouse_id)

    @mcp_server.tool
    def list_available_checks():
        """List all built-in DQX check functions available for use in rules.

        Returns a dict with:
            - 'checks': list of {name, type, signature, description} for each registered function.
            - 'count': total number of available check functions.
        """
        logger.info("Listing available check functions")
        return utils.submit_notebook_job("list_available_checks", {})

    @mcp_server.tool
    def get_workflow():
        """Get the recommended workflow for running DQX data quality checks on a table.

        Call this tool FIRST to understand the correct sequence of tool calls.
        """
        return {
            "description": "DQX data quality workflow for profiling a table, generating rules, and running checks.",
            "steps": [
                {
                    "step": 1,
                    "tool": "get_table_schema",
                    "purpose": "Understand the table structure before profiling.",
                    "required_input": {"table_name": "Fully qualified table name (e.g. 'catalog.schema.table')"},
                    "output": "Column names, types, and comments.",
                    "optional": False,
                },
                {
                    "step": 2,
                    "tool": "profile_table",
                    "purpose": "Profile the table data to discover patterns.",
                    "required_input": {"table_name": "Same table name from step 1"},
                    "output": "Summary statistics and a list of 'profiles' to feed into step 3.",
                    "optional": False,
                },
                {
                    "step": 3,
                    "tool": "generate_rules",
                    "purpose": "Convert profiling output into DQX check rule definitions.",
                    "required_input": {"profiles": "The 'profiles' list from step 2 output"},
                    "output": "A list of 'rules' (check definitions) to feed into steps 4 and 5.",
                    "optional": False,
                },
                {
                    "step": 4,
                    "tool": "validate_checks",
                    "purpose": "Validate that the rule definitions are correct before running them.",
                    "required_input": {"checks": "The 'rules' list from step 3 output"},
                    "output": "Whether rules are valid and any error messages.",
                    "optional": True,
                },
                {
                    "step": 5,
                    "tool": "run_checks",
                    "purpose": "Execute the rules against the table and get data quality results.",
                    "required_input": {
                        "table_name": "Same table name from step 1",
                        "checks": "The validated 'rules' from step 3",
                    },
                    "output": "Total/valid/invalid row counts, per-rule summary, and a sample of failing rows.",
                    "optional": False,
                },
            ],
            "helper_tools": [
                {"tool": "list_available_checks", "purpose": "Discover all 68+ built-in check functions."},
            ],
            "notes": [
                "All data tools require a real Unity Catalog table name.",
                "You can modify the generated rules between steps 3 and 5.",
                "Re-run validate_checks after any manual edits to rules.",
            ],
        }
