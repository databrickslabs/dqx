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

    Long-running tools (profile_table, run_checks, generate_rules, etc.) return
    a run_id immediately. Use get_run_result to poll for results.
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

        safe_table = utils.validate_and_quote_table_name(table_name)
        rows = utils.execute_sql(
            obo_ws,
            f"DESCRIBE TABLE {safe_table}",
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

        This tool submits a profiling job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            columns: Optional list of column names to profile. Profiles all columns if omitted.
            options: Optional profiling options.

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Profiling table: {table_name}")
        obo_ws = utils.get_obo_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)

        run_id = utils.submit_job_async(
            "profile_table",
            {"view_name": view_fqn, "columns": columns, "options": options},
            metadata={"view_fqn": view_fqn, "warehouse_id": warehouse_id, "table_name": table_name},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Profiling job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def generate_rules(profiles: list[dict], criticality: str = "error"):
        """Generate DQX data quality check definitions from profiling output.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            profiles: List of profile dicts from profile_table result.
            criticality: Default criticality: 'error' or 'warn' (default 'error').

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Generating rules from {len(profiles)} profiles, criticality={criticality}")

        run_id = utils.submit_job_async(
            "generate_rules",
            {"profiles": profiles, "criticality": criticality},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Rule generation job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def validate_checks(checks: list[dict]):
        """Validate a list of DQX data quality check definitions for correctness.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Validating {len(checks)} check(s)")

        run_id = utils.submit_job_async(
            "validate_checks",
            {"checks": checks},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Validation job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def run_checks(table_name: str, checks: list[dict], sample_size: int = 50):
        """Execute DQX data quality checks against a Databricks table.

        This tool submits a check job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            checks: List of DQX check definitions (metadata format).
            sample_size: Max number of invalid rows to include in the sample (default 50).

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Running {len(checks)} checks on table: {table_name}")
        obo_ws = utils.get_obo_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)

        run_id = utils.submit_job_async(
            "run_checks",
            {"view_name": view_fqn, "checks": checks, "sample_size": sample_size},
            metadata={"view_fqn": view_fqn, "warehouse_id": warehouse_id, "table_name": table_name},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Check job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def get_run_result(run_id: int):
        """Check the status of a submitted job and retrieve results when complete.

        Call this after profile_table, generate_rules, validate_checks, or run_checks.
        If the job is still running, call this tool again after a short wait.

        Args:
            run_id: The run_id returned by a prior tool call.

        Returns a dict with:
            - 'status': 'running', 'completed', or 'failed'
            - 'run_id': the run ID
            - 'result': the operation result (only when status is 'completed')
            - 'error': error message (only when status is 'failed')
        """
        logger.info(f"Checking run result: run_id={run_id}")
        return utils.get_run_status(run_id)

    @mcp_server.tool
    def list_available_checks():
        """List all built-in DQX check functions available for use in rules.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info("Listing available check functions")

        run_id = utils.submit_job_async(
            "list_available_checks",
            {},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def get_workflow():
        """Get the recommended workflow for running DQX data quality checks on a table.

        Call this tool FIRST to understand the correct sequence of tool calls.

        IMPORTANT: Most tools return a run_id immediately instead of blocking.
        After calling a tool, use get_run_result(run_id) to poll for results.
        If status is 'running', wait a moment and call get_run_result again.
        """
        return {
            "description": "DQX data quality workflow for profiling a table, generating rules, and running checks.",
            "async_pattern": "Most tools submit a job and return a run_id immediately. Call get_run_result(run_id) to poll for results. If status is 'running', wait and call get_run_result again.",
            "steps": [
                {
                    "step": 1,
                    "tool": "get_table_schema",
                    "purpose": "Understand the table structure before profiling.",
                    "required_input": {"table_name": "Fully qualified table name (e.g. 'catalog.schema.table')"},
                    "output": "Column names, types, and comments.",
                    "async": False,
                },
                {
                    "step": 2,
                    "tool": "profile_table",
                    "purpose": "Profile the table data to discover patterns.",
                    "required_input": {"table_name": "Same table name from step 1"},
                    "output": "Returns run_id. Call get_run_result(run_id) to get summary statistics and profiles.",
                    "async": True,
                },
                {
                    "step": 3,
                    "tool": "generate_rules",
                    "purpose": "Convert profiling output into DQX check rule definitions.",
                    "required_input": {"profiles": "The 'profiles' list from step 2 result"},
                    "output": "Returns run_id. Call get_run_result(run_id) to get the rules.",
                    "async": True,
                },
                {
                    "step": 4,
                    "tool": "validate_checks",
                    "purpose": "Validate that the rule definitions are correct before running them.",
                    "required_input": {"checks": "The 'rules' list from step 3 result"},
                    "output": "Returns run_id. Call get_run_result(run_id) to get validation status.",
                    "async": True,
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
                    "output": "Returns run_id. Call get_run_result(run_id) to get row counts, per-rule summary, and failing rows.",
                    "async": True,
                },
            ],
            "helper_tools": [
                {"tool": "get_run_result", "purpose": "Poll for results of any async tool call. Pass the run_id."},
                {"tool": "list_available_checks", "purpose": "Discover all 68+ built-in check functions (async)."},
            ],
            "notes": [
                "All data tools require a real Unity Catalog table name.",
                "You can modify the generated rules between steps 3 and 5.",
                "Re-run validate_checks after any manual edits to rules.",
            ],
        }
