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
        logger.info(f"Getting schema for table: {utils.sanitize_for_log(table_name)}")
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
        logger.info(f"Profiling table: {utils.sanitize_for_log(table_name)}")
        obo_ws = utils.get_obo_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)

        run_id = utils.submit_job_async(
            "profile_table",
            {"view_name": view_fqn, "table_name": table_name, "columns": columns, "options": options},
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
    def generate_rules_from_contract(
        contract_file: str,
        contract_format: str = "odcs",
        default_criticality: str = "error",
        process_text_rules: bool = False,
    ):
        """Generate DQX check definitions from a data contract (ODCS).

        Derives checks from the contract's schema and quality expectations — a
        deterministic alternative to profiling when a data contract already exists.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            contract_file: Path to the contract file (workspace, UC volume, or local).
            contract_format: Contract format (default 'odcs').
            default_criticality: Default criticality for generated rules: 'error' or 'warn'.
            process_text_rules: If True, also derive rules from free-text expectations via
                the LLM (requires the [llm] extra). Default False keeps generation deterministic.

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Generating rules from contract: {utils.sanitize_for_log(contract_file)}")

        run_id = utils.submit_job_async(
            "generate_rules_from_contract",
            {
                "contract_file": contract_file,
                "contract_format": contract_format,
                "default_criticality": default_criticality,
                "process_text_rules": process_text_rules,
            },
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Contract rule generation job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def load_checks(location: str, run_config_name: str = "default"):
        """Load saved DQX checks from a storage backend.

        Use this to retrieve previously saved checks. The backend is inferred from
        the location: a 'catalog.schema.table' name is a Delta table, a '/Volumes/...'
        path is a UC volume file, any other '/...' path is a workspace file.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            location: Table name or file path where the checks are stored.
            run_config_name: Run configuration name to filter by (table backends only).

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Loading checks from: {utils.sanitize_for_log(location)}")

        run_id = utils.submit_job_async(
            "load_checks",
            {"location": location, "run_config_name": run_config_name},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Load checks job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def save_checks(
        checks: list[dict],
        location: str,
        run_config_name: str = "default",
        mode: str = "append",
    ):
        """Save DQX checks to a storage backend for later reuse.

        The backend is inferred from the location: a 'catalog.schema.table' name is a
        Delta table, a '/Volumes/...' path is a UC volume file, any other '/...' path is
        a workspace file. Writes run as the app service principal, so it must have write
        access to the target location.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            checks: List of DQX check definitions (metadata format) to save.
            location: Table name or file path to save the checks to.
            run_config_name: Run configuration name to tag the checks with (table backends only).
            mode: Write mode for table backends, 'append' or 'overwrite' (ignored for files).

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(f"Saving {len(checks)} checks to: {utils.sanitize_for_log(location)}")

        run_id = utils.submit_job_async(
            "save_checks",
            {"checks": checks, "location": location, "run_config_name": run_config_name, "mode": mode},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Save checks job submitted. Call get_run_result with this run_id to get results.",
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
        logger.info(f"Running {len(checks)} checks on table: {utils.sanitize_for_log(table_name)}")
        obo_ws = utils.get_obo_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)

        run_id = utils.submit_job_async(
            "run_checks",
            {"view_name": view_fqn, "table_name": table_name, "checks": checks, "sample_size": sample_size},
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Check job submitted. Call get_run_result with this run_id to get results.",
        }

    @mcp_server.tool
    def apply_checks_and_save_to_table(
        table_name: str,
        checks: list[dict],
        output_table: str,
        quarantine_table: str | None = None,
        mode: str = "append",
    ):
        """Apply DQX checks to a table and persist the results to output table(s).

        Unlike run_checks (which returns a sample), this operationalizes the checks:
        results are written to Delta tables. If quarantine_table is given, valid rows go
        to output_table and invalid rows to quarantine_table; otherwise all rows (with
        _errors/_warnings columns) go to output_table. Writes run as the app service
        principal, which must have write access to the target schema.

        This tool submits a job and returns a run_id immediately.
        Call get_run_result with the run_id to check status and retrieve results.

        Args:
            table_name: Fully qualified source table name (e.g. 'catalog.schema.table').
            checks: List of DQX check definitions (metadata format).
            output_table: Fully qualified table to write results to.
            quarantine_table: Optional fully qualified table for invalid rows.
            mode: Write mode, 'append' or 'overwrite' (default 'append').

        Returns a dict with:
            - 'status': 'submitted'
            - 'run_id': job run ID to pass to get_run_result
        """
        logger.info(
            f"Applying {len(checks)} checks on {utils.sanitize_for_log(table_name)} "
            f"-> {utils.sanitize_for_log(output_table)}"
        )
        obo_ws = utils.get_obo_client()
        warehouse_id = utils.get_warehouse_id(obo_ws)
        catalog, schema = _get_tmp_view_config()

        view_fqn = utils.create_temp_view(obo_ws, table_name, catalog, schema, warehouse_id)

        run_id = utils.submit_job_async(
            "apply_checks_and_save_to_table",
            {
                "view_name": view_fqn,
                "table_name": table_name,
                "checks": checks,
                "output_table": output_table,
                "quarantine_table": quarantine_table,
                "mode": mode,
            },
        )

        return {
            "status": "submitted",
            "run_id": run_id,
            "message": "Apply-and-save job submitted. Call get_run_result with this run_id to get results.",
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
                {
                    "tool": "generate_rules_from_contract",
                    "purpose": (
                        "Alternative to steps 2-3: if a data contract (ODCS) exists, generate checks "
                        "directly from it. Output 'rules' feeds into validate_checks and run_checks."
                    ),
                    "async": True,
                },
                {
                    "tool": "save_checks",
                    "purpose": (
                        "Persist a validated rule set to a table, UC volume, or workspace file so DQX "
                        "pipelines (or a later session) can reuse it."
                    ),
                    "async": True,
                },
                {
                    "tool": "load_checks",
                    "purpose": "Retrieve a previously saved rule set by location; output feeds run_checks.",
                    "async": True,
                },
                {
                    "tool": "apply_checks_and_save_to_table",
                    "purpose": (
                        "Operationalized alternative to run_checks (step 5): write valid/quarantine rows "
                        "to Delta tables instead of returning a sample."
                    ),
                    "async": True,
                },
            ],
            "notes": [
                "All data tools require a real Unity Catalog table name.",
                "You can modify the generated rules between steps 3 and 5.",
                "Re-run validate_checks after any manual edits to rules.",
                "Rules can come from profiling (steps 2-3) or a data contract "
                "(generate_rules_from_contract); both produce the same format.",
                "To operationalize: save_checks to persist the rule set, and "
                "apply_checks_and_save_to_table to write results to Delta tables.",
            ],
        }
