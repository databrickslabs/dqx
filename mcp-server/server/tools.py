import inspect
import logging
from dataclasses import asdict

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.checks_resolver import resolve_check_function

from server import utils

logger = logging.getLogger(__name__)


def load_tools(mcp_server):
    """Register all DQX MCP tools with the server."""

    @mcp_server.tool
    def get_table_schema(table_name: str):
        """Retrieve the schema and basic metadata for a Databricks table.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').

        Returns a dict with:
            - 'table_name': the input table name
            - 'columns': list of {name, type, nullable} for each column
            - 'row_count': approximate number of rows
        """
        logger.info(f"Getting schema for table: {table_name}")
        spark = utils._get_spark()
        df = spark.table(table_name)
        columns = [
            {"name": field.name, "type": str(field.dataType), "nullable": field.nullable} for field in df.schema.fields
        ]
        row_count = df.count()
        result = {
            "table_name": table_name,
            "columns": columns,
            "row_count": row_count,
        }
        logger.info(f"Schema retrieved: {len(columns)} columns, {row_count} rows")
        return result

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
            options: Optional profiling options. Supported keys include:
                - sample_fraction (float): Fraction of data to sample (default 0.3).
                - limit (int): Max rows to profile (default 1000).
                - max_in_count (int): Max distinct values for is_in rules (default 10).
                - distinct_ratio (float): Threshold for is_in rules (default 0.05).
                - max_null_ratio (float): Threshold for is_not_null rules (default 0.01).
                - remove_outliers (bool): Whether to remove outliers for range rules (default True).
                - trim_strings (bool): Whether to trim strings for empty checks (default True).
                - max_empty_ratio (float): Threshold for is_not_null_or_empty rules (default 0.01).

        Returns a dict with:
            - 'table_name': the input table name
            - 'summary_stats': per-column summary statistics
            - 'profiles': list of profile dicts, each with {name, column, description, parameters}
        """
        logger.info(f"Profiling table: {table_name}, columns={columns}, options={options}")
        from databricks.labs.dqx.config import InputConfig

        profiler = utils._get_profiler()
        input_config = InputConfig(location=table_name)
        summary_stats, profiles = profiler.profile_table(input_config, columns=columns, options=options)

        profiles_dicts = [asdict(p) for p in profiles]

        result = {
            "table_name": table_name,
            "summary_stats": utils.make_json_safe(summary_stats),
            "profiles": utils.make_json_safe(profiles_dicts),
        }
        logger.info(f"Profiling complete: {len(profiles)} profiles generated")
        return result

    @mcp_server.tool
    def generate_rules(profiles: list[dict], criticality: str = "error"):
        """Generate DQX data quality check definitions from profiling output.

        Takes the 'profiles' list returned by `profile_table` and converts them into
        fully formed DQX check definitions that can be validated and executed.

        Args:
            profiles: List of profile dicts from `profile_table`, each with keys
                {name, column, description, parameters, filter}.
            criticality: Default criticality for generated rules: 'error' or 'warn' (default 'error').

        Returns a dict with:
            - 'rules': list of DQX check definitions (metadata format)
            - 'count': number of rules generated

        Example profile input:
        [
            {"name": "is_not_null", "column": "id", "description": null, "parameters": null, "filter": null},
            {"name": "min_max", "column": "amount", "description": null, "parameters": {"min": 0, "max": 1000}, "filter": null}
        ]
        """
        logger.info(f"Generating rules from {len(profiles)} profiles, criticality={criticality}")
        from databricks.labs.dqx.profiler.profiler import DQProfile

        dq_profiles = [
            DQProfile(
                name=p["name"],
                column=p["column"],
                description=p.get("description"),
                parameters=p.get("parameters"),
                filter=p.get("filter"),
            )
            for p in profiles
        ]

        generator = utils._get_generator()
        rules = generator.generate_dq_rules(dq_profiles, criticality=criticality)

        result = {
            "rules": utils.make_json_safe(rules),
            "count": len(rules),
        }
        logger.info(f"Generated {len(rules)} rules")
        return result

    @mcp_server.tool
    def validate_checks(checks: list[dict]):
        """Validate a list of DQX data quality check definitions for correctness.

        Each check should be a dictionary with at minimum a 'check' key containing
        'function' (the check function name) and 'arguments' (dict of arguments).

        Example input:
        [
            {
                "check": {"function": "is_not_null", "arguments": {"column": "id"}},
                "criticality": "error",
                "name": "id_not_null"
            }
        ]

        Returns a dict with 'valid' (bool) and 'errors' (list of error strings).
        """
        logger.info(f"Validating {len(checks)} check(s)")
        status = DQEngine.validate_checks(checks)
        result = {
            "valid": not status.has_errors,
            "errors": status.errors,
        }
        logger.info(f"Validation result: valid={result['valid']}, errors={len(result['errors'])}")
        return result

    @mcp_server.tool
    def run_checks(table_name: str, checks: list[dict], sample_size: int = 50):
        """Execute DQX data quality checks against a Databricks table and return results.

        Applies the given check definitions to the table and returns a summary of
        valid/invalid rows plus a sample of failing rows for inspection.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            checks: List of DQX check definitions (metadata format), as returned by
                `generate_rules` or written manually.
            sample_size: Max number of invalid rows to include in the sample (default 50).

        Returns a dict with:
            - 'table_name': the input table name
            - 'total_rows': total number of rows in the table
            - 'valid_rows': number of rows passing all checks
            - 'invalid_rows': number of rows failing at least one check
            - 'error_sample': list of dicts, each representing an invalid row with
                its '_errors' and '_warnings' columns
            - 'rule_summary': per-rule counts of errors and warnings
        """
        logger.info(f"Running {len(checks)} checks on table: {table_name}")
        spark = utils._get_spark()
        engine = utils._get_engine()

        df = spark.table(table_name)
        valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)

        total_rows = df.count()
        valid_rows = valid_df.count()
        invalid_rows = invalid_df.count()

        error_sample_rows = invalid_df.limit(sample_size).collect()
        error_sample = [utils.make_json_safe(row.asDict(recursive=True)) for row in error_sample_rows]

        rule_summary = utils.compute_rule_summary(invalid_df)

        result = {
            "table_name": table_name,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "error_sample": error_sample,
            "rule_summary": rule_summary,
        }
        logger.info(f"Check results: total={total_rows}, valid={valid_rows}, invalid={invalid_rows}")
        return result

    @mcp_server.tool
    def list_available_checks():
        """List all built-in DQX check functions available for use in rules.

        Returns a dict with:
            - 'checks': list of {name, type, signature, description} for each registered function.
                'type' is either 'row' (row-level check) or 'dataset' (dataset-level check).
            - 'count': total number of available check functions.
        """
        logger.info("Listing available check functions")
        checks = []
        for name, func_type in sorted(CHECK_FUNC_REGISTRY.items()):
            func = resolve_check_function(name, fail_on_missing=False)
            if func is None:
                continue
            sig = inspect.signature(func)
            params = [
                {"name": p.name, "type": str(p.annotation) if p.annotation != inspect.Parameter.empty else "Any"}
                for p in sig.parameters.values()
            ]
            doc = inspect.getdoc(func) or ""
            first_line = doc.split("\n")[0] if doc else ""
            checks.append(
                {
                    "name": name,
                    "type": func_type,
                    "signature": f"{name}{sig}",
                    "description": first_line,
                    "parameters": params,
                }
            )

        result = {"checks": checks, "count": len(checks)}
        logger.info(f"Found {len(checks)} check functions")
        return result

    @mcp_server.tool
    def get_workflow():
        """Get the recommended workflow for running DQX data quality checks on a table.

        Call this tool FIRST to understand the correct sequence of tool calls.
        Returns the step-by-step workflow with tool names, descriptions, and
        required/optional inputs for each step.
        """
        return {
            "description": "DQX data quality workflow for profiling a table, generating rules, and running checks.",
            "steps": [
                {
                    "step": 1,
                    "tool": "get_table_schema",
                    "purpose": "Understand the table structure before profiling.",
                    "required_input": {"table_name": "Fully qualified table name (e.g. 'catalog.schema.table')"},
                    "output": "Column names, types, nullability, and row count.",
                    "optional": False,
                },
                {
                    "step": 2,
                    "tool": "profile_table",
                    "purpose": "Profile the table data to discover patterns (nullability, value ranges, allowed values, uniqueness).",
                    "required_input": {"table_name": "Same table name from step 1"},
                    "optional_input": {
                        "columns": "List of column names to profile (default: all columns)",
                        "options": "Profiling options like sample_fraction, limit, etc.",
                    },
                    "output": "Summary statistics and a list of 'profiles' to feed into step 3.",
                    "optional": False,
                },
                {
                    "step": 3,
                    "tool": "generate_rules",
                    "purpose": "Convert profiling output into DQX check rule definitions.",
                    "required_input": {"profiles": "The 'profiles' list from step 2 output"},
                    "optional_input": {"criticality": "'error' (default) or 'warn'"},
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
                    "note": "Recommended but optional. Catches errors before execution.",
                },
                {
                    "step": 5,
                    "tool": "run_checks",
                    "purpose": "Execute the rules against the table and get data quality results.",
                    "required_input": {
                        "table_name": "Same table name from step 1",
                        "checks": "The validated 'rules' from step 3",
                    },
                    "optional_input": {"sample_size": "Max invalid rows to return (default: 50)"},
                    "output": "Total/valid/invalid row counts, per-rule summary, and a sample of failing rows.",
                    "optional": False,
                },
            ],
            "helper_tools": [
                {
                    "tool": "list_available_checks",
                    "purpose": "Discover all 68+ built-in check functions. Useful if you want to manually write or modify rules.",
                },
            ],
            "notes": [
                "All tools require a real Unity Catalog table name -- schema-only input is not supported.",
                "You can modify the generated rules between steps 3 and 5 (e.g. change criticality, add custom rules).",
                "Re-run validate_checks after any manual edits to rules.",
            ],
        }
