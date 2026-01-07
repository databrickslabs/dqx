import json
import re
import logging

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.profiler.common import val_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile
from databricks.labs.dqx.telemetry import telemetry_logger
from databricks.labs.dqx.errors import InvalidParameterError, MissingParameterError
from databricks.labs.dqx.config import LLMModelConfig, InputConfig
from databricks.labs.dqx.engine import DQEngine

# Conditional imports for LLM-assisted rules generation
try:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine
    from databricks.labs.dqx.llm.llm_utils import get_column_metadata

    LLM_ENABLED = True
except ImportError:
    LLM_ENABLED = False

__name_sanitize_re__ = re.compile(r"[^a-zA-Z0-9]+")
logger = logging.getLogger(__name__)


class DQDltGenerator(DQEngineBase):
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        llm_model_config: LLMModelConfig | None = None,
    ):
        """
        Initializes the DQDltGenerator with optional Spark session and LLM model configuration.

        Args:
            workspace_client: Databricks WorkspaceClient instance.
            spark: Optional SparkSession instance. If not provided, a new session will be created.
            llm_model_config: Optional LLM model configuration for AI-assisted rule generation.
        """
        super().__init__(workspace_client=workspace_client)
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

        llm_model_config = llm_model_config or LLMModelConfig()
        self.llm_engine = DQLLMEngine(model_config=llm_model_config, spark=self.spark) if LLM_ENABLED else None

    @telemetry_logger("generator", "generate_dlt_rules")
    def generate_dlt_rules(
        self, rules: list[DQProfile], action: str | None = None, language: str = "SQL"
    ) -> list[str] | str | dict:
        """
        Generates Lakeflow Pipelines (formerly Delta Live Table (DLT)) rules in the specified language.

        Args:
            rules: A list of data quality profiles to generate rules for.
            action: The action to take on rule violation (e.g., "drop", "fail").
            language: The language to generate the rules in ("SQL", "Python" or "Python_Dict").

        Returns:
            A list of strings representing the Lakeflow Pipelines rules in SQL, a string representing
            the Lakeflow Pipelines rules in Python, or dictionary with expressions.

        Raises:
            InvalidParameterError: If the specified language is not supported.
        """

        lang = language.lower()

        if lang == "sql":
            return self._generate_dlt_rules_sql(rules, action)

        if lang == "python":
            return self._generate_dlt_rules_python(rules, action)

        if lang == "python_dict":
            return self._generate_dlt_rules_python_dict(rules)

        raise InvalidParameterError(f"Unsupported language '{language}'. Only 'SQL' and 'Python' are supported.")

    @staticmethod
    def _dlt_generate_is_in(column: str, **params: dict):
        """
        Generates a Lakeflow Pipelines (formerly Delta Live Table (DLT)) rule to check if a column's value is in
        a specified list.

        Args:
            column: The name of the column to check.
            params: Additional parameters, including the list of values to check against.

        Returns:
            A string representing the Lakeflow Pipelines rule.
        """
        in_str = ", ".join([val_to_str(v) for v in params["in"]])
        return f"{column} in ({in_str})"

    @staticmethod
    def _dlt_generate_min_max(column: str, **params: dict):
        """
        Generates a Lakeflow Pipelines (formerly Delta Live Table (DLT)) rule to check if a column's value is within
        a specified range.

        Args:
            column: The name of the column to check.
            params: Additional parameters, including the minimum and maximum values.

        Returns:
            A string representing the Lakeflow Pipelines rule.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")
        if min_limit is not None and max_limit is not None:
            # We can generate `col between(min, max)`,
            # but this one is easier to modify if you need to remove some of the bounds
            return f"{column} >= {val_to_str(min_limit)} and {column} <= {val_to_str(max_limit)}"

        if max_limit is not None:
            return f"{column} <= {val_to_str(max_limit)}"

        if min_limit is not None:
            return f"{column} >= {val_to_str(min_limit)}"

        return ""

    @staticmethod
    def _dlt_generate_is_not_null_or_empty(column: str, **params: dict):
        """
        Generates a Lakeflow Pipelines (formerly Delta Live Table (DLT)) rule to check if a column's value is
        not null or empty.

        Args:
            column: The name of the column to check.
            params: Additional parameters, including whether to trim strings.

        Returns:
            A string representing the Lakeflow Pipelines rule.
        """
        trim_strings = params.get("trim_strings", True)
        msg = f"{column} is not null and "
        if trim_strings:
            msg += "trim("
        msg += column
        if trim_strings:
            msg += ")"
        msg += " <> ''"
        return msg

    _checks_mapping = {
        "is_not_null": lambda column, **params: f"{column} is not null",
        "is_in": _dlt_generate_is_in,
        "min_max": _dlt_generate_min_max,
        "is_not_null_or_empty": _dlt_generate_is_not_null_or_empty,
    }

    def _generate_dlt_rules_python_dict(self, rules: list[DQProfile]) -> dict:
        """
        Generates a Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules as Python dictionary.

        Args:
            rules: A list of data quality profiles to generate rules for.

        Returns:
            A dict representing the Lakeflow Pipelines rules in Python.
        """
        expectations = {}
        for rule in rules or []:
            rule_name = rule.name
            column = rule.column
            params = rule.parameters or {}
            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = function_mapping[rule_name](column, **params)
            if expr == "":
                logger.info("Empty expression was generated for rule '{nm}' for column '{cl}'")
                continue
            exp_name = re.sub(__name_sanitize_re__, "_", f"{column}_{rule_name}")
            expectations[exp_name] = expr

        return expectations

    def _generate_dlt_rules_python(self, rules: list[DQProfile], action: str | None = None) -> str:
        """
        Generates a Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules in Python.

        Args:
            rules: A list of data quality profiles to generate rules for.
            action: The action to take on rule violation (e.g., "drop", "fail").

        Returns:
            A string representing the Lakeflow Pipelines rules in Python.
        """
        expectations = self._generate_dlt_rules_python_dict(rules)

        if len(expectations) == 0:
            return ""

        json_expectations = json.dumps(expectations)
        expectations_mapping = {
            "drop": "@dlt.expect_all_or_drop",
            "fail": "@dlt.expect_all_or_fail",
            None: "@dlt.expect_all",
        }
        decorator = expectations_mapping.get(action, "@dlt.expect_all")

        return f"""{decorator}(
{json_expectations}
)"""

    def _generate_dlt_rules_sql(self, rules: list[DQProfile], action: str | None = None) -> list[str]:
        """
        Generates a Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules in sql.

        Args:
            rules: A list of data quality profiles to generate rules for.
            action: The action to take on rule violation (e.g., "drop", "fail").

        Returns:
            A list of Lakeflow Pipelines rules.
        """
        dlt_rules = []
        act_str = ""
        if action == "drop":
            act_str = " ON VIOLATION DROP ROW"
        elif action == "fail":
            act_str = " ON VIOLATION FAIL UPDATE"
        for rule in rules or []:
            rule_name = rule.name
            column = rule.column
            params = rule.parameters or {}
            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = function_mapping[rule_name](column, **params)
            if expr == "":
                logger.info("Empty expression was generated for rule '{nm}' for column '{cl}'")
                continue
            dlt_rule = f"CONSTRAINT {column}_{rule_name} EXPECT ({expr}){act_str}"
            dlt_rules.append(dlt_rule)

        return dlt_rules

    @telemetry_logger("generator", "generate_dlt_rules_ai_assisted")
    def generate_dlt_rules_ai_assisted(
        self,
        user_input: str = "",
        summary_stats: dict | None = None,
        input_config: InputConfig | None = None,
        language: str = "SQL",
    ) -> list[str] | str | dict:
        """
        Generates Lakeflow Pipelines (formerly Delta Live Table (DLT)) rules using LLM based on natural language input
        and/or summary statistics.

        Args:
            user_input: Optional natural language description of data quality requirements.
            summary_stats: Optional summary statistics of the input data.
            input_config: Optional input config providing input data location as a path or fully qualified table name
                to infer schema. If not provided, LLM will be used to guess the table schema.
            language: The language to generate the rules in ("SQL", "Python" or "Python_Dict").

        Returns:
            A list of strings representing the Lakeflow Pipelines rules in SQL, a string representing
            the Lakeflow Pipelines rules in Python, or dictionary with expressions.

        Raises:
            MissingParameterError: If LLM engine is not available or required inputs are missing.
            InvalidParameterError: If the specified language is not supported.

        Example:
            >>> from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
            >>> from databricks.sdk import WorkspaceClient
            >>> from databricks.labs.dqx.profiler.profiler import DQProfiler
            >>>
            >>> ws = WorkspaceClient()
            >>> profiler = DQProfiler(workspace_client=ws, spark=spark)
            >>> input_df = spark.read.table("catalog1.schema1.sales_data")
            >>> summary_stats, profiles = profiler.profile(input_df)
            >>>
            >>> generator = DQDltGenerator(workspace_client=ws, spark=spark)
            >>>
            >>> # Generate DLT rules with user input and summary stats
            >>> dlt_rules = generator.generate_dlt_rules_ai_assisted(
            ...     user_input="Validate sales data for anomalies and business rules",
            ...     summary_stats=summary_stats
            ... )
            >>>
            >>> # Generate DLT rules with summary stats only
            >>> dlt_rules = generator.generate_dlt_rules_ai_assisted(
            ...     summary_stats=summary_stats
            ... )
        """
        if self.llm_engine is None:
            raise MissingParameterError(
                "LLM engine not available. Make sure LLM dependencies are installed: "
                "pip install 'databricks-labs-dqx[llm]'"
            )
        if not summary_stats and not user_input:
            raise MissingParameterError(
                "Either summary statistics or user input must be provided to generate rules using LLM."
            )

        # Validate language early to ensure consistent error handling
        lang = language.lower()
        if lang not in ("sql", "python", "python_dict"):
            raise InvalidParameterError(
                f"Unsupported language '{language}'. Only 'SQL', 'Python', and 'Python_Dict' are supported."
            )

        logger.info(f"Generating DLT rules with LLM for input: '{user_input}'")
        schema_info = get_column_metadata(self.spark, input_config) if input_config else ""

        # Generate rules using pre-initialized LLM compiler
        prediction = self.llm_engine.detect_business_rules_with_llm(
            user_input=user_input, schema_info=schema_info, summary_stats=summary_stats
        )

        # Validate the generated rules
        dq_rules = json.loads(prediction.quality_rules)
        status = DQEngine.validate_checks(checks=dq_rules, custom_check_functions=None)
        if status.has_errors:
            logger.warning(f"Generated rules have validation errors: {status.errors}")
        else:
            logger.info(f"Generated {len(dq_rules)} rules with LLM: {dq_rules}")
            logger.info(f"LLM reasoning: {prediction.reasoning}")

        # Convert DQ rules to DQProfile objects inline
        profiles: list[DQProfile] = []
        for dq_rule in dq_rules:
            check = dq_rule.get("check", {})
            check_func = check.get("function", "")
            check_args = check.get("arguments", {})
            criticality = dq_rule.get("criticality", "error")
            filter_expr = dq_rule.get("filter")

            # Map DQ rule check functions to DQProfile names
            profile_name: str | None = None
            profile_params: dict = {}

            if check_func == "is_not_null":
                profile_name = "is_not_null"
            elif check_func == "is_in_list":
                profile_name = "is_in"
                profile_params["in"] = check_args.get("allowed", [])
            elif check_func == "is_not_null_and_not_empty":
                profile_name = "is_not_null_or_empty"
                profile_params["trim_strings"] = check_args.get("trim_strings", True)
            elif check_func == "is_in_range":
                profile_name = "min_max"
                profile_params["min"] = check_args.get("min_limit")
                profile_params["max"] = check_args.get("max_limit")
            elif check_func == "is_not_less_than":
                profile_name = "min_max"
                profile_params["min"] = check_args.get("limit")
            elif check_func == "is_not_greater_than":
                profile_name = "min_max"
                profile_params["max"] = check_args.get("limit")
            else:
                logger.info(
                    f"Unsupported check function '{check_func}' for column '{check_args.get('column', 'unknown')}'. skipping..."
                )
                continue

            column = check_args.get("column")
            if not column:
                logger.warning(f"Missing column in check arguments for rule: {dq_rule}")
                continue

            # Create DQProfile with criticality stored in parameters for later use
            profile = DQProfile(
                name=profile_name,
                column=column,
                parameters=profile_params,
                filter=filter_expr,
            )
            # Store criticality in a way we can access it later
            # We'll use a custom attribute or store it in parameters
            setattr(profile, "_criticality", criticality)
            profiles.append(profile)

        if not profiles:
            logger.warning("No valid profiles generated from LLM rules")
            if lang == "sql":
                return []
            elif lang == "python":
                return ""
            else:  # python_dict (already validated above)
                return {}

        # Generate DLT rules with per-rule criticality
        if lang == "sql":
            return self._generate_dlt_rules_sql_with_criticality(profiles)

        if lang == "python":
            return self._generate_dlt_rules_python_with_criticality(profiles)

        if lang == "python_dict":
            return self._generate_dlt_rules_python_dict_with_criticality(profiles)

        # This should never be reached due to early validation, but kept for safety
        raise InvalidParameterError(
            f"Unsupported language '{language}'. Only 'SQL', 'Python', and 'Python_Dict' are supported."
        )

    def _generate_dlt_rules_sql_with_criticality(self, rules: list[DQProfile]) -> list[str]:
        """
        Generates Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules in SQL with per-rule criticality.

        Args:
            rules: A list of data quality profiles to generate rules for.

        Returns:
            A list of Lakeflow Pipelines rules with appropriate ON VIOLATION clauses.
        """
        dlt_rules = []
        for rule in rules:
            rule_name = rule.name
            column = rule.column
            params = rule.parameters or {}
            criticality = getattr(rule, "_criticality", "error")

            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue

            expr = function_mapping[rule_name](column, **params)
            if expr == "":
                logger.info(f"Empty expression was generated for rule '{rule_name}' for column '{column}'")
                continue

            # Add ON VIOLATION clause based on criticality
            act_str = ""
            if criticality == "error":
                act_str = " ON VIOLATION FAIL UPDATE"
            # warn criticality has no ON VIOLATION clause (just EXPECT)

            dlt_rule = f"CONSTRAINT {column}_{rule_name} EXPECT ({expr}){act_str}"
            dlt_rules.append(dlt_rule)

        return dlt_rules

    def _generate_dlt_rules_python_with_criticality(self, rules: list[DQProfile]) -> str:
        """
        Generates Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules in Python with per-rule criticality.

        Args:
            rules: A list of data quality profiles to generate rules for.

        Returns:
            A string representing the Lakeflow Pipelines rules in Python with separate decorator blocks
            for error and warn criticality.
        """
        # Group rules by criticality
        error_rules: list[DQProfile] = []
        warn_rules: list[DQProfile] = []

        for rule in rules:
            criticality = getattr(rule, "_criticality", "error")
            if criticality == "error":
                error_rules.append(rule)
            else:  # warn
                warn_rules.append(rule)

        result_parts = []

        # Generate error rules with @dlt.expect_all_or_fail
        if error_rules:
            error_expectations = self._generate_dlt_rules_python_dict(error_rules)
            if error_expectations:
                json_expectations = json.dumps(error_expectations)
                result_parts.append(
                    f"""@dlt.expect_all_or_fail(
{json_expectations}
)"""
                )

        # Generate warn rules with @dlt.expect_all
        if warn_rules:
            warn_expectations = self._generate_dlt_rules_python_dict(warn_rules)
            if warn_expectations:
                json_expectations = json.dumps(warn_expectations)
                result_parts.append(
                    f"""@dlt.expect_all(
{json_expectations}
)"""
                )

        return "\n\n".join(result_parts) if result_parts else ""

    def _generate_dlt_rules_python_dict_with_criticality(self, rules: list[DQProfile]) -> dict:
        """
        Generates Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules as Python dictionary with per-rule criticality.

        Args:
            rules: A list of data quality profiles to generate rules for.

        Returns:
            A dict representing the Lakeflow Pipelines rules in Python, with criticality metadata.
        """
        expectations = {}
        for rule in rules:
            rule_name = rule.name
            column = rule.column
            params = rule.parameters or {}
            criticality = getattr(rule, "_criticality", "error")

            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue

            expr = function_mapping[rule_name](column, **params)
            if expr == "":
                logger.info(f"Empty expression was generated for rule '{rule_name}' for column '{column}'")
                continue

            exp_name = re.sub(__name_sanitize_re__, "_", f"{column}_{rule_name}")
            expectations[exp_name] = {
                "expression": expr,
                "criticality": criticality,
            }

        return expectations
