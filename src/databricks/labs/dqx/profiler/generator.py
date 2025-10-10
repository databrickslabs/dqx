import logging
import json
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.common import val_maybe_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile
from databricks.labs.dqx.telemetry import telemetry_logger

# Conditional imports for LLM functionality
try:
    from databricks.labs.dqx.llm.llm_core import get_dspy_compiler
    from databricks.labs.dqx.llm.llm_engine import get_business_rules_with_llm
    from databricks.labs.dqx.llm.llm_utils import get_column_metadata

    LLM_ENABLED = True
except ImportError:
    LLM_ENABLED = False

logger = logging.getLogger(__name__)


class DQGenerator(DQEngineBase):
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        model: str = "databricks/databricks-meta-llama-3-3-70b-instruct",
        api_key: str = "",
        api_base: str = "",
    ):
        super().__init__(workspace_client=workspace_client)
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

        # Initialize DSPy compiler during init
        if not LLM_ENABLED:
            logger.warning("LLM dependencies not installed. DSPy compiler not available.")
            self.dspy_compiler = None
        else:
            try:
                self.dspy_compiler = get_dspy_compiler(model=model, api_key=api_key, api_base=api_base)
                logger.info(f"DSPy compiler initialized with model: {model}")
            except Exception as e:
                logger.error(f"Failed to initialize DSPy compiler: {e}")
                self.dspy_compiler = None

    @telemetry_logger("generator", "generate_dq_rules")
    def generate_dq_rules(self, profiles: list[DQProfile] | None = None, level: str = "error") -> list[dict]:
        """
        Generates a list of data quality rules based on the provided dq profiles.

        Args:
                profiles: A list of data quality profiles to generate rules for.
                level: The criticality level of the rules (default is "error").

        Returns:
                A list of dictionaries representing the data quality rules.
        """
        if profiles is None:
            profiles = []
        dq_rules = []
        for profile in profiles:
            rule_name = profile.name
            column = profile.column
            params = profile.parameters or {}
            dataset_filter = profile.filter
            if rule_name not in self._checks_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = self._checks_mapping[rule_name](column, level, **params)

            if expr:
                if dataset_filter is not None:
                    expr["filter"] = dataset_filter
                dq_rules.append(expr)

        status = DQEngine.validate_checks(dq_rules)
        assert not status.has_errors

        return dq_rules

    @telemetry_logger("generator", "generate_dq_rules_with_llm")
    def generate_dq_rules_with_llm(self, user_input: str, table_name: str = "") -> list[dict]:
        """
        Generates data quality rules using LLM based on natural language input.

        Args:
            user_input: Natural language description of data quality requirements.
            table_name: Optional fully qualified table name.
                        If not provided, LLM will be used to guess the table schema.

        Returns:
            A list of dictionaries representing the generated data quality rules.

        Raises:
            RuntimeError: If DSPy compiler is not available.
        """
        # Check if DSPy compiler is available
        if self.dspy_compiler is None:
            raise RuntimeError(
                "DSPy compiler not available. Make sure LLM dependencies are installed: "
                "pip install 'databricks-labs-dqx[llm]'"
            )

        schema_info = get_column_metadata(table_name, self.spark) if table_name else ""

        logger.info(f"Generating DQ rules with LLM for input: '{user_input}'")

        # Generate rules using pre-initialized LLM compiler
        prediction = get_business_rules_with_llm(
            user_input=user_input, dspy_compiler=self.dspy_compiler, schema_info=schema_info
        )

        # Parse the generated rules
        try:
            dq_rules = json.loads(prediction.quality_rules)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM generated rules: {e}")

        # Validate the generated rules using DQEngine
        status = DQEngine.validate_checks(dq_rules)
        if status.has_errors:
            logger.warning(f"Generated rules have validation errors: {status.errors}")
        else:
            logger.info(f"Generated {len(dq_rules)} rules with LLM:{dq_rules}")
            logger.info(f"LLM reasoning: {prediction.reasoning}")

        return dq_rules

    @staticmethod
    def dq_generate_is_in(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is in a specified list.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters, including the list of values to check against.

        Returns:
                A dictionary representing the data quality rule.
        """
        return {
            "check": {"function": "is_in_list", "arguments": {"column": column, "allowed": params["in"]}},
            "name": f"{column}_other_value",
            "criticality": level,
        }

    @staticmethod
    def dq_generate_min_max(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is within a specified range.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters, including the minimum and maximum values.

        Returns:
                A dictionary representing the data quality rule, or None if no limits are provided.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")

        if not isinstance(min_limit, int) or not isinstance(max_limit, int):
            return None  # TODO handle timestamp and dates: https://github.com/databrickslabs/dqx/issues/71

        if min_limit is not None and max_limit is not None:
            return {
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": column,
                        "min_limit": val_maybe_to_str(min_limit, include_sql_quotes=False),
                        "max_limit": val_maybe_to_str(max_limit, include_sql_quotes=False),
                    },
                },
                "name": f"{column}_isnt_in_range",
                "criticality": level,
            }

        if max_limit is not None:
            return {
                "check": {
                    "function": "is_not_greater_than",
                    "arguments": {"column": column, "limit": val_maybe_to_str(max_limit, include_sql_quotes=False)},
                },
                "name": f"{column}_not_greater_than",
                "criticality": level,
            }

        if min_limit is not None:
            return {
                "check": {
                    "function": "is_not_less_than",
                    "arguments": {"column": column, "limit": val_maybe_to_str(min_limit, include_sql_quotes=False)},
                },
                "name": f"{column}_not_less_than",
                "criticality": level,
            }

        return None

    @staticmethod
    def dq_generate_is_not_null(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters.

        Returns:
                A dictionary representing the data quality rule.
        """
        params = params or {}

        return {
            "check": {"function": "is_not_null", "arguments": {"column": column}},
            "name": f"{column}_is_null",
            "criticality": level,
        }

    @staticmethod
    def dq_generate_is_not_null_or_empty(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null or empty.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters, including whether to trim strings.

        Returns:
                A dictionary representing the data quality rule.
        """

        return {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": column, "trim_strings": params.get("trim_strings", True)},
            },
            "name": f"{column}_is_null_or_empty",
            "criticality": level,
        }

    _checks_mapping = {
        "is_not_null": dq_generate_is_not_null,
        "is_in": dq_generate_is_in,
        "min_max": dq_generate_min_max,
        "is_not_null_or_empty": dq_generate_is_not_null_or_empty,
    }
