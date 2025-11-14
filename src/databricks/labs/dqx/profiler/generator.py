import logging
import json
from collections.abc import Callable

from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.config import LLMModelConfig, InputConfig
from databricks.labs.dqx.datacontract.contract_rules_generator import DataContractRulesGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.common import val_maybe_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile
from databricks.labs.dqx.telemetry import telemetry_logger
from databricks.labs.dqx.errors import MissingParameterError

# Conditional imports for LLM-assisted rules generation
try:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine
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
        llm_model_config: LLMModelConfig | None = None,
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initializes the DQGenerator with optional Spark session and LLM model configuration.

        Args:
            workspace_client: Databricks WorkspaceClient instance.
            spark: Optional SparkSession instance. If not provided, a new session will be created.
            llm_model_config: Optional LLM model configuration for AI-assisted rule generation.
            custom_check_functions: Optional dictionary of custom check functions.
        """
        super().__init__(workspace_client=workspace_client)
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

        self.custom_check_functions = custom_check_functions
        llm_model_config = llm_model_config or LLMModelConfig()

        self.llm_engine = (
            DQLLMEngine(model_config=llm_model_config, custom_check_functions=custom_check_functions)
            if LLM_ENABLED
            else None
        )

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

        status = DQEngine.validate_checks(dq_rules, self.custom_check_functions)
        assert not status.has_errors

        return dq_rules

    @telemetry_logger("generator", "generate_dq_rules_ai_assisted")
    def generate_dq_rules_ai_assisted(self, user_input: str, input_config: InputConfig | None = None) -> list[dict]:
        """
        Generates data quality rules using LLM based on natural language input.

        Args:
            user_input: Natural language description of data quality requirements.
            input_config: Optional input config providing input data location as a path or fully qualified table name
                to infer schema. If not provided, LLM will be used to guess the table schema.

        Returns:
            A list of dictionaries representing the generated data quality rules.

        Raises:
            MissingParameterError: If DSPy compiler is not available.
        """
        if self.llm_engine is None:
            raise MissingParameterError(
                "LLM engine not available. Make sure LLM dependencies are installed: "
                "pip install 'databricks-labs-dqx[llm]'"
            )

        logger.info(f"Generating DQ rules with LLM for input: '{user_input}'")
        schema_info = get_column_metadata(self.spark, input_config) if input_config else ""

        # Generate rules using pre-initialized LLM compiler
        prediction = self.llm_engine.get_business_rules_with_llm(user_input=user_input, schema_info=schema_info)

        # Validate the generated rules
        dq_rules = json.loads(prediction.quality_rules)
        status = DQEngine.validate_checks(checks=dq_rules, custom_check_functions=self.custom_check_functions)
        if status.has_errors:
            logger.warning(f"Generated rules have validation errors: {status.errors}")
        else:
            logger.info(f"Generated {len(dq_rules)} rules with LLM: {dq_rules}")
            logger.info(f"LLM reasoning: {prediction.reasoning}")

        return dq_rules

    @telemetry_logger("generator", "generate_rules_from_contract")
    def generate_rules_from_contract(
        self,
        contract: dict,
        format: str = "odcs",
        generate_implicit_rules: bool = True,
        process_text_rules: bool = True,
        default_criticality: str = "error",
        criticality_mapping: dict[str, str] | None = None,
    ) -> list[dict]:
        """
        Generate DQX quality rules from a data contract specification.

        Parses a data contract (currently supporting ODCS v3.0.x) and generates rules based on
        schema properties, explicit quality definitions, and text-based expectations.

        Args:
            contract: Dictionary representation of the data contract.
            format: Contract format specification (default is "odcs").
            generate_implicit_rules: Whether to generate rules from schema properties.
            process_text_rules: Whether to process text-based expectations using LLM.
            default_criticality: Default criticality level (default is "error").
            criticality_mapping: Optional mapping of quality dimensions to criticality levels.

        Returns:
            A list of dictionaries representing the generated DQX quality rules.

        Raises:
            ValueError: If the contract format is not supported or validation fails.
        """
        # Create a contract generator with the same context
        contract_generator = DataContractRulesGenerator(
            workspace_client=self._workspace_client,
            spark=self.spark,
            llm_engine=self.llm_engine,
            custom_check_functions=self.custom_check_functions,
        )

        # Delegate to the contract generator
        return contract_generator.generate_rules_from_contract(
            contract=contract,
            format=format,
            generate_implicit_rules=generate_implicit_rules,
            process_text_rules=process_text_rules,
            default_criticality=default_criticality,
            criticality_mapping=criticality_mapping,
        )

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
