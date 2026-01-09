from __future__ import annotations

import logging
import datetime
import json
from collections.abc import Callable
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.config import LLMModelConfig, InputConfig
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

# Conditional imports for data contract support
try:
    from databricks.labs.dqx.datacontract.contract_rules_generator import DataContractRulesGenerator
    from datacontract.data_contract import DataContract  # type: ignore

    DATACONTRACT_ENABLED = True
except ImportError:
    DATACONTRACT_ENABLED = False

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
            DQLLMEngine(model_config=llm_model_config, spark=self.spark, custom_check_functions=custom_check_functions)
            if LLM_ENABLED
            else None
        )

    @telemetry_logger("generator", "generate_dq_rules")
    def generate_dq_rules(self, profiles: list[DQProfile] | None = None, criticality: str = "error") -> list[dict]:
        """
        Generates a list of data quality rules based on the provided dq profiles.

        Args:
            profiles: A list of data quality profiles to generate rules for.
            criticality: The criticality of the rules as "warn" or "error" (default is "error").

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
            expr = self._checks_mapping[rule_name](column, criticality, **params)

            if expr:
                if dataset_filter is not None:
                    expr["filter"] = dataset_filter
                dq_rules.append(expr)

        status = DQEngine.validate_checks(dq_rules, self.custom_check_functions)
        assert not status.has_errors

        logger.info(f"✅ Quality rules generation completed. Generated {len(dq_rules)} rules.")
        return dq_rules

    @telemetry_logger("generator", "generate_dq_rules_ai_assisted")
    def generate_dq_rules_ai_assisted(
        self, user_input: str = "", summary_stats: dict | None = None, input_config: InputConfig | None = None
    ) -> list[dict]:
        """
        Generates data quality rules using LLM based on natural language input.

        Args:
            user_input: Optional Natural language description of data quality requirements.
            summary_stats: Optional summary statistics of the input data.
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
        if not summary_stats and not user_input:
            raise MissingParameterError(
                "Either summary statistics or user input must be provided to generate rules using LLM."
            )

        logger.info(f"Generating DQ rules with LLM for input: '{user_input}'")
        schema_info = get_column_metadata(self.spark, input_config) if input_config else ""

        # Generate rules using pre-initialized LLM compiler
        prediction = self.llm_engine.detect_business_rules_with_llm(
            user_input=user_input, schema_info=schema_info, summary_stats=summary_stats
        )

        # Validate the generated rules
        dq_rules = json.loads(prediction.quality_rules)
        status = DQEngine.validate_checks(checks=dq_rules, custom_check_functions=self.custom_check_functions)
        if status.has_errors:
            logger.warning(f"Generated rules have validation errors: {status.errors}")
        else:
            logger.info(f"Generated {len(dq_rules)} rules with LLM: {dq_rules}")
            logger.info(f"LLM reasoning: {prediction.reasoning}")

        logger.info(f"✅ AI-Assisted quality rules generation completed. Generated {len(dq_rules)} rules.")
        return dq_rules

    @telemetry_logger("generator", "generate_rules_from_contract")
    def generate_rules_from_contract(
        self,
        contract: DataContract | None = None,
        contract_file: str | None = None,
        contract_format: str = "odcs",
        generate_predefined_rules: bool = True,
        process_text_rules: bool = True,
        default_criticality: str = "error",
    ) -> list[dict]:
        """
        Generate DQX quality rules from a data contract specification.

        Parses a data contract (currently supporting ODCS v3.0.x) and generates rules based on
        schema properties, explicit quality definitions, and text-based expectations.

        Args:
            contract: Pre-loaded DataContract object from datacontract-cli. Can be created with:
                - DataContract(data_contract_file=path) - from a file path
                - DataContract(data_contract_str=yaml_string) - from a YAML/JSON string
                Either `contract` or `contract_file` must be provided.
            contract_file: Path to contract YAML file (local, volume, or workspace).
            contract_format: Contract format specification (default is "odcs").
            generate_predefined_rules: Whether to generate rules from schema properties.
            process_text_rules: Whether to process text-based expectations using LLM.
            default_criticality: Default criticality for generated rules as "warn" or "error" (default is "error").

        Returns:
            A list of dictionaries representing the generated DQX quality rules.

        Raises:
            MissingParameterError: If datacontract-cli is not installed.
            ParameterError: If neither or both parameters are provided, or format not supported.

        Note:
            Exactly one of 'contract' or 'contract_file' must be provided.
        """
        if not DATACONTRACT_ENABLED:
            raise MissingParameterError(
                "Data contract support requires datacontract-cli. "
                "Install it with: pip install 'databricks-labs-dqx[datacontract]'"
            )

        # Create a contract generator with the same context
        contract_generator = DataContractRulesGenerator(
            workspace_client=self._workspace_client,
            llm_engine=self.llm_engine,
            custom_check_functions=self.custom_check_functions,
        )

        # Delegate to the contract generator
        dq_rules = contract_generator.generate_rules_from_contract(
            contract=contract,
            contract_file=contract_file,
            contract_format=contract_format,
            generate_predefined_rules=generate_predefined_rules,
            process_text_rules=process_text_rules,
            default_criticality=default_criticality,
        )
        logger.info(
            f"✅ Quality rules generation from a data contract specification completed. Generated {len(dq_rules)} rules."
        )
        return dq_rules

    @staticmethod
    def dq_generate_is_in(column: str, criticality: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is in a specified list.

        Args:
                column: The name of the column to check.
                criticality: The criticality of the rule as "warn" or "error"  (default is "error").
                params: Additional parameters, including the list of values to check against.

        Returns:
                A dictionary representing the data quality rule.
        """
        return {
            "check": {"function": "is_in_list", "arguments": {"column": column, "allowed": params["in"]}},
            "name": f"{column}_other_value",
            "criticality": criticality,
        }

    @staticmethod
    def dq_generate_min_max(column: str, criticality: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is within a specified range.

        Args:
            column: The name of the column to check.
            criticality: The criticality of the rule as "warn" or "error"  (default is "error").
            params: Additional parameters, including the minimum and maximum values.

        Returns:
            A dictionary representing the data quality rule, or None if no limits are provided.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")

        if min_limit is None and max_limit is None:
            return None

        def _is_num(value):
            return isinstance(value, (int, float))

        def _is_temporal(value):
            return isinstance(value, (datetime.date, datetime.datetime))

        def _same_family(value_a, value_b):
            # numeric with numeric OR temporal with temporal
            return any(
                [
                    _is_num(value_a) and _is_num(value_b),
                    _is_temporal(value_a) and _is_temporal(value_b),
                ]
            )

        # Both bounds
        if min_limit is not None and max_limit is not None and _same_family(min_limit, max_limit):
            return {
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": column,
                        # pass through Python numeric types (int, float) without stringification
                        # except for temporal types (datetime/date) which are not Json serializable
                        "min_limit": val_maybe_to_str(min_limit, include_sql_quotes=False),
                        "max_limit": val_maybe_to_str(max_limit, include_sql_quotes=False),
                    },
                },
                "name": f"{column}_isnt_in_range",
                "criticality": criticality,
            }

        # Only max
        if max_limit is not None and (_is_num(max_limit) or _is_temporal(max_limit)):
            return {
                "check": {
                    "function": "is_not_greater_than",
                    "arguments": {"column": column, "limit": val_maybe_to_str(max_limit, include_sql_quotes=False)},
                },
                "name": f"{column}_not_greater_than",
                "criticality": criticality,
            }

        # Only min
        if min_limit is not None and (_is_num(min_limit) or _is_temporal(min_limit)):
            return {
                "check": {
                    "function": "is_not_less_than",
                    "arguments": {"column": column, "limit": val_maybe_to_str(min_limit, include_sql_quotes=False)},
                },
                "name": f"{column}_not_less_than",
                "criticality": criticality,
            }

        return None

    @staticmethod
    def dq_generate_is_not_null(column: str, criticality: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null.

        Args:
                column: The name of the column to check.
                criticality: The criticality of the rule as "warn" or "error"  (default is "error").
                params: Additional parameters.

        Returns:
                A dictionary representing the data quality rule.
        """
        params = params or {}

        return {
            "check": {"function": "is_not_null", "arguments": {"column": column}},
            "name": f"{column}_is_null",
            "criticality": criticality,
        }

    @staticmethod
    def dq_generate_is_not_null_or_empty(column: str, criticality: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null or empty.

        Args:
                column: The name of the column to check.
                criticality: The criticality of the rule as "warn" or "error"  (default is "error").
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
            "criticality": criticality,
        }

    @staticmethod
    def dq_generate_is_unique(column: str, criticality: str = "error", **params: dict):
        """Generates a data quality rule to check if specified columns are unique.

        Uses is_unique with nulls_distinct=True for uniqueness validation.

        Args:
            column: Comma-separated list of column names that form the primary key. Uses all columns if not provided.
            criticality: The criticality of the rule as "warn" or "error" (default is "error").
            params: Additional parameters including columns list, confidence, reasoning, etc.

        Returns:
            A dictionary representing the data quality rule.
        """
        columns = params.get("columns", column.split(","))

        # Clean up column names (remove whitespace)
        columns = [col.strip() for col in columns]

        confidence = params.get("confidence", "unknown")
        nulls_distinct = params.get("nulls_distinct", False)

        # Create base metadata
        user_metadata = {
            "pk_detection_confidence": confidence,
        }

        return {
            "check": {
                "function": "is_unique",
                "arguments": {"columns": columns, "nulls_distinct": nulls_distinct},
            },
            "name": f"primary_key_{'_'.join(columns)}_validation",
            "criticality": criticality,
            "user_metadata": user_metadata,
        }

    _checks_mapping = {
        "is_not_null": dq_generate_is_not_null,
        "is_in": dq_generate_is_in,
        "min_max": dq_generate_min_max,
        "is_not_null_or_empty": dq_generate_is_not_null_or_empty,
        "is_unique": dq_generate_is_unique,
    }
