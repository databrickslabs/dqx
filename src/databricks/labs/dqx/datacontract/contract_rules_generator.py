"""
Data Contract to DQX Rules Generator.

This module provides functionality to generate DQX quality rules from data contract
specifications like ODCS (Open Data Contract Standard).
"""

import json
import logging
from collections.abc import Callable

from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.datacontract.formats.odcs import ODCSContract, ODCSProperty
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.telemetry import telemetry_logger

# Conditional imports for LLM-assisted rules generation
try:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine

    LLM_ENABLED = True
except ImportError:
    LLM_ENABLED = False

logger = logging.getLogger(__name__)


class DataContractRulesGenerator(DQEngineBase):
    """
    Generator for creating DQX quality rules from data contract specifications.

    This class supports generating quality rules from data contracts in various formats
    (currently ODCS v3.0.x). Rules can be generated implicitly from schema constraints
    or explicitly from quality definitions in the contract.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        llm_engine: 'DQLLMEngine | None' = None,
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initialize the DataContractRulesGenerator.

        Args:
            workspace_client: Databricks WorkspaceClient instance.
            spark: Optional SparkSession instance. If not provided, a new session will be created.
            llm_engine: Optional LLM engine for processing text-based quality expectations.
            custom_check_functions: Optional dictionary of custom check functions.
        """
        super().__init__(workspace_client=workspace_client)
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self.llm_engine = llm_engine
        self.custom_check_functions = custom_check_functions

    @telemetry_logger("datacontract", "generate_rules_from_contract")
    def generate_rules_from_contract(
        self,
        contract: dict,
        format: str = "odcs",
        generate_implicit_rules: bool = True,
        process_text_rules: bool = True,
        default_criticality: str = "error",
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
            default_criticality: Default criticality level for generated rules (default is "error").

        Returns:
            A list of dictionaries representing the generated DQX quality rules.

        Raises:
            ValueError: If the contract format is not supported or validation fails.
        """
        if format != "odcs":
            raise ValueError(f"Contract format '{format}' not supported. Currently only 'odcs' is supported.")

        # Parse and validate the contract (validation happens in from_dict)
        try:
            odcs_contract = ODCSContract.from_dict(contract)
        except ValueError as e:
            # Re-raise with context
            raise ValueError(f"Failed to parse ODCS contract: {e}") from e

        logger.info(f"Parsing ODCS contract '{odcs_contract.name}' v{odcs_contract.version}")

        dq_rules = []

        # Generate implicit rules from schema properties
        if generate_implicit_rules:
            for prop in odcs_contract.properties:
                implicit_rules = self._generate_implicit_rules_for_property(
                    prop, odcs_contract, default_criticality
                )
                dq_rules.extend(implicit_rules)

        # Process text-based quality expectations
        if process_text_rules:
            text_rules = self._process_text_based_rules(odcs_contract, default_criticality)
            dq_rules.extend(text_rules)

        # Process explicit DQX format rules from custom properties
        explicit_rules = self._process_explicit_dqx_rules(odcs_contract, default_criticality)
        dq_rules.extend(explicit_rules)

        # Validate generated rules
        if dq_rules:
            status = DQEngine.validate_checks(dq_rules, self.custom_check_functions)
            if status.has_errors:
                logger.warning(f"Generated rules have validation errors: {status.errors}")
            else:
                logger.info(f"Successfully generated {len(dq_rules)} DQX rules from ODCS contract")

        return dq_rules

    def _generate_implicit_rules_for_property(
        self, prop: ODCSProperty, contract: ODCSContract, default_criticality: str
    ) -> list[dict]:
        """Generate implicit DQ rules from a property's schema definition."""
        rules = []
        contract_metadata = {
            "odcs_contract_name": contract.name,
            "odcs_contract_version": contract.version,
            "odcs_property": prop.name,
        }

        # Completeness: required/not_null checks
        if prop.required or prop.not_null:
            dimension = "completeness"
            criticality = default_criticality

            if prop.not_empty and prop.logical_type in ('string', 'text', None):
                # For strings, check both null and empty
                rules.append(
                    {
                        "check": {
                            "function": "is_not_null_and_not_empty",
                            "arguments": {"column": prop.name, "trim_strings": True},
                        },
                        "name": f"{prop.name}_is_null_or_empty",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "odcs_dimension": dimension,
                            "odcs_rule_type": "implicit",
                        },
                    }
                )
            else:
                rules.append(
                    {
                        "check": {"function": "is_not_null", "arguments": {"column": prop.name}},
                        "name": f"{prop.name}_is_null",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "odcs_dimension": dimension,
                            "odcs_rule_type": "implicit",
                        },
                    }
                )

        # Validity: valid_values (enum) check
        if prop.valid_values:
            dimension = "validity"
            criticality = default_criticality
            rules.append(
                {
                    "check": {
                        "function": "is_in_list",
                        "arguments": {"column": prop.name, "allowed": prop.valid_values},
                    },
                    "name": f"{prop.name}_invalid_value",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "odcs_dimension": dimension,
                        "odcs_rule_type": "implicit",
                    },
                }
            )

        # Validity: pattern/regex check
        if prop.pattern:
            dimension = "validity"
            criticality = default_criticality
            rules.append(
                {
                    "check": {"function": "regex_match", "arguments": {"column": prop.name, "regex": prop.pattern}},
                    "name": f"{prop.name}_invalid_pattern",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "odcs_dimension": dimension,
                        "odcs_rule_type": "implicit",
                    },
                }
            )

        # Validity: range checks
        if prop.min_value is not None or prop.max_value is not None:
            dimension = "validity"
            criticality = default_criticality

            if prop.min_value is not None and prop.max_value is not None:
                rules.append(
                    {
                        "check": {
                            "function": "is_in_range",
                            "arguments": {
                                "column": prop.name,
                                "min_limit": prop.min_value,
                                "max_limit": prop.max_value,
                            },
                        },
                        "name": f"{prop.name}_out_of_range",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "odcs_dimension": dimension,
                            "odcs_rule_type": "implicit",
                        },
                    }
                )
            elif prop.min_value is not None:
                rules.append(
                    {
                        "check": {
                            "function": "is_not_less_than",
                            "arguments": {"column": prop.name, "limit": prop.min_value},
                        },
                        "name": f"{prop.name}_below_minimum",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "odcs_dimension": dimension,
                            "odcs_rule_type": "implicit",
                        },
                    }
                )
            elif prop.max_value is not None:
                rules.append(
                    {
                        "check": {
                            "function": "is_not_greater_than",
                            "arguments": {"column": prop.name, "limit": prop.max_value},
                        },
                        "name": f"{prop.name}_above_maximum",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "odcs_dimension": dimension,
                            "odcs_rule_type": "implicit",
                        },
                    }
                )

        # Uniqueness: unique constraint
        if prop.unique:
            dimension = "uniqueness"
            criticality = default_criticality
            rules.append(
                {
                    "check": {"function": "is_unique", "arguments": {"column": prop.name}},
                    "name": f"{prop.name}_not_unique",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "odcs_dimension": dimension,
                        "odcs_rule_type": "implicit",
                    },
                }
            )

        # Validity: date/timestamp format checks
        if prop.format:
            dimension = "validity"
            criticality = default_criticality

            if prop.logical_type in ('date', 'datetime', 'timestamp'):
                # Detect if it's a timestamp based on format (time components: HH, mm, ss)
                is_timestamp = prop.format and any(time_component in prop.format for time_component in ['HH', 'mm', 'ss', 'hh'])
                
                if is_timestamp or prop.logical_type in ('datetime', 'timestamp'):
                    rules.append(
                        {
                            "check": {
                                "function": "is_valid_timestamp",
                                "arguments": {"column": prop.name, "timestamp_format": prop.format},
                            },
                            "name": f"{prop.name}_invalid_timestamp_format",
                            "criticality": criticality,
                            "user_metadata": {
                                **contract_metadata,
                                "odcs_dimension": dimension,
                                "odcs_rule_type": "implicit",
                            },
                        }
                    )
                else:
                    rules.append(
                        {
                            "check": {
                                "function": "is_valid_date",
                                "arguments": {"column": prop.name, "date_format": prop.format},
                            },
                            "name": f"{prop.name}_invalid_date_format",
                            "criticality": criticality,
                            "user_metadata": {
                                **contract_metadata,
                                "odcs_dimension": dimension,
                                "odcs_rule_type": "implicit",
                            },
                        }
                    )

        return rules

    def _build_schema_info(self, contract: ODCSContract) -> str:
        """Build schema info JSON string from ODCS contract for LLM context."""
        columns = []
        for prop in contract.properties:
            col_info = {"name": prop.name}
            
            # Map ODCS logical types to simple type names
            if prop.logical_type:
                type_mapping = {
                    "string": "string",
                    "text": "string",
                    "integer": "int",
                    "numeric": "decimal",
                    "decimal": "decimal",
                    "float": "float",
                    "double": "double",
                    "boolean": "boolean",
                    "date": "date",
                    "datetime": "timestamp",
                    "timestamp": "timestamp",
                }
                col_info["type"] = type_mapping.get(prop.logical_type, prop.logical_type)
            else:
                col_info["type"] = "string"  # default
            
            columns.append(col_info)
        
        schema_dict = {"columns": columns}
        return json.dumps(schema_dict)

    def _process_text_based_rules(self, contract: ODCSContract, default_criticality: str) -> list[dict]:
        """Process text-based quality expectations using LLM."""
        rules: list[dict] = []

        if self.llm_engine is None:
            logger.warning(
                "Text-based rules found but LLM engine not available. "
                "Skipping text rules. Install LLM dependencies: pip install 'databricks-labs-dqx[llm]'"
            )
            return rules

        contract_metadata = {
            "odcs_contract_name": contract.name,
            "odcs_contract_version": contract.version,
        }

        # Generate schema info from contract for LLM context
        schema_info = self._build_schema_info(contract)

        # Extract text-based expectations from properties
        # ODCS v3.0.x: quality is an array of quality check objects with type field
        for prop in contract.properties:
            if not prop.quality or not isinstance(prop.quality, list):
                continue
            
            for quality_check in prop.quality:
                if not isinstance(quality_check, dict):
                    continue
                    
                check_type = quality_check.get('type')
                
                # Handle ODCS type: text format
                if check_type == 'text':
                    text_expectation = quality_check.get('description', '')
                    if not text_expectation:
                        continue

                for text_expectation in [text_expectation]:
                    try:
                        # Include property context for better LLM understanding
                        context_info = f"Property: {prop.name}"
                        if prop.logical_type:
                            context_info += f", Type: {prop.logical_type}"
                        if prop.description:
                            context_info += f", Description: {prop.description}"

                        user_input = f"{context_info}\nExpectation: {text_expectation}"

                        # Generate rules using LLM with schema context
                        logger.info(f"Processing text rule for {prop.name}: {text_expectation}")
                        prediction = self.llm_engine.get_business_rules_with_llm(
                            user_input=user_input, schema_info=schema_info
                        )

                        text_rules = json.loads(prediction.quality_rules)

                        # Add metadata to generated rules
                        for rule in text_rules:
                            if 'user_metadata' not in rule:
                                rule['user_metadata'] = {}
                            rule['user_metadata'].update(
                                {
                                    **contract_metadata,
                                    "odcs_property": prop.name,
                                    "odcs_rule_type": "text_llm",
                                    "odcs_text_expectation": text_expectation,
                                }
                            )
                            rules.append(rule)

                    except Exception as e:
                        logger.warning(f"Failed to process text rule for {prop.name}: {e}")

        return rules

    def _process_explicit_dqx_rules(self, contract: ODCSContract, default_criticality: str) -> list[dict]:
        """Process explicit DQX format rules from custom properties."""
        rules = []

        contract_metadata = {
            "odcs_contract_name": contract.name,
            "odcs_contract_version": contract.version,
        }

        # Check for custom properties with DQX native format
        # ODCS v3.0.x: quality is an array of quality check objects with type field
        for prop in contract.properties:
            if not prop.quality or not isinstance(prop.quality, list):
                continue
            
            for quality_check in prop.quality:
                if not isinstance(quality_check, dict):
                    continue
                    
                check_type = quality_check.get('type')
                
                # Handle ODCS type: custom format with DQX engine
                if check_type == 'custom' and quality_check.get('engine') == 'dqx':
                    implementation = quality_check.get('implementation', {})
                    if not isinstance(implementation, dict):
                        continue
                    
                    # Check if it's DQX format (has 'check' key with 'function')
                    if 'check' in implementation:
                        custom_rules_list = [implementation]
                    else:
                        continue
                else:
                    continue

                for custom_rule in custom_rules_list:
                    if isinstance(custom_rule, dict) and 'check' in custom_rule:
                        # Add metadata
                        if 'user_metadata' not in custom_rule:
                            custom_rule['user_metadata'] = {}
                        custom_rule['user_metadata'].update(
                            {
                                **contract_metadata,
                                "odcs_property": prop.name,
                                "odcs_rule_type": "explicit",
                            }
                        )
                        rules.append(custom_rule)
                        logger.info(f"Added explicit DQX rule for {prop.name}")

        return rules
