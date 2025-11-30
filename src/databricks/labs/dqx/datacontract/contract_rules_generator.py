"""
Data Contract to DQX Rules Generator.

This module provides functionality to generate DQX quality rules from data contract
specifications like ODCS (Open Data Contract Standard).
"""

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

# Import datacontract dependencies (validated in __init__.py)
from datacontract.data_contract import DataContract  # type: ignore
from open_data_contract_standard.model import (  # type: ignore
    DataQuality,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)
from pydantic import ValidationError  # type: ignore

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import ODCSContractError, ParameterError
from databricks.labs.dqx.telemetry import telemetry_logger
from databricks.labs.dqx.utils import missing_required_packages

# Type checking imports (for type hints only, not evaluated at runtime)
if TYPE_CHECKING:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine  # type: ignore

logger = logging.getLogger(__name__)


class DataContractRulesGenerator(DQEngineBase):
    """
    Generator for creating DQX quality rules from ODCS v3.x data contracts.

    This class processes Open Data Contract Standard (ODCS) v3.x contracts natively,
    extracting constraints from logicalTypeOptions and generating DQX quality rules.
    Supports predefined rules from schema properties, explicit rules from quality sections,
    and text-based expectations processed via LLM.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        llm_engine: "DQLLMEngine | None" = None,
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initialize the DataContractRulesGenerator.

        Args:
            workspace_client: Databricks WorkspaceClient instance.
            llm_engine: Optional LLM engine for processing text-based quality expectations.
            custom_check_functions: Optional dictionary of custom check functions.

        Raises:
            ImportError: If LLM dependencies are missing when llm_engine is provided.
        """
        if llm_engine is not None:
            required_llm_specs = ["dspy"]
            if missing_required_packages(required_llm_specs):
                raise ImportError(
                    "LLM extras not installed. Install additional dependencies by running "
                    "`pip install databricks-labs-dqx[llm]`."
                )

        super().__init__(workspace_client=workspace_client)
        self.llm_engine = llm_engine
        self.custom_check_functions = custom_check_functions

    @telemetry_logger("datacontract", "generate_rules_from_contract")
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
        Generate DQX quality rules from an ODCS v3.x data contract.

        Parses an ODCS v3.x contract natively and generates rules based on schema properties,
        logicalTypeOptions constraints, explicit quality definitions, and text-based expectations.

        Args:
            contract: Pre-loaded DataContract object from datacontract-cli. Can be created with:
                - DataContract(data_contract_file=path) - from a file path
                - DataContract(data_contract_str=yaml_string) - from a YAML/JSON string
                Either `contract` or `contract_file` must be provided.
            contract_file: Path to contract YAML/JSON file (local, volume, or workspace). Either `contract` or `contract_file` must be provided.
            contract_format: Contract format specification (default is "odcs"). Only "odcs" is supported.
            generate_predefined_rules: Whether to generate rules from schema properties (default True). Set to False to only generate explicit rules.
            process_text_rules: Whether to process text-based expectations using LLM (default True). Requires llm_engine to be provided in __init__.
            default_criticality: Default criticality level for generated rules (default is "error").

        Returns:
            A list of dictionaries representing the generated DQX quality rules.

        Raises:
            InvalidParameterError: If neither or both contract parameters are provided, or format not supported.

        Note:
            Exactly one of 'contract' or 'contract_file' must be provided.
        """
        self._validate_inputs(contract, contract_file, contract_format)
        odcs = self._load_contract_spec(contract, contract_file)
        self._validate_contract_spec(odcs)

        dq_rules = self._generate_all_rules(odcs, generate_predefined_rules, process_text_rules, default_criticality)
        valid_rules = self._validate_generated_rules(dq_rules)

        return valid_rules

    def _validate_inputs(self, contract: DataContract | None, contract_file: str | None, contract_format: str) -> None:
        """Validate input parameters."""
        if contract is None and contract_file is None:
            raise ParameterError("Either 'contract' or 'contract_file' must be provided")

        if contract is not None and contract_file is not None:
            raise ParameterError("Cannot provide both 'contract' and 'contract_file'")

        if contract_format != "odcs":
            raise ParameterError(
                f"Contract format '{contract_format}' not supported. Currently only 'odcs' is supported."
            )

    def _load_contract_spec(self, contract: DataContract | None, contract_file: str | None) -> OpenDataContractStandard:
        """Load ODCS v3.x contract natively (no conversion to v1.2.1)."""
        if contract_file is not None:
            return self._load_contract_from_file(contract_file)

        if contract is not None:
            # Try to load from file path if available
            contract_file_path = getattr(contract, '_data_contract_file', None) or getattr(
                contract, 'data_contract_file', None
            )

            if contract_file_path:
                return self._load_contract_from_file(contract_file_path)

            # Try to load from data_contract attribute (pre-parsed dict)
            contract_data = getattr(contract, 'data_contract', None)
            if contract_data:
                return OpenDataContractStandard.model_validate(contract_data)

            # Try to load from data_contract_str attribute (YAML/JSON string)
            contract_str = getattr(contract, '_data_contract_str', None) or getattr(contract, 'data_contract_str', None)
            if contract_str:
                contract_dict = yaml.safe_load(contract_str)
                return OpenDataContractStandard.model_validate(contract_dict)

            raise ParameterError(
                "DataContract object must have either a file path, data_contract dict, or data_contract_str attribute"
            )

        raise ParameterError("Either contract or contract_file must be provided")

    def _load_contract_from_file(self, contract_location: str) -> OpenDataContractStandard:
        """
        Load ODCS v3.x contract directly from YAML/JSON file.

        This method provides a clean, direct path to load ODCS v3.x contracts by reading
        the YAML/JSON file and using Pydantic validation to create the OpenDataContractStandard object.

        Args:
            contract_location: Path to the contract YAML/JSON file

        Returns:
            OpenDataContractStandard object

        Raises:
            NotFound: If contract file does not exist.
            ODCSContractError: If contract file cannot be parsed parse.
        """
        contract_path = Path(contract_location)

        if not contract_path.exists():
            raise NotFound(f"Contract file not found: {contract_location}")

        with open(contract_path, 'r', encoding='utf-8') as f:
            contract_dict = yaml.safe_load(f)

        try:
            return OpenDataContractStandard.model_validate(contract_dict)
        except ValidationError as e:
            raise ODCSContractError(f"Failed to parse ODCS contract from {contract_location}: {e}") from e

    def _validate_contract_spec(self, odcs: OpenDataContractStandard) -> None:
        """
        Validate ODCS v3.x contract specification.

        Note: We skip the datacontract library's lint() method and perform validation
        on generated rules via DQEngine.validate_checks() instead, which provides more
        relevant feedback for DQX rule generation.
        """
        contract_version = odcs.version or "unknown"
        contract_name = odcs.name or odcs.id or "unknown"
        logger.info(f"Parsing ODCS v3.x contract '{contract_name}' v{contract_version} (API {odcs.apiVersion})")

    def _generate_all_rules(
        self,
        odcs: OpenDataContractStandard,
        generate_predefined_rules: bool,
        process_text_rules: bool,
        default_criticality: str,
    ) -> list[dict]:
        """Generate all rules from ODCS v3.x contract schemas."""
        dq_rules = []

        # ODCS v3.x uses schema_ list instead of models dict
        for schema_obj in odcs.schema_ or []:
            schema_name = schema_obj.name or "unknown_schema"

            if generate_predefined_rules:
                predefined_rules = self._generate_predefined_rules_for_schema(
                    schema_obj, schema_name, odcs, default_criticality
                )
                dq_rules.extend(predefined_rules)

            if process_text_rules:
                text_rules = self._process_text_rules_for_schema(schema_obj, schema_name, odcs)
                dq_rules.extend(text_rules)

            explicit_rules = self._process_explicit_rules_for_schema(schema_obj, schema_name, odcs, default_criticality)
            dq_rules.extend(explicit_rules)

        return dq_rules

    def _validate_generated_rules(self, dq_rules: list[dict]) -> list[dict]:
        """Validate generated DQX rules and filter out invalid ones.

        Returns:
            List of valid rules. Invalid rules are logged as warnings and excluded.
        """
        if not dq_rules:
            return []

        valid_rules = []
        invalid_count = 0

        for rule in dq_rules:
            status = DQEngine.validate_checks([rule], self.custom_check_functions)
            if status.has_errors:
                invalid_count += 1
                rule_name = rule.get('name', 'unnamed_rule')
                error_summary = "; ".join(status.errors)
                logger.warning(f"Excluding invalid rule '{rule_name}' from contract: {error_summary}")
            else:
                valid_rules.append(rule)

        if invalid_count > 0:
            logger.warning(
                f"Generated {len(dq_rules)} rules from data contract, excluded {invalid_count} invalid rule(s). "
                f"Returning {len(valid_rules)} valid rule(s)."
            )
        else:
            logger.info(f"Successfully generated {len(valid_rules)} DQX rules from data contract")

        return valid_rules

    # ODCS v3.x Native Support Methods
    def _generate_predefined_rules_for_schema(
        self, schema_obj: SchemaObject, schema_name: str, odcs: OpenDataContractStandard, default_criticality: str
    ) -> list[dict]:
        """Generate predefined rules from all properties in an ODCS schema."""
        rules = []

        contract_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
        }

        for prop in schema_obj.properties or []:
            prop_rules = self._generate_predefined_rules_for_property(
                prop, schema_name, contract_metadata, default_criticality
            )
            rules.extend(prop_rules)

        return rules

    def _generate_predefined_rules_for_property(
        self,
        prop: SchemaProperty,
        schema_name: str,
        contract_metadata: dict,
        default_criticality: str,
        parent_path: str = "",
        recursion_depth: int = 0,
    ) -> list[dict]:
        """Generate predefined DQ rules from an ODCS v3.x property."""

        max_recursion_depth = 20
        if recursion_depth > max_recursion_depth:
            logger.warning(
                f"Maximum recursion depth ({max_recursion_depth}) exceeded for property '{prop.name}'. "
                f"Skipping further nested properties."
            )
            return []

        # Skip properties without a name
        if not prop.name:
            logger.warning(f"Skipping property without name in schema '{schema_name}'")
            return []

        column_path = f"{parent_path}.{prop.name}" if parent_path else prop.name
        field_metadata = {**contract_metadata, "field": column_path}

        rules = []

        # Handle nested properties (objects)
        if prop.logicalType == "object" and prop.properties:
            for nested_prop in prop.properties:
                nested_rules = self._generate_predefined_rules_for_property(
                    nested_prop,
                    schema_name,
                    contract_metadata,
                    default_criticality,
                    column_path,
                    recursion_depth + 1,
                )
                rules.extend(nested_rules)
            return rules

        rules.extend(
            self._generate_rules_from_direct_attributes(prop, column_path, field_metadata, default_criticality)
        )

        if prop.logicalTypeOptions:
            rules.extend(
                self._generate_rules_from_logical_type_options(
                    prop, column_path, field_metadata, default_criticality, prop.logicalTypeOptions
                )
            )

        return rules

    def _generate_rules_from_direct_attributes(
        self, prop: SchemaProperty, column_path: str, field_metadata: dict, default_criticality: str
    ) -> list[dict]:
        """Generate rules from direct property attributes (required, unique)."""
        rules = []
        if prop.required:
            rules.extend(self._generate_not_null_rules_from_property(column_path, field_metadata, default_criticality))
        if prop.unique:
            rules.extend(self._generate_unique_rules_from_property(column_path, field_metadata, default_criticality))
        return rules

    def _generate_rules_from_logical_type_options(
        self, prop: SchemaProperty, column_path: str, field_metadata: dict, default_criticality: str, opts: dict
    ) -> list[dict]:
        """Generate rules from logicalTypeOptions (pattern, ranges, length, format)."""
        rules = []

        # Handle pattern constraints
        if opts.get('pattern'):
            rules.extend(
                self._generate_pattern_rules_from_options(column_path, field_metadata, default_criticality, opts)
            )

        # Handle range constraints from minimum and maximum
        if opts.get('minimum') is not None or opts.get('maximum') is not None:
            rules.extend(
                self._generate_range_rules_from_options(column_path, field_metadata, default_criticality, opts)
            )

        # Handle string length constraints
        if opts.get('minLength') is not None or opts.get('maxLength') is not None:
            rules.extend(
                self._generate_string_length_rules_from_options(column_path, field_metadata, default_criticality, opts)
            )

        # Handle date and timestamp format constraints
        if prop.logicalType in {'date', 'timestamp', 'datetime'} and opts.get('format'):
            rules.extend(
                self._generate_format_rules_from_options(
                    column_path, prop.logicalType, field_metadata, default_criticality, opts
                )
            )

        return rules

    def _generate_not_null_rules_from_property(
        self, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate not_null rules from required property constraint."""
        return [
            {
                "check": {"function": "is_not_null", "arguments": {"column": column_path}},
                "name": f"{column_path}_is_null",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "completeness",
                    "rule_type": "predefined",
                },
            }
        ]

    def _generate_unique_rules_from_property(
        self, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate uniqueness rules from ODCS property."""
        return [
            {
                "check": {"function": "is_unique", "arguments": {"columns": [column_path]}},
                "name": f"{column_path}_not_unique",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "uniqueness",
                    "rule_type": "predefined",
                },
            }
        ]

    def _generate_pattern_rules_from_options(
        self, column_path: str, contract_metadata: dict, criticality: str, opts: dict
    ) -> list[dict]:
        """Generate pattern/regex rules from logicalTypeOptions."""
        pattern = opts.get('pattern')
        if not pattern:
            return []

        return [
            {
                "check": {"function": "regex_match", "arguments": {"column": column_path, "regex": pattern}},
                "name": f"{column_path}_invalid_pattern",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "validity",
                    "rule_type": "predefined",
                },
            }
        ]

    def _generate_range_rules_from_options(
        self, column_path: str, contract_metadata: dict, criticality: str, opts: dict
    ) -> list[dict]:
        """Generate range rules from logicalTypeOptions minimum/maximum."""
        minimum = opts.get('minimum')
        maximum = opts.get('maximum')

        if minimum is None and maximum is None:
            return []

        # Check if limits are floats - use sql_expression for float constraints
        has_float_limits = (minimum is not None and isinstance(minimum, float)) or (
            maximum is not None and isinstance(maximum, float)
        )

        if minimum is not None and maximum is not None:
            if has_float_limits:
                return [
                    {
                        "check": {
                            "function": "sql_expression",
                            "arguments": {
                                "expression": f"{column_path} >= {minimum} AND {column_path} <= {maximum}",
                                "columns": [column_path],
                            },
                        },
                        "name": f"{column_path}_out_of_range",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "dimension": "validity",
                            "rule_type": "predefined",
                        },
                    }
                ]
            # Use is_in_range for non-float constraints
            return [
                {
                    "check": {
                        "function": "is_in_range",
                        "arguments": {
                            "column": column_path,
                            "min_limit": minimum,
                            "max_limit": maximum,
                        },
                    },
                    "name": f"{column_path}_out_of_range",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        if minimum is not None:
            if has_float_limits:
                return [
                    {
                        "check": {
                            "function": "sql_expression",
                            "arguments": {
                                "expression": f"{column_path} >= {minimum}",
                                "columns": [column_path],
                            },
                        },
                        "name": f"{column_path}_below_minimum",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "dimension": "validity",
                            "rule_type": "predefined",
                        },
                    }
                ]
            return [
                {
                    "check": {
                        "function": "is_aggr_not_less_than",
                        "arguments": {
                            "column": column_path,
                            "limit": minimum,
                            "aggr_type": "min",
                        },
                    },
                    "name": f"{column_path}_below_minimum",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        if maximum is not None:
            if has_float_limits:
                return [
                    {
                        "check": {
                            "function": "sql_expression",
                            "arguments": {
                                "expression": f"{column_path} <= {maximum}",
                                "columns": [column_path],
                            },
                        },
                        "name": f"{column_path}_above_maximum",
                        "criticality": criticality,
                        "user_metadata": {
                            **contract_metadata,
                            "dimension": "validity",
                            "rule_type": "predefined",
                        },
                    }
                ]
            return [
                {
                    "check": {
                        "function": "is_aggr_not_greater_than",
                        "arguments": {
                            "column": column_path,
                            "limit": maximum,
                            "aggr_type": "max",
                        },
                    },
                    "name": f"{column_path}_above_maximum",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        return []

    def _generate_string_length_rules_from_options(
        self, column_path: str, contract_metadata: dict, criticality: str, opts: dict
    ) -> list[dict]:
        """Generate string length rules from logicalTypeOptions minLength/maxLength."""
        min_length = opts.get('minLength')
        max_length = opts.get('maxLength')

        if min_length is None and max_length is None:
            return []

        if min_length is not None and max_length is not None and min_length == max_length:
            return [
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {
                            "expression": f"LENGTH({column_path}) = {min_length}",
                            "columns": [column_path],
                        },
                    },
                    "name": f"{column_path}_invalid_length",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        if min_length is not None and max_length is not None:
            return [
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {
                            "expression": f"LENGTH({column_path}) >= {min_length} AND LENGTH({column_path}) <= {max_length}",
                            "columns": [column_path],
                        },
                    },
                    "name": f"{column_path}_invalid_length",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        if min_length is not None:
            return [
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {
                            "expression": f"LENGTH({column_path}) >= {min_length}",
                            "columns": [column_path],
                        },
                    },
                    "name": f"{column_path}_too_short",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        if max_length is not None:
            return [
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {
                            "expression": f"LENGTH({column_path}) <= {max_length}",
                            "columns": [column_path],
                        },
                    },
                    "name": f"{column_path}_too_long",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]

        return []

    def _generate_format_rules_from_options(
        self, column_path: str, logical_type: str, contract_metadata: dict, criticality: str, opts: dict
    ) -> list[dict]:
        """Generate format validation rules from logicalTypeOptions format (for date/timestamp fields)."""
        format_str = opts.get('format')
        if not format_str:
            return []

        python_format = self._convert_to_python_format(format_str)

        if logical_type == 'date':
            return [
                {
                    "check": {
                        "function": "is_valid_date",
                        "arguments": {
                            "column": column_path,
                            "date_format": python_format,
                        },
                    },
                    "name": f"{column_path}_valid_date_format",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]
        if logical_type in {'timestamp', 'datetime'}:
            return [
                {
                    "check": {
                        "function": "is_valid_timestamp",
                        "arguments": {
                            "column": column_path,
                            "timestamp_format": python_format,
                        },
                    },
                    "name": f"{column_path}_valid_timestamp_format",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "predefined",
                    },
                }
            ]
        logger.warning(
            f"Format '{format_str}' specified for non-date/timestamp type '{logical_type}' on '{column_path}'"
        )
        return []

    def _convert_to_python_format(self, format_str: str) -> str:
        """
        Convert Java SimpleDateFormat or ISO 8601 format to Python strftime format.

        Common mappings:
        - yyyy -> %Y (4-digit year)
        - MM -> %m (2-digit month)
        - dd -> %d (2-digit day)
        - HH -> %H (24-hour format)
        - mm -> %M (minutes)
        - ss -> %S (seconds)
        """
        # If it's already in Python format (starts with %), return as-is
        if '%' in format_str:
            return format_str

        # Common Java SimpleDateFormat to Python strftime conversions
        conversions = {
            'yyyy': '%Y',
            'yy': '%y',
            'MM': '%m',
            'dd': '%d',
            'HH': '%H',
            'hh': '%I',
            'mm': '%M',
            'ss': '%S',
            'SSS': '%f',  # Milliseconds
            'a': '%p',  # AM/PM
        }

        result = format_str
        for java_fmt, python_fmt in conversions.items():
            result = result.replace(java_fmt, python_fmt)

        return result

    def _process_text_rules_for_schema(
        self, schema_obj: SchemaObject, schema_name: str, odcs: OpenDataContractStandard
    ) -> list[dict]:
        """Process text-based quality rules from ODCS schema using LLM."""
        if not self.llm_engine:
            return []

        rules: list[dict] = []

        contract_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
        }

        schema_info_json = self._build_schema_info_from_model(schema_obj)

        # Process property-level text rules
        for prop in schema_obj.properties or []:
            if prop.quality:
                property_text_rules = self._process_text_rules_for_property(
                    prop, schema_info_json, schema_name, contract_metadata
                )
                rules.extend(property_text_rules)

        # Process schema-level text rules
        if schema_obj.quality:
            schema_text_rules = self._process_text_rules_for_schema_level(
                schema_obj.quality, schema_info_json, schema_name, contract_metadata
            )
            rules.extend(schema_text_rules)

        return rules

    def _process_text_rules_for_property(
        self, prop: SchemaProperty, schema_info_json: str, schema_name: str, contract_metadata: dict
    ) -> list[dict]:
        """Process text rules for a property using LLM."""
        if not self.llm_engine:
            return []

        rules = []
        for quality_rule in prop.quality or []:
            if quality_rule.type == 'text' and quality_rule.description:
                logger.info(f"Processing text rule for property '{prop.name}': {quality_rule.description}")

                prediction = self.llm_engine.detect_business_rules_with_llm(
                    user_input=quality_rule.description, schema_info=schema_info_json
                )

                llm_rules_json = prediction.quality_rules
                llm_rules = json.loads(llm_rules_json) if isinstance(llm_rules_json, str) else llm_rules_json

                for rule in llm_rules:
                    rule["user_metadata"] = {
                        **contract_metadata,
                        **rule.get("user_metadata", {}),
                        "schema": schema_name,
                        "field": prop.name,
                        "rule_type": "text_llm",
                        "text_expectation": quality_rule.description,
                    }
                    rules.append(rule)

        return rules

    def _process_text_rules_for_schema_level(
        self, quality_list: list[DataQuality] | None, schema_info_json: str, schema_name: str, contract_metadata: dict
    ) -> list[dict]:
        """Process schema-level text rules using LLM."""
        if not self.llm_engine:
            return []

        rules = []
        for quality_rule in quality_list or []:
            if quality_rule.type == 'text' and quality_rule.description:
                logger.info(f"Processing text rule for schema '{schema_name}': {quality_rule.description}")

                prediction = self.llm_engine.detect_business_rules_with_llm(
                    user_input=quality_rule.description, schema_info=schema_info_json
                )

                llm_rules_json = prediction.quality_rules
                llm_rules = json.loads(llm_rules_json) if isinstance(llm_rules_json, str) else llm_rules_json

                for rule in llm_rules:
                    rule["user_metadata"] = {
                        **contract_metadata,
                        **rule.get("user_metadata", {}),
                        "schema": schema_name,
                        "rule_type": "text_llm",
                        "text_expectation": quality_rule.description,
                    }
                    rules.append(rule)

        return rules

    def _build_schema_info_from_model(self, schema_obj: SchemaObject) -> str:
        """
        Build schema information from ODCS schema object for LLM context.

        Returns JSON string with schema structure for LLM processing.
        """
        columns = []

        def _extract_columns(props: list[SchemaProperty] | None, prefix: str = "") -> None:
            """Recursively extract column information from properties."""
            for prop in props or []:
                # Skip properties without a name
                if not prop.name:
                    continue

                column_path = f"{prefix}.{prop.name}" if prefix else prop.name
                col_info = {
                    "name": column_path,
                    "type": prop.logicalType,
                }
                if prop.description:
                    col_info["description"] = prop.description
                columns.append(col_info)

                # Recursively process nested properties
                if prop.logicalType == 'object' and hasattr(prop, 'properties') and prop.properties:
                    _extract_columns(prop.properties, column_path)

        _extract_columns(schema_obj.properties)

        schema_info = {"name": schema_obj.name, "columns": columns}

        return json.dumps(schema_info)

    def _process_explicit_rules_for_schema(
        self, schema_obj: SchemaObject, schema_name: str, odcs: OpenDataContractStandard, default_criticality: str
    ) -> list[dict]:
        """Process explicitly defined DQX quality rules from ODCS schema."""
        rules = []

        # Process property-level explicit rules
        for prop in schema_obj.properties or []:
            if prop.quality:
                rules.extend(self._extract_property_explicit_rules(prop, schema_name, odcs, default_criticality))

        # Process schema-level explicit rules
        if schema_obj.quality:
            rules.extend(
                self._extract_schema_explicit_rules(schema_obj.quality, schema_name, odcs, default_criticality)
            )

        return rules

    def _extract_property_explicit_rules(
        self, prop: SchemaProperty, schema_name: str, odcs: OpenDataContractStandard, default_criticality: str
    ) -> list[dict]:
        """Extract explicit DQX rules from property quality definitions."""
        rules: list[dict] = []

        if prop.quality is None:
            return rules

        for quality_rule in prop.quality:
            if self._is_dqx_explicit_rule(quality_rule):
                rule = self._build_explicit_rule_from_quality(
                    quality_rule, prop.name, schema_name, odcs, default_criticality
                )
                if rule:
                    rules.append(rule)
        return rules

    def _extract_schema_explicit_rules(
        self,
        quality_list: list[DataQuality],
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Extract explicit DQX rules from schema quality definitions."""
        rules = []
        for quality_rule in quality_list:
            if self._is_dqx_explicit_rule(quality_rule):
                rule = self._build_explicit_rule_from_quality(
                    quality_rule, None, schema_name, odcs, default_criticality
                )
                if rule:
                    rules.append(rule)
        return rules

    def _is_dqx_explicit_rule(self, quality_rule: DataQuality) -> bool:
        """Check if a quality rule is a DQX explicit rule with implementation.

        In ODCS v3.x, implementation is always a dict when loaded directly.
        """
        if quality_rule.type != 'custom' or quality_rule.engine != 'dqx':
            return False
        if not hasattr(quality_rule, 'implementation') or not quality_rule.implementation:
            return False
        impl = quality_rule.implementation
        # impl is always a dict in ODCS v3.x
        return isinstance(impl, dict) and 'check' in impl

    def _build_explicit_rule_from_quality(
        self,
        quality_rule: DataQuality,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> dict | None:
        """Build a DQX rule from a quality rule's implementation."""
        return self._build_explicit_rule_from_implementation(
            quality_rule.implementation, property_name, schema_name, odcs, default_criticality
        )

    def _build_explicit_rule_from_implementation(
        self,
        impl: str | dict[str, Any] | None,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> dict | None:
        """Build a DQX rule from an explicit implementation in the contract.

        Raises ODCSContractError if the implementation structure is invalid.
        """
        try:
            check, name, criticality = self._extract_impl_attributes(impl, default_criticality)
            if check is None:
                logger.warning("Implementation missing 'check' attribute, skipping rule")
                return None

            return self._build_rule_dict(check, name, criticality, schema_name, property_name, odcs)
        except (AttributeError, KeyError, TypeError) as e:
            # Malformed contract structure - fail fast
            raise ODCSContractError(
                f"Invalid explicit rule implementation structure in schema '{schema_name}': {e}"
            ) from e

    def _extract_impl_attributes(self, impl: str | dict[str, Any] | None, default_criticality: str):
        """Extract check, name, and criticality from implementation dict.

        In ODCS v3.x, implementation is always a dict when loaded directly.
        """
        if not impl or not isinstance(impl, dict):
            raise TypeError(
                f"Unexpected implementation type: {type(impl).__name__}. "
                f"Expected dict, which is the standard format for ODCS v3.x implementations."
            )
        check = impl.get("check")
        name = impl.get("name", "unnamed_rule")
        criticality = impl.get("criticality", default_criticality)
        return check, name, criticality

    def _build_rule_dict(
        self,
        check_dict: dict,
        name: str,
        criticality: str,
        schema_name: str,
        property_name: str | None,
        odcs: OpenDataContractStandard,
    ) -> dict:
        """Build the final rule dictionary with metadata."""
        user_metadata: dict = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "explicit",
        }
        if property_name:
            user_metadata["field"] = property_name

        rule = {
            "check": check_dict,
            "name": name,
            "criticality": criticality,
            "user_metadata": user_metadata,
        }
        return rule
