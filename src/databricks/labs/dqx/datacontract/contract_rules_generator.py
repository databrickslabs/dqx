"""
Data Contract to DQX Rules Generator.

This module provides functionality to generate DQX quality rules from data contract
specifications like ODCS (Open Data Contract Standard).
"""

import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.telemetry import telemetry_logger

# Conditional imports for Data Contract CLI
try:
    from datacontract.data_contract import DataContract
    from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model, Quality

    DATACONTRACT_ENABLED = True
except ImportError:
    DATACONTRACT_ENABLED = False

# Type checking imports (never evaluated at runtime, only by static type checkers)
if TYPE_CHECKING:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine
    from datacontract.data_contract import DataContract
    from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model, Quality

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
        llm_engine: 'DQLLMEngine | None' = None,
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initialize the DataContractRulesGenerator.

        Args:
            workspace_client: Databricks WorkspaceClient instance.
            llm_engine: Optional LLM engine for processing text-based quality expectations.
            custom_check_functions: Optional dictionary of custom check functions.
        """
        super().__init__(workspace_client=workspace_client)
        self.llm_engine = llm_engine
        self.custom_check_functions = custom_check_functions

    @telemetry_logger("datacontract", "generate_rules_from_contract")
    def generate_rules_from_contract(
        self,
        contract: "DataContract | None" = None,
        contract_file: str | None = None,
        contract_format: str = "odcs",
        generate_implicit_rules: bool = True,
        process_text_rules: bool = True,
        default_criticality: str = "error",
    ) -> list[dict]:
        """
        Generate DQX quality rules from a data contract specification.

        Parses a data contract (currently supporting ODCS v3.0.x) and generates rules based on
        schema properties, explicit quality definitions, and text-based expectations.

        Args:
            contract: Pre-loaded DataContract object from datacontract-cli.
            contract_file: Path to contract YAML file (local, volume, or workspace).
            contract_format: Contract format specification (default is "odcs").
            generate_implicit_rules: Whether to generate rules from schema properties.
            process_text_rules: Whether to process text-based expectations using LLM.
            default_criticality: Default criticality level for generated rules (default is "error").

        Returns:
            A list of dictionaries representing the generated DQX quality rules.

        Raises:
            ImportError: If datacontract-cli is not installed.
            ValueError: If neither or both parameters are provided, or format not supported.

        Note:
            Exactly one of 'contract' or 'contract_file' must be provided.
        """
        self._validate_inputs(contract, contract_file, contract_format)
        spec = self._load_contract_spec(contract, contract_file)
        self._validate_contract_spec(spec)

        dq_rules = self._generate_all_rules(spec, generate_implicit_rules, process_text_rules, default_criticality)
        self._validate_generated_rules(dq_rules)

        return dq_rules

    def _validate_inputs(
        self, contract: "DataContract | None", contract_file: str | None, contract_format: str
    ) -> None:
        """Validate input parameters."""
        if not DATACONTRACT_ENABLED:
            raise ImportError(
                "Data contract functionality requires datacontract-cli. "
                "Install with: pip install 'databricks-labs-dqx[datacontract]'"
            )

        if contract is None and contract_file is None:
            raise ValueError("Either 'contract' or 'contract_file' must be provided")

        if contract is not None and contract_file is not None:
            raise ValueError("Cannot provide both 'contract' and 'contract_file'")

        if contract_format != "odcs":
            raise ValueError(f"Contract format '{contract_format}' not supported. Currently only 'odcs' is supported.")

    def _load_contract_spec(
        self, contract: "DataContract | None", contract_file: str | None
    ) -> "DataContractSpecification":
        """Load DataContractSpecification from contract or file."""
        if contract_file is not None:
            data_contract = DataContract(data_contract_file=contract_file)
            return data_contract.get_data_contract_specification()

        # Contract is guaranteed to be not None by validation
        assert contract is not None
        return contract.get_data_contract_specification()

    def _validate_contract_spec(self, spec: "DataContractSpecification") -> None:
        """Validate contract specification using datacontract-cli lint."""
        lint_result = (DataContract(data_contract=spec.model_dump())).lint()  # type: ignore[arg-type]
        if lint_result.result != "passed":
            checks = lint_result.checks or []
            errors = [f"{c.check}: {c.details}" for c in checks if hasattr(c, "check") and c.result != "passed"]
            if errors:
                logger.warning(f"Contract validation warnings: {errors}")

        contract_version = getattr(spec.info, "version", "unknown") if spec.info else "unknown"
        logger.info(f"Parsing data contract '{spec.id or 'unknown'}' v{contract_version}")

    def _generate_all_rules(
        self,
        spec: "DataContractSpecification",
        generate_implicit_rules: bool,
        process_text_rules: bool,
        default_criticality: str,
    ) -> list[dict]:
        """Generate all rules from contract models."""
        dq_rules = []

        for model_name, model in (spec.models or {}).items():
            if generate_implicit_rules:
                implicit_rules = self._generate_implicit_rules_for_model(model, model_name, spec, default_criticality)
                dq_rules.extend(implicit_rules)

            if process_text_rules:
                text_rules = self._process_text_rules_for_model(model, model_name, spec, default_criticality)
                dq_rules.extend(text_rules)

            explicit_rules = self._process_explicit_rules_for_model(model, model_name, spec, default_criticality)
            dq_rules.extend(explicit_rules)

        return dq_rules

    def _validate_generated_rules(self, dq_rules: list[dict]) -> None:
        """Validate generated DQX rules."""
        if dq_rules:
            status = DQEngine.validate_checks(dq_rules, self.custom_check_functions)
            if status.has_errors:
                logger.warning(f"Generated rules have validation errors: {status.errors}")
            else:
                logger.info(f"Successfully generated {len(dq_rules)} DQX rules from data contract")

    @staticmethod
    def _get_contract_version(spec: DataContractSpecification) -> str:
        """Extract contract version from specification."""
        return getattr(spec.info, 'version', 'unknown') if spec.info else 'unknown'

    def _generate_implicit_rules_for_model(
        self, model: Model, model_name: str, spec: DataContractSpecification, default_criticality: str
    ) -> list[dict]:
        """Generate implicit rules from all fields in a model."""
        rules = []
        for field_name, field in (model.fields or {}).items():
            field_rules = self._generate_implicit_rules_for_field(
                field, field_name, model_name, spec, default_criticality
            )
            rules.extend(field_rules)
        return rules

    def _generate_implicit_rules_for_field(
        self,
        field: Field,
        field_name: str,
        model_name: str,
        spec: DataContractSpecification,
        default_criticality: str,
        parent_path: str = "",
    ) -> list[dict]:
        """Generate implicit DQ rules from a field's constraints."""
        # Build full column path
        column_path = f"{parent_path}.{field_name}" if parent_path else field_name

        contract_metadata = {
            "contract_id": spec.id or "unknown",
            "contract_version": self._get_contract_version(spec),
            "model": model_name,
            "field": column_path,
        }

        rules = []

        # If field has nested fields, recurse
        if field.fields:
            for nested_name, nested_field in field.fields.items():
                nested_rules = self._generate_implicit_rules_for_field(
                    nested_field, nested_name, model_name, spec, default_criticality, column_path
                )
                rules.extend(nested_rules)
            return rules

        # Generate rules based on field constraints
        rules.extend(self._generate_required_rules(field, column_path, contract_metadata, default_criticality))
        rules.extend(self._generate_unique_rules(field, column_path, contract_metadata, default_criticality))
        rules.extend(self._generate_enum_rules(field, column_path, contract_metadata, default_criticality))
        rules.extend(self._generate_pattern_rules(field, column_path, contract_metadata, default_criticality))
        rules.extend(self._generate_range_rules_from_field(field, column_path, contract_metadata, default_criticality))
        rules.extend(self._generate_length_rules(field, column_path, contract_metadata, default_criticality))
        rules.extend(self._generate_format_rules_from_field(field, column_path, contract_metadata, default_criticality))

        return rules

    def _generate_required_rules(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate required/not_null rules."""
        if not field.required:
            return []

        return [
            {
                "check": {"function": "is_not_null", "arguments": {"column": column_path}},
                "name": f"{column_path}_is_null",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "completeness",
                    "rule_type": "implicit",
                },
            }
        ]

    def _generate_unique_rules(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate uniqueness rules."""
        if not field.unique:
            return []

        return [
            {
                "check": {"function": "is_unique", "arguments": {"columns": [column_path]}},
                "name": f"{column_path}_not_unique",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "uniqueness",
                    "rule_type": "implicit",
                },
            }
        ]

    def _generate_enum_rules(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate enum/valid values rules."""
        if not field.enum:
            return []

        return [
            {
                "check": {"function": "is_in_list", "arguments": {"column": column_path, "allowed": field.enum}},
                "name": f"{column_path}_invalid_value",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "validity",
                    "rule_type": "implicit",
                },
            }
        ]

    def _generate_pattern_rules(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate pattern/regex rules."""
        if not field.pattern:
            return []

        return [
            {
                "check": {"function": "regex_match", "arguments": {"column": column_path, "regex": field.pattern}},
                "name": f"{column_path}_invalid_pattern",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "validity",
                    "rule_type": "implicit",
                },
            }
        ]

    def _generate_range_rules_from_field(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate range rules from minimum/maximum constraints."""
        if field.minimum is None and field.maximum is None:
            return []

        if field.minimum is not None and field.maximum is not None:
            return [
                {
                    "check": {
                        "function": "is_in_range",
                        "arguments": {
                            "column": column_path,
                            "min_limit": field.minimum,
                            "max_limit": field.maximum,
                        },
                    },
                    "name": f"{column_path}_out_of_range",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "implicit",
                    },
                }
            ]

        if field.minimum is not None:
            return [
                {
                    "check": {
                        "function": "is_not_less_than",
                        "arguments": {"column": column_path, "limit": field.minimum},
                    },
                    "name": f"{column_path}_below_minimum",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "implicit",
                    },
                }
            ]

        return [
            {
                "check": {
                    "function": "is_not_greater_than",
                    "arguments": {"column": column_path, "limit": field.maximum},
                },
                "name": f"{column_path}_above_maximum",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "validity",
                    "rule_type": "implicit",
                },
            }
        ]

    def _generate_length_rules(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate string length rules from minLength/maxLength using SQL expressions."""
        rules = []

        min_length = getattr(field, "minLength", None)
        max_length = getattr(field, "maxLength", None)

        # Generate minLength rule
        if min_length is not None:
            rules.append(
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {
                            "column": column_path,
                            "expression": f"LENGTH({column_path}) >= {min_length}",
                        },
                    },
                    "name": f"{column_path}_min_length",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "implicit",
                    },
                }
            )

        # Generate maxLength rule
        if max_length is not None:
            rules.append(
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {
                            "column": column_path,
                            "expression": f"LENGTH({column_path}) <= {max_length}",
                        },
                    },
                    "name": f"{column_path}_max_length",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "implicit",
                    },
                }
            )

        return rules

    def _generate_format_rules_from_field(
        self, field: Field, column_path: str, contract_metadata: dict, criticality: str
    ) -> list[dict]:
        """Generate format validation rules for dates/timestamps."""
        if not field.format:
            return []

        # Detect if it's a timestamp based on format (time components: %H, %M, %S, %I, %p)
        # Support both Python strftime (%H) and Java SimpleDateFormat (HH) formats
        time_components = ('%H', '%M', '%S', '%I', '%p', 'HH', 'mm', 'ss', 'hh')
        is_timestamp = any(time_component in field.format for time_component in time_components)

        if is_timestamp:
            return [
                {
                    "check": {
                        "function": "is_valid_timestamp",
                        "arguments": {"column": column_path, "timestamp_format": field.format},
                    },
                    "name": f"{column_path}_invalid_timestamp_format",
                    "criticality": criticality,
                    "user_metadata": {
                        **contract_metadata,
                        "dimension": "validity",
                        "rule_type": "implicit",
                    },
                }
            ]

        # Assume it's a date format
        return [
            {
                "check": {
                    "function": "is_valid_date",
                    "arguments": {"column": column_path, "date_format": field.format},
                },
                "name": f"{column_path}_invalid_date_format",
                "criticality": criticality,
                "user_metadata": {
                    **contract_metadata,
                    "dimension": "validity",
                    "rule_type": "implicit",
                },
            }
        ]

    def _process_text_rules_for_model(
        self, model: Model, model_name: str, spec: DataContractSpecification, _default_criticality: str
    ) -> list[dict]:
        """Process text-based quality expectations using LLM for a model."""
        if self.llm_engine is None:
            return []

        contract_metadata = {
            "contract_id": spec.id or "unknown",
            "contract_version": self._get_contract_version(spec),
            "model": model_name,
        }
        schema_info = self._build_schema_info_from_model(model)

        rules = []
        for field_name, field in (model.fields or {}).items():
            rules.extend(self._process_text_rules_for_field(field, field_name, contract_metadata, schema_info))

        return rules

    def _process_text_rules_for_field(
        self, field: Field, field_name: str, contract_metadata: dict, schema_info: str, parent_path: str = ""
    ) -> list[dict]:
        """Process text-based rules for a single field."""
        column_path = f"{parent_path}.{field_name}" if parent_path else field_name

        # Recurse for nested fields
        if field.fields:
            rules = []
            for nested_name, nested_field in field.fields.items():
                rules.extend(
                    self._process_text_rules_for_field(
                        nested_field, nested_name, contract_metadata, schema_info, column_path
                    )
                )
            return rules

        # Process quality checks for this field
        if not field.quality:
            return []

        rules = []
        for quality in field.quality:
            if self._is_text_quality(quality):
                text_expectation = quality.description or ""
                if text_expectation:
                    rules.extend(
                        self._process_single_text_rule_from_field(
                            field, column_path, text_expectation, contract_metadata, schema_info
                        )
                    )

        return rules

    def _is_text_quality(self, quality: Quality) -> bool:
        """Check if quality is a text-based expectation."""
        return quality.type == "text" if hasattr(quality, "type") else False

    def _process_single_text_rule_from_field(
        self, field: Field, column_path: str, text_expectation: str, contract_metadata: dict, schema_info: str
    ) -> list[dict]:
        """Process a single text rule and add metadata."""
        try:
            generated_rules = self._generate_rules_from_text_field(field, column_path, text_expectation, schema_info)
            return [
                self._add_text_rule_metadata(rule, column_path, text_expectation, contract_metadata)
                for rule in generated_rules
            ]
        except Exception as e:
            logger.warning(f"Failed to process text rule for {column_path}: {e}")
            return []

    def _generate_rules_from_text_field(
        self, field: Field, column_path: str, text_expectation: str, schema_info: str
    ) -> list[dict]:
        """Generate DQX rules from text expectation using LLM."""
        if self.llm_engine is None:
            return []

        # Build context for LLM
        context_info = f"Field: {column_path}"
        if field.type:
            context_info += f", Type: {field.type}"
        if field.description:
            context_info += f", Description: {field.description}"

        user_input = f"{context_info}\nExpectation: {text_expectation}"

        # Generate rules using LLM
        logger.info(f"Processing text rule for {column_path}: {text_expectation}")
        prediction = self.llm_engine.get_business_rules_with_llm(user_input=user_input, schema_info=schema_info)

        return json.loads(prediction.quality_rules)

    def _add_text_rule_metadata(
        self, rule: dict, column_path: str, text_expectation: str, contract_metadata: dict
    ) -> dict:
        """Add contract metadata to a text-generated rule."""
        if 'user_metadata' not in rule:
            rule['user_metadata'] = {}
        rule['user_metadata'].update(
            {
                **contract_metadata,
                "field": column_path,
                "rule_type": "text_llm",
                "text_expectation": text_expectation,
            }
        )
        return rule

    def _process_explicit_rules_for_model(
        self, model: Model, model_name: str, spec: DataContractSpecification, _default_criticality: str
    ) -> list[dict]:
        """Process explicit DQX format rules from a model."""
        contract_metadata = {
            "contract_id": spec.id or "unknown",
            "contract_version": self._get_contract_version(spec),
            "model": model_name,
        }

        rules = []
        for field_name, field in (model.fields or {}).items():
            rules.extend(self._extract_explicit_rules_for_field(field, field_name, contract_metadata))

        return rules

    def _extract_explicit_rules_for_field(
        self, field: Field, field_name: str, contract_metadata: dict, parent_path: str = ""
    ) -> list[dict]:
        """Extract explicit DQX rules from a field's quality checks."""
        column_path = f"{parent_path}.{field_name}" if parent_path else field_name

        # Recurse for nested fields
        if field.fields:
            rules = []
            for nested_name, nested_field in field.fields.items():
                rules.extend(
                    self._extract_explicit_rules_for_field(nested_field, nested_name, contract_metadata, column_path)
                )
            return rules

        # Extract explicit DQX rules from quality checks
        if not field.quality:
            return []

        rules = []
        for quality in field.quality:
            if self._is_dqx_quality(quality):
                dqx_rule = self._extract_dqx_rule_from_quality(quality, column_path, contract_metadata)
                if dqx_rule:
                    rules.append(dqx_rule)

        return rules

    def _is_dqx_quality(self, quality: Quality) -> bool:
        """Check if quality is a DQX custom check."""
        # Check if quality has type='custom' and specification with DQX format
        if not hasattr(quality, "type") or quality.type != "custom":
            return False

        # DQX rules are in quality.specification or quality.model_extra['specification']
        spec = getattr(quality, "specification", None) or (quality.model_extra or {}).get("specification", {})
        return isinstance(spec, dict) and "check" in spec

    def _extract_dqx_rule_from_quality(
        self, quality: Quality, column_path: str, contract_metadata: dict
    ) -> dict | None:
        """Extract a DQX rule from a Quality object."""
        # Get the specification (DQX rule definition)
        spec = getattr(quality, "specification", None) or (quality.model_extra or {}).get("specification", {})

        if not isinstance(spec, dict) or "check" not in spec:
            return None

        dqx_rule = spec.copy()
        if 'user_metadata' not in dqx_rule:
            dqx_rule['user_metadata'] = {}

        dqx_rule['user_metadata'].update({"field": column_path, "rule_type": "explicit", **contract_metadata})

        logger.info(f"Added explicit DQX rule for {column_path}")
        return dqx_rule

    def _build_schema_info_from_model(self, model: Model) -> str:
        """
        Build schema info JSON string from Model for LLM context.

        Currently provides basic field name and type.
        """
        columns = []
        for field_name, field in (model.fields or {}).items():
            col_info = {"name": field_name, "type": field.type or "string"}

            # TODO: Future enhancement - include field constraints for richer LLM context
            # This would help LLM avoid duplicating implicit rules and generate
            # more complementary business logic rules.

            columns.append(col_info)

        schema_dict = {"columns": columns}
        return json.dumps(schema_dict)
