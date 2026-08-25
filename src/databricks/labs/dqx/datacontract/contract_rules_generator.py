"""Data Contract to DQX Rules Generator.

Generates DQX quality rules from ODCS (Open Data Contract Standard) v3.x contracts.

For schema validation we require every property to have physicalType set to a Unity Catalog
data type (e.g. STRING, INT, ARRAY<STRING>, DECIMAL(10,2)). No ODCS→Unity mapping is performed.
See: https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes
"""

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import pyspark.sql.functions as F
import yaml
from pyspark.sql import Column

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
from databricks.labs.dqx.errors import InvalidPhysicalTypeError, ODCSContractError, ParameterError
from databricks.labs.dqx.telemetry import telemetry_logger
from databricks.labs.dqx.package_utils import missing_required_packages
from databricks.labs.dqx.utils import sanitize_for_logging

# DQLLMEngine is referenced only as a type annotation. Eagerly importing it
# requires installation of [llm] extras which may not be installed or wanted
# by the user. If llm_engine is specified and [llm] extras are not installed,
# an error is raised when instantiating the DataContractRulesGenerator.
if TYPE_CHECKING:  # pragma: no cover
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class DataContractRulesGenerator(DQEngineBase):
    """
    Generator for DQX quality rules from ODCS v3.x data contracts.

    Schema validation requires every property to have physicalType set to a Unity Catalog type.
    We do not map ODCS types; invalid or missing physicalType raises InvalidPhysicalTypeError.
    Supports predefined rules from schema, explicit quality sections, and LLM-based expectations.
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
        generate_schema_validation: bool = True,
        strict_schema_validation: bool = True,
        default_criticality: str = "error",
    ) -> list[dict]:
        """
        Generate DQX quality rules from an ODCS v3.x data contract.

        Parses an ODCS v3.x contract natively and generates rules based on schema properties,
        logicalTypeOptions constraints, explicit quality definitions, and text-based expectations.
        When the contract defines a schema and generate_schema_validation is True, one dataset-level
        has_valid_schema rule per schema is generated. strict_schema_validation is passed as the
        strict argument to has_valid_schema (default True = exact match).

        Explicit rules carry their quality entry's ``dimension``, ``description`` and flattened
        ``customProperties`` in ``user_metadata`` alongside the contract provenance keys, so
        contract-authored context is not lost on import. A quality ``owner`` or ``steward``
        custom property (or, failing that, the first ODCS ``team`` member with an
        owner/steward-like role) is emitted as a top-level ``owner`` on the generated rule
        for Rules Registry ownership.

        Args:
            contract: Pre-loaded DataContract object from datacontract-cli. Can be created with:
                - DataContract(data_contract_file=path) - from a file path
                - DataContract(data_contract_str=yaml_string) - from a YAML/JSON string
                Either `contract` or `contract_file` must be provided.
            contract_file: Path to contract YAML/JSON file (local, volume, or workspace). Either `contract` or `contract_file` must be provided.
            contract_format: Contract format specification (default is "odcs"). Only "odcs" is supported.
            generate_predefined_rules: Whether to generate rules from schema properties (default True). Set to False to only generate explicit rules.
            process_text_rules: Whether to process text-based expectations using LLM (default True). Requires llm_engine to be provided in __init__.
            generate_schema_validation: Whether to generate dataset-level has_valid_schema rules from the contract schema (default True).
            strict_schema_validation: Passed as the strict argument to has_valid_schema (default True = exact columns, order, types; False = permissive).
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

        dq_rules = self._generate_all_rules(
            odcs,
            generate_predefined_rules,
            process_text_rules,
            generate_schema_validation,
            strict_schema_validation,
            default_criticality,
        )
        self._apply_contract_owners(dq_rules, odcs)
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
        generate_schema_validation: bool,
        strict_schema_validation: bool,
        default_criticality: str,
    ) -> list[dict]:
        """Generate all rules from ODCS v3.x contract schemas."""
        dq_rules = []

        # ODCS v3.x uses schema_ list instead of models dict
        for schema_obj in odcs.schema_ or []:
            schema_name = schema_obj.name or "unknown_schema"

            if generate_schema_validation:
                schema_validation_rules = self._generate_schema_validation_rules_for_schema(
                    schema_obj, schema_name, odcs, default_criticality, strict_schema_validation
                )
                dq_rules.extend(schema_validation_rules)

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

            library_rules = self._process_library_rules_for_schema(schema_obj, schema_name, odcs, default_criticality)
            dq_rules.extend(library_rules)

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

    # Schema validation: require physicalType to be a Unity Catalog type; no mapping.
    # See: https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes
    # VOID and OBJECT are excluded: VOID (NullType) cannot be stored in Delta Lake,
    # and OBJECT cannot be stored in table columns. Both would fail StructType.fromDDL().
    _UNITY_SIMPLE_TYPES: frozenset[str] = frozenset(
        {
            "STRING",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "BINARY",
            "VARIANT",
            "SMALLINT",
            "TINYINT",
        }
    )
    # GEOGRAPHY, GEOMETRY, INTERVAL: we allow by prefix only; inner content is not validated.
    # Malformed values (e.g. GEOGRAPHY(GARBAGE)) will pass here but may fail at DDL parse or runtime.
    _UNITY_COMPLEX_PREFIXES: tuple[str, ...] = (
        "GEOGRAPHY(",
        "GEOMETRY(",
        "INTERVAL ",
    )
    _UNITY_DECIMAL_PATTERN = re.compile(r"^DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.IGNORECASE)
    _MAX_TYPE_RECURSION_DEPTH = 50

    @classmethod
    def _extract_content_in_angle_brackets(cls, type_str: str) -> str:
        """Return the substring between the first '<' and its matching '>'; uses bracket counting."""
        start = type_str.find("<")
        if start < 0:
            raise InvalidPhysicalTypeError(f"physicalType '{type_str}' has no opening angle bracket for complex type.")
        depth = 1
        i = start + 1
        while i < len(type_str) and depth > 0:
            if type_str[i] == "<":
                depth += 1
            elif type_str[i] == ">":
                depth -= 1
            i += 1
        if depth != 0:
            raise InvalidPhysicalTypeError(f"physicalType '{type_str}' has unmatched angle brackets.")
        return type_str[start + 1 : i - 1].strip()

    @classmethod
    def _split_top_level_comma(cls, content: str) -> list[str]:
        """Split content by commas only when bracket depth is 0."""
        parts: list[str] = []
        depth = 0
        start = 0
        for i, char in enumerate(content):
            if char in "<(":
                depth += 1
            elif char in ">)":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append(content[start:i].strip())
                start = i + 1
        parts.append(content[start:].strip())
        return parts

    @classmethod
    def _validate_decimal(cls, decimal_match: re.Match[str]) -> str:
        """Validate DECIMAL precision/scale and return normalized string."""
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision < 1 or precision > 38:
            raise InvalidPhysicalTypeError(
                f"DECIMAL precision must be between 1 and 38 (Spark limit), got {precision}."
            )
        if scale < 0 or scale > precision:
            raise InvalidPhysicalTypeError(f"DECIMAL scale must be between 0 and precision ({precision}), got {scale}.")
        return f"DECIMAL({precision},{scale})"

    @classmethod
    def _validate_array_type(cls, type_str_stripped: str, type_str: str, depth: int) -> str:
        """Validate ARRAY<T> and return normalized string."""
        inner = cls._extract_content_in_angle_brackets(type_str_stripped)
        if not inner:
            raise InvalidPhysicalTypeError(
                f"physicalType '{type_str}' has empty ARRAY element type. Use ARRAY<element_type>, e.g. ARRAY<STRING>."
            )
        validated_inner = cls._validate_unity_physical_type(inner, depth + 1)
        return f"ARRAY<{validated_inner}>"

    @classmethod
    def _validate_map_type(cls, type_str_stripped: str, type_str: str, depth: int) -> str:
        """Validate MAP<K,V> and return normalized string."""
        inner = cls._extract_content_in_angle_brackets(type_str_stripped)
        if not inner:
            raise InvalidPhysicalTypeError(
                f"physicalType '{type_str}' has empty MAP key/value types. Use MAP<key_type,value_type>, e.g. MAP<STRING,INT>."
            )
        key_value = cls._split_top_level_comma(inner)
        if len(key_value) != 2:
            raise InvalidPhysicalTypeError(
                f"physicalType MAP must have exactly two type parameters (key, value), got {len(key_value)}."
            )
        validated_key = cls._validate_unity_physical_type(key_value[0], depth + 1)
        validated_val = cls._validate_unity_physical_type(key_value[1], depth + 1)
        return f"MAP<{validated_key},{validated_val}>"

    @classmethod
    def _parse_struct_field_and_validate(cls, field_spec: str, type_str: str, depth: int) -> str:
        """Parse one STRUCT field 'name: type' and return 'name:validated_type'."""
        if not field_spec:
            raise InvalidPhysicalTypeError(f"physicalType STRUCT has empty field spec in '{type_str}'.")
        colon_at = -1
        bracket_depth = 0
        for idx, char in enumerate(field_spec):
            if char in "<(":
                bracket_depth += 1
            elif char in ">)":
                bracket_depth -= 1
            elif char == ":" and bracket_depth == 0:
                colon_at = idx
                break
        if colon_at < 0:
            raise InvalidPhysicalTypeError(f"physicalType STRUCT field must be 'name: type', got '{field_spec}'.")
        field_name = field_spec[:colon_at].strip()
        field_type = field_spec[colon_at + 1 :].strip()
        if not field_name or not field_type:
            raise InvalidPhysicalTypeError(f"physicalType STRUCT field has missing name or type in '{field_spec}'.")
        validated_type = cls._validate_unity_physical_type(field_type, depth + 1)
        return f"{field_name}:{validated_type}"

    @classmethod
    def _validate_struct_type(cls, type_str_stripped: str, type_str: str, depth: int) -> str:
        """Validate STRUCT<...> and return normalized string."""
        inner = cls._extract_content_in_angle_brackets(type_str_stripped)
        if not inner:
            raise InvalidPhysicalTypeError(
                f"physicalType '{type_str}' has empty STRUCT fields. Use STRUCT<name:type,...>, e.g. STRUCT<id:STRING,count:INT>."
            )
        fields = cls._split_top_level_comma(inner)
        validated_parts = [cls._parse_struct_field_and_validate(spec, type_str, depth) for spec in fields]
        return "STRUCT<" + ",".join(validated_parts) + ">"

    @classmethod
    def _validate_unity_physical_type(cls, type_str: str, depth: int = 0) -> str:
        """Validate and return normalized Unity Catalog type; inner types are validated recursively.

        DECIMAL precision/scale are bounded by Spark's limit (precision <= 38, scale <= precision).
        Raises InvalidPhysicalTypeError if invalid or recursion depth exceeded.
        """
        if depth > cls._MAX_TYPE_RECURSION_DEPTH:
            raise InvalidPhysicalTypeError(
                f"physicalType nesting exceeds maximum depth ({cls._MAX_TYPE_RECURSION_DEPTH}). "
                "Check for malformed or excessively nested types."
            )
        type_str_stripped = type_str.strip()
        if not type_str_stripped:
            raise InvalidPhysicalTypeError(
                "physicalType must be set to a Unity Catalog data type (e.g. STRING, INT, ARRAY<STRING>). "
                "See: https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes"
            )
        type_upper = type_str_stripped.upper()
        if type_upper in cls._UNITY_SIMPLE_TYPES:
            return type_upper
        decimal_match = cls._UNITY_DECIMAL_PATTERN.match(type_str_stripped)
        if decimal_match:
            return cls._validate_decimal(decimal_match)
        if type_upper.startswith("ARRAY<"):
            return cls._validate_array_type(type_str_stripped, type_str, depth)
        if type_upper.startswith("MAP<"):
            return cls._validate_map_type(type_str_stripped, type_str, depth)
        if type_upper.startswith("STRUCT<"):
            return cls._validate_struct_type(type_str_stripped, type_str, depth)
        if any(type_upper.startswith(prefix) for prefix in cls._UNITY_COMPLEX_PREFIXES):
            return type_upper
        allowed_hint = (
            "Allowed types include: STRING, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, "
            "DECIMAL(p,s), ARRAY<T>, MAP<K,V>, STRUCT<...>. "
        )
        raise InvalidPhysicalTypeError(
            f"physicalType '{type_str}' is not a valid Unity Catalog data type. {allowed_hint}"
            "See: https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes"
        )

    def _schema_object_to_ddl(self, schema_obj: SchemaObject, schema_name: str = "") -> str:
        """Build a Spark/Unity Catalog DDL string from an ODCS schema object.

        Every property must have physicalType set to a Unity Catalog type. Raises
        InvalidPhysicalTypeError (with schema and property name) when missing or invalid.
        """
        parts: list[str] = []
        for prop in schema_obj.properties or []:
            if not prop.name:
                logger.warning(
                    f"Schema '{schema_name}' has a field with no 'name'; it will be excluded from schema validation. Set the 'name' attribute on every property in the contract schema."
                )
                continue
            # SchemaProperty from ODCS model defines physicalType (may be None if omitted in contract).
            physical_type = prop.physicalType
            if not physical_type:
                raise InvalidPhysicalTypeError(
                    f"Schema '{schema_name}', property '{prop.name}': physicalType is required. "
                    "Set physicalType to a Unity Catalog data type (e.g. STRING, INT). "
                    "See: https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-datatypes"
                )
            try:
                unity_type = self._validate_unity_physical_type(physical_type)
            except InvalidPhysicalTypeError as e:
                raise InvalidPhysicalTypeError(f"Schema '{schema_name}', property '{prop.name}': {e!s}") from e
            col_name = prop.name
            # Databricks/ANSI: valid unquoted identifier = start with letter/underscore, then [a-zA-Z0-9_]*.
            # We do not check reserved keywords; Databricks handles that when parsing the DDL.
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                col_name = f"`{col_name}`"
            parts.append(f"{col_name} {unity_type}")
        return ", ".join(parts)

    def _generate_schema_validation_rules_for_schema(
        self,
        schema_obj: SchemaObject,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
        strict_schema_validation: bool,
    ) -> list[dict]:
        """Generate one dataset-level has_valid_schema rule per ODCS schema. strict_schema_validation is passed to the check."""
        ddl = self._schema_object_to_ddl(schema_obj, schema_name)
        if not ddl:
            logger.warning(f"Schema '{schema_name}' has no flat properties; skipping schema validation rule.")
            return []
        contract_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "schema_validation",
        }
        rule = {
            "check": {
                "function": "has_valid_schema",
                "arguments": {"expected_schema": ddl, "strict": strict_schema_validation},
            },
            "name": f"{schema_name}_schema_validation",
            "criticality": default_criticality,
            "user_metadata": contract_metadata,
        }
        return [rule]

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

        if not prop.name:
            logger.warning(
                f"Schema '{schema_name}' has a field with no 'name'; no quality checks will be generated for it. Set the 'name' attribute on every property in the contract schema."
            )
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

        schema_name_for_log = schema_obj.name or "unknown"

        def _extract_columns(props: list[SchemaProperty] | None, prefix: str = "") -> None:
            """Recursively extract column information from properties."""
            for prop in props or []:
                if not prop.name:
                    logger.warning(
                        f"Schema '{schema_name_for_log}' has a field with no 'name'; no rules will be generated from its text expectations. Set the 'name' attribute on every property in the contract schema."
                    )
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
            quality_rule.implementation,
            property_name,
            schema_name,
            odcs,
            default_criticality,
            quality_rule=quality_rule,
        )

    def _build_explicit_rule_from_implementation(
        self,
        impl: str | dict[str, Any] | None,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
        quality_rule: DataQuality | None = None,
    ) -> dict | None:
        """Build a DQX rule from an explicit implementation in the contract.

        Raises ODCSContractError if the implementation structure is invalid.
        """
        try:
            check, name, criticality, filter_rule = self._extract_impl_attributes(impl, default_criticality)
            if check is None:
                logger.warning("Implementation missing 'check' attribute, skipping rule")
                return None
            filter_rule = filter_rule or self._quality_row_filter(quality_rule)

            return self._build_rule_dict(
                check, name, criticality, filter_rule, schema_name, property_name, odcs, quality_rule
            )
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
        filter_rule: str | None = impl.get("filter")
        return check, name, criticality, filter_rule

    # Provenance keys this generator owns. Contract-authored metadata may add
    # tags but must never overwrite these, or downstream consumers that filter
    # on provenance (which contract a rule came from, which schema/field) would
    # be reading attacker- or typo-controlled values.
    _PROVENANCE_METADATA_KEYS: frozenset[str] = frozenset(
        {"contract_id", "contract_version", "odcs_version", "schema", "rule_type", "field"}
    )
    _ROW_FILTER_PROPERTY_KEYS: frozenset[str] = frozenset({"row_filter", "rowFilter"})
    # Accept both legacy ODCS ``steward`` and preferred ``owner`` customProperty keys.
    _OWNER_PROPERTY_KEYS: frozenset[str] = frozenset({"steward", "owner"})
    # ODCS team roles that imply ownership of generated rules (legacy steward roles kept).
    _OWNER_TEAM_ROLES: frozenset[str] = frozenset({"steward", "data steward", "owner"})

    @classmethod
    def _quality_row_filter(cls, quality_rule: DataQuality | None) -> str | None:
        """Read an operational row filter from ODCS custom properties.

        ``implementation.filter`` remains authoritative. This fallback lets
        contracts carry the filter through the standard ODCS extension point.
        """
        if quality_rule is None:
            return None
        for custom_property in quality_rule.customProperties or []:
            key = getattr(custom_property, "property", None)
            value = getattr(custom_property, "value", None)
            normalized_key = key.strip() if isinstance(key, str) else None
            if normalized_key in cls._ROW_FILTER_PROPERTY_KEYS and isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @classmethod
    def _iter_team_members(cls, odcs: OpenDataContractStandard):
        """Yield ODCS team members whether ``team`` is a list or a Team object."""
        team = getattr(odcs, "team", None)
        if team is None:
            return
        members = team if isinstance(team, list) else getattr(team, "members", None) or []
        yield from members

    @classmethod
    def _contract_owner(cls, odcs: OpenDataContractStandard) -> str | None:
        """First contract ``team`` member with an owner/steward-like role."""
        for member in cls._iter_team_members(odcs):
            role = getattr(member, "role", None)
            username = getattr(member, "username", None)
            if not isinstance(username, str) or not username.strip():
                continue
            if isinstance(role, str) and role.strip().lower() in cls._OWNER_TEAM_ROLES:
                return username.strip()
        return None

    @classmethod
    def _quality_owner(cls, quality_rule: DataQuality | None) -> str | None:
        """Per-quality owner override from customProperties (``owner`` or ``steward``)."""
        if quality_rule is None:
            return None
        for custom_property in quality_rule.customProperties or []:
            key = getattr(custom_property, "property", None)
            value = getattr(custom_property, "value", None)
            normalized_key = key.strip() if isinstance(key, str) else None
            if normalized_key in cls._OWNER_PROPERTY_KEYS and isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @classmethod
    def _resolve_owner(cls, odcs: OpenDataContractStandard, quality_rule: DataQuality | None = None) -> str | None:
        return cls._quality_owner(quality_rule) or cls._contract_owner(odcs)

    @classmethod
    def _apply_contract_owners(cls, rules: list[dict], odcs: OpenDataContractStandard) -> None:
        """Fill missing top-level ``owner`` from the contract team (predefined/schema rules)."""
        default_owner = cls._contract_owner(odcs)
        if not default_owner:
            return
        for rule in rules:
            if isinstance(rule, dict) and not rule.get("owner"):
                rule["owner"] = default_owner

    @classmethod
    def _quality_metadata(cls, quality_rule: DataQuality | None) -> dict[str, str]:
        """Collect the ODCS quality fields that belong in ``user_metadata``.

        ``customProperties`` is the ODCS extension point, so it is flattened to
        ``{property: value}`` entries. The first-class ``dimension`` and
        ``description`` fields are applied afterwards, so they win over a
        same-named custom property. Provenance keys are dropped.

        Values are stringified because ``user_metadata`` is a string map;
        nested structures are JSON-encoded rather than discarded.
        """
        if quality_rule is None:
            return {}

        metadata: dict[str, str] = {}
        for custom_property in quality_rule.customProperties or []:
            key = getattr(custom_property, "property", None)
            value = getattr(custom_property, "value", None)
            if not isinstance(key, str) or not key.strip() or value is None:
                continue
            metadata[key.strip()] = json.dumps(value) if isinstance(value, (dict, list)) else str(value)

        for key, value in (("dimension", quality_rule.dimension), ("description", quality_rule.description)):
            if isinstance(value, str) and value.strip():
                metadata[key] = value.strip()

        operational_keys = cls._PROVENANCE_METADATA_KEYS | cls._ROW_FILTER_PROPERTY_KEYS | cls._OWNER_PROPERTY_KEYS
        return {k: v for k, v in metadata.items() if k not in operational_keys}

    def _build_rule_dict(
        self,
        check_dict: dict,
        name: str,
        criticality: str,
        filter_rule: str | None,
        schema_name: str,
        property_name: str | None,
        odcs: OpenDataContractStandard,
        quality_rule: DataQuality | None = None,
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
        user_metadata.update(self._quality_metadata(quality_rule))

        rule = {
            "check": check_dict,
            "name": name,
            "criticality": criticality,
            "user_metadata": user_metadata,
        }
        if filter_rule:
            rule["filter"] = filter_rule
        owner = self._resolve_owner(odcs, quality_rule)
        if owner:
            rule["owner"] = owner
        return rule

    # ODCS type: library quality metric support.
    #
    # Sibling dispatch to the explicit-rule extractors above: explicit rules fail fast on
    # malformed structure and dispatch on a single generic `implementation` dict, while library
    # rules warn-and-skip per-entry and dispatch per-metric to distinct builders. Conflating the
    # two would mix incompatible error philosophies into one method.

    _SUPPORTED_LIBRARY_METRICS: tuple[str, ...] = (
        "rowCount",
        "nullValues",
        "missingValues",
        "invalidValues",
        "duplicateValues",
    )

    # Per-metric default `user_metadata["dimension"]` when the contract doesn't set its own,
    # drawn from ODCS's own dimension vocabulary.
    _LIBRARY_METRIC_DEFAULT_DIMENSIONS: dict[str, str] = {
        "rowCount": "completeness",
        "nullValues": "completeness",
        "missingValues": "completeness",
        "invalidValues": "conformity",
        "duplicateValues": "uniqueness",
    }

    _SAFE_SQL_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    # The eight ODCS threshold fields shared by every library metric (rowCount, duplicateValues, ...).
    _THRESHOLD_FIELDS: tuple[str, ...] = (
        "mustBe",
        "mustNotBe",
        "mustBeGreaterThan",
        "mustBeGreaterOrEqualTo",
        "mustBeLessThan",
        "mustBeLessOrEqualTo",
        "mustBeBetween",
        "mustNotBeBetween",
    )

    _MAX_LIBRARY_PATTERN_LENGTH = 200
    _NESTED_QUANTIFIER_PATTERN = re.compile(r"\([^()]*[+*]\)[+*{]")
    _ALTERNATION_QUANTIFIER_PATTERN = re.compile(r"\([^()]*\|[^()]*\)[+*{]")

    def _is_dqx_library_rule(self, quality_rule: DataQuality) -> bool:
        """Check if a quality rule is an ODCS type: library quality metric entry."""
        return quality_rule.type == 'library'

    def _process_library_rules_for_schema(
        self, schema_obj: SchemaObject, schema_name: str, odcs: OpenDataContractStandard, default_criticality: str
    ) -> list[dict]:
        """Process ODCS type: library quality metric entries from an ODCS schema."""
        rules: list[dict] = []

        # Process property-level library rules
        for prop in schema_obj.properties or []:
            if prop.quality:
                rules.extend(self._extract_property_library_rules(prop, schema_name, odcs, default_criticality))

        # Process schema-level library rules (e.g. rowCount)
        if schema_obj.quality:
            rules.extend(self._extract_schema_library_rules(schema_obj.quality, schema_name, odcs, default_criticality))

        return rules

    def _extract_property_library_rules(
        self, prop: SchemaProperty, schema_name: str, odcs: OpenDataContractStandard, default_criticality: str
    ) -> list[dict]:
        """Extract DQX rules from property-level type: library quality metric entries."""
        rules: list[dict] = []

        if prop.quality is None:
            return rules

        for quality_rule in prop.quality:
            if not self._is_dqx_library_rule(quality_rule):
                continue
            try:
                metric = self._resolve_library_metric(quality_rule, schema_name, prop.name)
                if metric is None:
                    continue
                rules.extend(
                    self._build_library_rules_for_metric(
                        quality_rule, metric, prop.name, schema_name, odcs, default_criticality
                    )
                )
            except (AttributeError, KeyError, TypeError) as e:
                logger.warning(
                    f"Skipping malformed type: library quality entry on property "
                    f"'{sanitize_for_logging(prop.name or 'unknown')}' in schema "
                    f"'{sanitize_for_logging(schema_name)}': {sanitize_for_logging(str(e))}"
                )
        return rules

    def _extract_schema_library_rules(
        self,
        quality_list: list[DataQuality],
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Extract DQX rules from schema-level type: library quality metric entries."""
        rules: list[dict] = []
        for quality_rule in quality_list:
            if not self._is_dqx_library_rule(quality_rule):
                continue
            try:
                metric = self._resolve_library_metric(quality_rule, schema_name)
                if metric is None:
                    continue
                rules.extend(
                    self._build_library_rules_for_metric(
                        quality_rule, metric, None, schema_name, odcs, default_criticality
                    )
                )
            except (AttributeError, KeyError, TypeError) as e:
                logger.warning(
                    f"Skipping malformed type: library quality entry in schema "
                    f"'{sanitize_for_logging(schema_name)}': {sanitize_for_logging(str(e))}"
                )
        return rules

    def _resolve_library_metric(
        self, quality_rule: DataQuality, schema_name: str, property_name: str | None = None
    ) -> str | None:
        """Validate quality_rule.metric against the five supported ODCS library metrics.

        Warns and returns None for a missing or unrecognized metric name so the caller can skip
        the entry without raising. The five supported metrics are named in the warning so a
        contract author can self-correct a typo without reading DQX source.
        """
        metric = quality_rule.metric
        supported = ", ".join(self._SUPPORTED_LIBRARY_METRICS)
        location = (
            f"property '{sanitize_for_logging(property_name)}' in schema '{sanitize_for_logging(schema_name)}'"
            if property_name
            else f"schema '{sanitize_for_logging(schema_name)}'"
        )

        if not metric:
            logger.warning(
                f"Missing 'metric' on type: library quality entry on {location}; skipping this quality check. "
                f"Supported metrics: {supported}."
            )
            return None

        if metric not in self._SUPPORTED_LIBRARY_METRICS:
            logger.warning(
                f"Unrecognized library metric '{sanitize_for_logging(metric)}' on {location}; skipping this "
                f"quality check. Supported metrics: {supported}."
            )
            return None

        return metric

    def _build_library_rules_for_metric(
        self,
        quality_rule: DataQuality,
        metric: str,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Dispatch a recognized type: library metric to its rule-building implementation.

        Per-metric builders (rowCount, nullValues, missingValues, invalidValues, duplicateValues)
        are added incrementally; a recognized metric with no builder yet produces no rules rather
        than raising or warning.
        """
        if metric == "rowCount":
            return self._build_row_count_rules(quality_rule, schema_name, odcs, default_criticality)
        if metric == "nullValues":
            return self._build_nullvalues_rules(quality_rule, property_name, schema_name, odcs, default_criticality)
        if metric == "missingValues":
            return self._build_missing_values_rules(quality_rule, property_name, schema_name, odcs, default_criticality)
        if metric == "invalidValues":
            return self._build_invalid_values_rules(quality_rule, property_name, schema_name, odcs, default_criticality)
        if metric == "duplicateValues":
            return self._build_duplicate_values_rules(
                quality_rule, property_name, schema_name, odcs, default_criticality
            )
        return []

    # rowCount ODCS threshold field -> (threshold_field, DQX check dict) builder, tried in order.
    # mustBe/mustNotBe/mustBeGreaterOrEqualTo/mustBeLessOrEqualTo map onto exact-fit dataset-level
    # aggregate checks; strict inequalities and both range forms (both bounds exclusive per ODCS)
    # have no aggregate equivalent and fall back to the dataset-level sql_query escape hatch. See
    # .scratch/odcs-library-metrics/issues/01-rowcount-mapping.md for the full mapping and rationale.

    def _build_row_count_rules(
        self,
        quality_rule: DataQuality,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Build the single DQX dataset-level row-count rule for an ODCS rowCount library entry."""
        resolved = self._row_count_check(quality_rule, schema_name)
        if resolved is None:
            return []
        threshold_field, check_dict = resolved

        user_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "metric",
            "metric": "rowCount",
            "threshold_field": threshold_field,
            "unit": quality_rule.unit or "rows",
            "dimension": self._library_dimension(quality_rule, "rowCount"),
            **self._library_severity_metadata(quality_rule),
        }

        return [
            {
                "check": check_dict,
                "name": f"{schema_name}_rowCount",
                "criticality": default_criticality,
                "user_metadata": user_metadata,
            }
        ]

    def _row_count_check(self, quality_rule: DataQuality, schema_name: str) -> tuple[str, dict] | None:
        """Resolve the first set ODCS threshold field on a rowCount entry to (field name, check dict).

        Returns None (after logging) when none of the eight ODCS threshold fields are set.
        """
        if quality_rule.mustBe is not None:
            return "mustBe", self._row_count_aggregate_check("is_aggr_equal", quality_rule.mustBe)
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._row_count_aggregate_check("is_aggr_not_equal", quality_rule.mustNotBe)
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._row_count_aggregate_check(
                "is_aggr_not_less_than", quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._row_count_aggregate_check(
                "is_aggr_not_greater_than", quality_rule.mustBeLessOrEqualTo
            )
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT COUNT(*) <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT COUNT(*) >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustNotBeBetween is not None:
            min_val, max_val = quality_rule.mustNotBeBetween
            return "mustNotBeBetween", self._library_sql_query_check(
                f"SELECT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}"
            )

        logger.warning(
            f"type: library rowCount entry on schema '{sanitize_for_logging(schema_name)}' has no recognized "
            "threshold field set (mustBe, mustNotBe, mustBeGreaterOrEqualTo, mustBeLessOrEqualTo, "
            "mustBeGreaterThan, mustBeLessThan, mustBeBetween, mustNotBeBetween); skipping this quality check."
        )
        return None

    @staticmethod
    def _row_count_aggregate_check(function: str, limit: Any) -> dict:
        """Build a dataset-level count-aggregate check dict (is_aggr_equal / is_aggr_not_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": "*", "limit": limit, "aggr_type": "count"},
        }

    @staticmethod
    def _library_sql_query_check(query: str, *, row_filter: str | None = None) -> dict:
        """Build a dataset-level sql_query check dict. condition_column semantics: true = violation.

        Shared strict/between fallback across library metrics (rowCount, nullValues,
        duplicateValues, ...). row_filter, when provided, narrows the view to the rows the query's
        aggregate should be scoped to (e.g. only the null rows for a nullValues check).
        """
        arguments: dict[str, Any] = {"query": query, "condition_column": "condition"}
        if row_filter is not None:
            arguments["row_filter"] = row_filter
        return {
            "function": "sql_query",
            "arguments": arguments,
        }

    # duplicateValues: single-property (argument-less) and composite (arguments.properties) forms
    # share one mapping, since is_unique's `columns` argument already accepts a list. mustBe: 0 maps
    # directly to is_unique; every other threshold routes through a COUNT(*) OVER (PARTITION BY ...)
    # duplicate-count indicator, because a window function can't live in row_filter/WHERE, so it is
    # passed via the `column` argument instead (F.expr evaluates it in aggregate context). See
    # .scratch/odcs-library-metrics/issues/05-duplicatevalues-mapping.md for the full mapping.

    def _build_duplicate_values_rules(
        self,
        quality_rule: DataQuality,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Build the DQX dataset-level uniqueness rule for an ODCS duplicateValues library entry.

        property_name is set for the property-level, single-column, argument-less form; None for
        the schema-level entry, whose composite key is read from arguments.properties.
        """
        key_columns = self._duplicate_values_key_columns(quality_rule, property_name, schema_name)
        if key_columns is None:
            return []

        resolved = self._duplicate_values_check(quality_rule, key_columns, schema_name)
        if resolved is None:
            return []
        threshold_field, check_dict = resolved

        user_metadata: dict[str, Any] = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "metric",
            "metric": "duplicateValues",
            "threshold_field": threshold_field,
            "unit": quality_rule.unit or "rows",
            "dimension": self._library_dimension(quality_rule, "duplicateValues"),
            **self._library_severity_metadata(quality_rule),
        }
        if property_name is not None:
            user_metadata["field"] = property_name
            rule_name = f"{property_name}_duplicateValues"
        else:
            user_metadata["fields"] = key_columns
            rule_name = f"{schema_name}_duplicateValues"

        return [
            {
                "check": check_dict,
                "name": rule_name,
                "criticality": default_criticality,
                "user_metadata": user_metadata,
            }
        ]

    def _duplicate_values_key_columns(
        self, quality_rule: DataQuality, property_name: str | None, schema_name: str
    ) -> list[str] | None:
        """Resolve the duplicateValues key columns.

        Returns [property_name] for the property-level form. For the schema-level form
        (property_name is None), reads the composite key from arguments.properties, warning and
        returning None if it is missing, not a list, empty, or contains a non-string entry.
        """
        if property_name is not None:
            return [property_name]

        properties = self._read_library_argument(quality_rule.arguments, "properties", list)
        if properties is None:
            return None
        if not all(isinstance(entry, str) for entry in properties):
            logger.warning(
                f"'arguments.properties' for type: library duplicateValues entry in schema "
                f"'{sanitize_for_logging(schema_name)}' must be a list of property name strings; "
                "skipping this quality check."
            )
            return None
        return properties

    def _duplicate_values_check(
        self, quality_rule: DataQuality, key_columns: list[str], schema_name: str
    ) -> tuple[str, dict] | None:
        """Resolve the first set ODCS threshold field on a duplicateValues entry to (field name, check dict).

        mustBe: 0 maps directly to is_unique (unit-independent: 0 rows = 0% either way). Every other
        threshold requires a resolved unit (rows/percent) to build the duplicate-count indicator;
        returns None (after logging) when no threshold field is set, or when unit is missing/unrecognized.
        """
        if quality_rule.mustBe == 0:
            return "mustBe", {"function": "is_unique", "arguments": {"columns": key_columns}}

        if not self._has_any_threshold_field(quality_rule):
            logger.warning(
                f"type: library duplicateValues entry in schema '{sanitize_for_logging(schema_name)}' has no "
                "recognized threshold field set (mustBe, mustNotBe, mustBeGreaterOrEqualTo, mustBeLessOrEqualTo, "
                "mustBeGreaterThan, mustBeLessThan, mustBeBetween, mustNotBeBetween); skipping this quality check."
            )
            return None

        unit = self._resolve_duplicate_values_unit(quality_rule, schema_name)
        if unit is None:
            return None

        indicator = self._duplicate_values_indicator_sql(key_columns, unit)
        aggr_type = "sum" if unit == "rows" else "avg"
        sql_aggr_fn = "SUM" if unit == "rows" else "AVG"

        if quality_rule.mustBe is not None:
            return "mustBe", self._duplicate_values_aggregate_check(
                "is_aggr_equal", indicator, aggr_type, quality_rule.mustBe
            )
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._duplicate_values_aggregate_check(
                "is_aggr_not_equal", indicator, aggr_type, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._duplicate_values_aggregate_check(
                "is_aggr_not_less_than", indicator, aggr_type, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._duplicate_values_aggregate_check(
                "is_aggr_not_greater_than", indicator, aggr_type, quality_rule.mustBeLessOrEqualTo
            )
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT {sql_aggr_fn}({indicator}) <= {quality_rule.mustBeGreaterThan} "
                "AS condition FROM {{ input_view }}"
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT {sql_aggr_fn}({indicator}) >= {quality_rule.mustBeLessThan} "
                "AS condition FROM {{ input_view }}"
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT ({sql_aggr_fn}({indicator}) > {min_val} AND {sql_aggr_fn}({indicator}) < {max_val}) "
                "AS condition FROM {{ input_view }}"
            )
        if quality_rule.mustNotBeBetween is not None:
            min_val, max_val = quality_rule.mustNotBeBetween
            return "mustNotBeBetween", self._library_sql_query_check(
                f"SELECT ({sql_aggr_fn}({indicator}) > {min_val} AND {sql_aggr_fn}({indicator}) < {max_val}) "
                "AS condition FROM {{ input_view }}"
            )

        # Unreachable: _has_any_threshold_field guarantees one of the eight fields above is set.
        raise AssertionError("Unreachable: no duplicateValues threshold field matched despite a prior check.")

    @classmethod
    def _has_any_threshold_field(cls, quality_rule: DataQuality) -> bool:
        """Return True if any of the eight ODCS threshold fields is set on quality_rule."""
        return any(getattr(quality_rule, field) is not None for field in cls._THRESHOLD_FIELDS)

    def _resolve_duplicate_values_unit(self, quality_rule: DataQuality, schema_name: str) -> str | None:
        """Resolve the unit for a non-zero duplicateValues threshold: 'rows' (default) or 'percent'.

        Warns and returns None for any other value (unrecognized unit, general library-metric policy).
        """
        unit = quality_rule.unit or "rows"
        if unit not in ("rows", "percent"):
            logger.warning(
                f"Unrecognized unit '{sanitize_for_logging(unit)}' on type: library duplicateValues entry in "
                f"schema '{sanitize_for_logging(schema_name)}'; expected 'rows' or 'percent'. Skipping this "
                "quality check."
            )
            return None
        return unit

    @classmethod
    def _duplicate_values_indicator_sql(cls, key_columns: list[str], unit: str) -> str:
        """Build the duplicate-count indicator SQL expression, replicating is_unique's nulls_distinct=True.

        A row in an all-non-null key group is only "in the group" with rows sharing its identical
        non-null key, so gating the whole expression on every key column being non-null is
        sufficient -- no nested NULL-handling inside the window COUNT(*) is needed. Every key
        column is quoted via _safe_sql_identifier before interpolation.
        """
        quoted = [cls._safe_sql_identifier(col) for col in key_columns]
        not_null_clause = " AND ".join(f"{col} IS NOT NULL" for col in quoted)
        partition_by = ", ".join(quoted)
        value = "1" if unit == "rows" else "100.0"
        return (
            f"CASE WHEN {not_null_clause} AND COUNT(*) OVER (PARTITION BY {partition_by}) > 1 "
            f"THEN {value} ELSE 0 END"
        )

    @staticmethod
    def _duplicate_values_aggregate_check(function: str, indicator_sql: str, aggr_type: str, limit: Any) -> dict:
        """Build a dataset-level duplicate-count aggregate check dict (is_aggr_equal / is_aggr_not_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": indicator_sql, "limit": limit, "aggr_type": aggr_type},
        }

    # nullValues ODCS threshold field -> (threshold_field, DQX check dict) builder, tried in order.
    # mustBe: 0 is a special case mapping onto the row-level is_not_null check (cheaper, pinpoints
    # the offending row, and is unit-independent: 0 nulls and 0% nulls are the same fact). Every
    # other threshold (including mustBe with N > 0) maps onto a dataset-level null-count/percentage
    # aggregate for the entry's unit: rows (the default, count(*) over rows where the column IS
    # NULL) or percent (AVG of an F.when(...)-indicator, since is_aggr_*'s row_filter+"*" mechanism
    # can only count rows, not express a percentage). Strict inequalities and both range forms (both
    # bounds exclusive per ODCS) have no aggregate equivalent and fall back to the dataset-level
    # sql_query escape hatch for both units. See
    # .scratch/odcs-library-metrics/issues/02-nullvalues-mapping.md for the full mapping and rationale.

    def _build_nullvalues_rules(
        self,
        quality_rule: DataQuality,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Build the single DQX rule for an ODCS nullValues library entry.

        nullValues is a property-level ODCS metric; a schema-level entry (no property) is skipped.
        """
        if not property_name:
            logger.warning(
                f"type: library nullValues entry in schema '{sanitize_for_logging(schema_name)}' has no "
                "property; nullValues is a property-level metric. Skipping this quality check."
            )
            return []

        resolved = self._nullvalues_check(quality_rule, property_name, schema_name)
        if resolved is None:
            return []
        threshold_field, check_dict = resolved

        user_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "metric",
            "metric": "nullValues",
            "threshold_field": threshold_field,
            "unit": quality_rule.unit or "rows",
            "dimension": self._library_dimension(quality_rule, "nullValues"),
            "field": property_name,
            **self._library_severity_metadata(quality_rule),
        }

        return [
            {
                "check": check_dict,
                "name": f"{property_name}_nullValues",
                "criticality": default_criticality,
                "user_metadata": user_metadata,
            }
        ]

    def _nullvalues_check(
        self, quality_rule: DataQuality, property_name: str, schema_name: str
    ) -> tuple[str, dict] | None:
        """Resolve the first set ODCS threshold field on a nullValues entry to (field name, check dict).

        mustBe: 0 is checked before consulting unit at all. Every other threshold routes through
        the null-count (unit: rows) or null-percentage (unit: percent) mechanism. Returns None
        (after logging) when no threshold field is set.
        """
        if quality_rule.mustBe == 0:
            return "mustBe", {"function": "is_not_null", "arguments": {"column": property_name}}

        if not self._has_any_threshold_field(quality_rule):
            logger.warning(
                f"type: library nullValues entry on property '{sanitize_for_logging(property_name)}' in schema "
                f"'{sanitize_for_logging(schema_name)}' has no recognized threshold field set (mustBe, mustNotBe, "
                "mustBeGreaterOrEqualTo, mustBeLessOrEqualTo, mustBeGreaterThan, mustBeLessThan, mustBeBetween, "
                "mustNotBeBetween); skipping this quality check."
            )
            return None

        quoted_column = self._safe_sql_identifier(property_name)
        if (quality_rule.unit or "rows") == "percent":
            return self._nullvalues_percent_check(quality_rule, quoted_column)
        return self._nullvalues_rows_check(quality_rule, quoted_column)

    def _nullvalues_rows_check(self, quality_rule: DataQuality, quoted_column: str) -> tuple[str, dict]:
        """Build the unit: rows (default) null-count check: count(*) over rows where column IS NULL."""
        row_filter = f"{quoted_column} IS NULL"

        if quality_rule.mustBe is not None:
            return "mustBe", self._nullvalues_aggregate_check("is_aggr_equal", row_filter, quality_rule.mustBe)
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._nullvalues_aggregate_check(
                "is_aggr_not_equal", row_filter, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._nullvalues_aggregate_check(
                "is_aggr_not_less_than", row_filter, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._nullvalues_aggregate_check(
                "is_aggr_not_greater_than", row_filter, quality_rule.mustBeLessOrEqualTo
            )
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT COUNT(*) <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}",
                row_filter=row_filter,
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT COUNT(*) >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}",
                row_filter=row_filter,
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}",
                row_filter=row_filter,
            )
        assert quality_rule.mustNotBeBetween is not None  # only remaining field per _has_any_threshold_field
        min_val, max_val = quality_rule.mustNotBeBetween
        return "mustNotBeBetween", self._library_sql_query_check(
            f"SELECT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}",
            row_filter=row_filter,
        )

    def _nullvalues_percent_check(self, quality_rule: DataQuality, quoted_column: str) -> tuple[str, dict]:
        """Build the unit: percent null-percentage check.

        Uses an F.when(...)-indicator Column (100.0 when null, else 0.0) with aggr_type="avg" for
        the exact-fit thresholds, since is_aggr_*'s row_filter+"*" mechanism can only count rows,
        not express a percentage (validate_star_aggregate rejects "*" with aggr_type="avg"). No
        row_filter is used for the percentage fallback either, so the denominator stays the full
        row count rather than shrinking to just the null rows.
        """
        indicator = F.when(F.expr(f"{quoted_column} IS NULL"), F.lit(100.0)).otherwise(F.lit(0.0))

        if quality_rule.mustBe is not None:
            return "mustBe", self._nullvalues_percent_aggregate_check("is_aggr_equal", indicator, quality_rule.mustBe)
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._nullvalues_percent_aggregate_check(
                "is_aggr_not_equal", indicator, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._nullvalues_percent_aggregate_check(
                "is_aggr_not_less_than", indicator, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._nullvalues_percent_aggregate_check(
                "is_aggr_not_greater_than", indicator, quality_rule.mustBeLessOrEqualTo
            )

        percent_expr = f"AVG(CASE WHEN {quoted_column} IS NULL THEN 100.0 ELSE 0.0 END)"
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT {percent_expr} <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT {percent_expr} >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT ({percent_expr} > {min_val} AND {percent_expr} < {max_val}) "
                f"AS condition FROM {{{{ input_view }}}}"
            )
        assert quality_rule.mustNotBeBetween is not None  # only remaining field per _has_any_threshold_field
        min_val, max_val = quality_rule.mustNotBeBetween
        return "mustNotBeBetween", self._library_sql_query_check(
            f"SELECT ({percent_expr} > {min_val} AND {percent_expr} < {max_val}) AS condition FROM {{{{ input_view }}}}"
        )

    @staticmethod
    def _nullvalues_aggregate_check(function: str, row_filter: str, limit: Any) -> dict:
        """Build a dataset-level null-count aggregate check dict (is_aggr_equal / is_aggr_not_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": "*", "limit": limit, "aggr_type": "count", "row_filter": row_filter},
        }

    @staticmethod
    def _nullvalues_percent_aggregate_check(function: str, indicator: Column, limit: Any) -> dict:
        """Build a dataset-level null-percentage aggregate check dict (is_aggr_equal / is_aggr_not_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": indicator, "limit": limit, "aggr_type": "avg"},
        }

    # invalidValues ODCS threshold field -> (threshold_field, DQX check dict) builder, tried in
    # order. mustBe: 0 maps onto row-level is_in_list / regex_match checks -- one per present
    # mechanism (validValues allowlist and/or pattern), OR'd via DQX's own per-row
    # _errors/_warnings union when both are present. Every other threshold routes through a
    # dataset-level aggregate over the OR'd "invalid" SQL condition (NOT IN the allowlist and/or
    # NOT RLIKE the pattern), for the entry's unit: rows (row_filter+"*"+count, matching
    # nullValues) or percent (a CASE WHEN indicator SQL string with aggr_type="avg", since
    # row_filter+"*" can't express a percentage -- a raw Column indicator is deliberately avoided,
    # since DQEngine.validate_checks' semantic conflict detection isn't Column-aware and raises on
    # one, the same issue missingValues works around below). Strict inequalities and both range
    # forms (both bounds exclusive per ODCS) fall back to the dataset-level sql_query escape hatch
    # for both units. See .scratch/odcs-library-metrics/issues/04-invalidvalues-mapping.md for the
    # full mapping and rationale.

    def _build_invalid_values_rules(
        self,
        quality_rule: DataQuality,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Build the DQX rule(s) for an ODCS invalidValues library entry.

        invalidValues is a property-level ODCS metric; a schema-level entry (no property) is skipped.
        """
        if not property_name:
            logger.warning(
                f"type: library invalidValues entry in schema '{sanitize_for_logging(schema_name)}' has no "
                "property; invalidValues is a property-level metric. Skipping this quality check."
            )
            return []

        valid_values, pattern = self._invalid_values_arguments(quality_rule, property_name, schema_name)
        if valid_values is None and pattern is None:
            return []

        contract_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "metric",
            "metric": "invalidValues",
            "unit": quality_rule.unit or "rows",
            "dimension": self._library_dimension(quality_rule, "invalidValues"),
            "field": property_name,
            **self._library_severity_metadata(quality_rule),
        }

        if quality_rule.mustBe == 0:
            return self._invalid_values_row_level_rules(
                valid_values, pattern, property_name, contract_metadata, default_criticality
            )

        resolved = self._invalid_values_check(quality_rule, valid_values, pattern, property_name, schema_name)
        if resolved is None:
            return []
        threshold_field, check_dict = resolved

        return [
            {
                "check": check_dict,
                "name": f"{property_name}_invalidValues",
                "criticality": default_criticality,
                "user_metadata": {**contract_metadata, "threshold_field": threshold_field},
            }
        ]

    def _invalid_values_arguments(
        self, quality_rule: DataQuality, property_name: str, schema_name: str
    ) -> tuple[list | None, str | None]:
        """Read arguments.validValues and/or arguments.pattern for an invalidValues library entry.

        Either, neither, or both may be present. A syntactically valid pattern that fails the
        generation-time ReDoS safety guard is treated as absent (its own warning is logged)
        rather than as a malformed-arguments error. Warns once more, generically, only when
        neither mechanism yields a usable value overall.
        """
        arguments = quality_rule.arguments

        valid_values: list | None = None
        if isinstance(arguments, dict) and "validValues" in arguments:
            valid_values = self._read_library_argument(arguments, "validValues", list)

        pattern: str | None = None
        if isinstance(arguments, dict) and "pattern" in arguments:
            raw_pattern = self._read_library_argument(arguments, "pattern", str)
            if raw_pattern is not None:
                if self._is_library_pattern_safe(raw_pattern):
                    pattern = raw_pattern
                else:
                    logger.warning(
                        f"'arguments.pattern' on property '{sanitize_for_logging(property_name)}' in schema "
                        f"'{sanitize_for_logging(schema_name)}' failed the ReDoS safety guard (200-character "
                        "cap, nested-quantifier check, alternation-quantifier check, or re.compile); skipping "
                        "the pattern-based check for this entry."
                    )

        if valid_values is None and pattern is None:
            logger.warning(
                f"type: library invalidValues entry on property '{sanitize_for_logging(property_name)}' in "
                f"schema '{sanitize_for_logging(schema_name)}' has neither a usable 'arguments.validValues' "
                "list nor a usable 'arguments.pattern' string; skipping this quality check."
            )

        return valid_values, pattern

    def _invalid_values_row_level_rules(
        self,
        valid_values: list | None,
        pattern: str | None,
        property_name: str,
        contract_metadata: dict,
        default_criticality: str,
    ) -> list[dict]:
        """Build the row-level rule(s) for mustBe: 0 -- one per present mechanism (validValues
        allowlist and/or pattern). Emitting both as separate rules (sharing identical
        user_metadata, only *name* differs) reproduces "invalid if either criterion fails" via
        DQX's own per-row _errors/_warnings union, the same OR-of-rules treatment missingValues
        uses for its null/sentinel split.
        """
        user_metadata = {**contract_metadata, "threshold_field": "mustBe"}
        rules = []
        if valid_values is not None:
            rules.append(
                {
                    "check": {
                        "function": "is_in_list",
                        "arguments": {
                            "column": property_name,
                            "allowed": [self._is_in_list_literal(value) for value in valid_values],
                            "case_sensitive": True,
                        },
                    },
                    "name": f"{property_name}_invalidValues_allowed",
                    "criticality": default_criticality,
                    "user_metadata": dict(user_metadata),
                }
            )
        if pattern is not None:
            rules.append(
                {
                    "check": {"function": "regex_match", "arguments": {"column": property_name, "regex": pattern}},
                    "name": f"{property_name}_invalidValues_pattern",
                    "criticality": default_criticality,
                    "user_metadata": dict(user_metadata),
                }
            )
        return rules

    @staticmethod
    def _is_in_list_literal(value: Any) -> Any:  # value/return: any contract-supplied scalar (str, number, bool)
        """Render a validValues entry as an is_in_list *allowed* list literal.

        is_in_list resolves each *allowed* entry like a comparison-check limit: a bare string is
        parsed as a **column expression**, not a string literal (see check_funcs.is_in_list's own
        docstring) -- so a contract-supplied string value must be single-quoted to compare
        correctly. Non-string values are passed through unchanged; get_limit_expr already resolves
        them via F.lit().
        """
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        return value

    def _invalid_values_check(
        self,
        quality_rule: DataQuality,
        valid_values: list | None,
        pattern: str | None,
        property_name: str,
        schema_name: str,
    ) -> tuple[str, dict] | None:
        """Resolve a non-zero-threshold invalidValues entry to (threshold_field, check dict).

        Routes through the invalid-row-count (unit: rows) or invalid-row-percentage (unit:
        percent) mechanism. Returns None (after logging) when no threshold field is set, or when
        unit is missing/unrecognized.
        """
        if not self._has_any_threshold_field(quality_rule):
            logger.warning(
                f"type: library invalidValues entry on property '{sanitize_for_logging(property_name)}' in "
                f"schema '{sanitize_for_logging(schema_name)}' has no recognized threshold field set (mustBe, "
                "mustNotBe, mustBeGreaterOrEqualTo, mustBeLessOrEqualTo, mustBeGreaterThan, mustBeLessThan, "
                "mustBeBetween, mustNotBeBetween); skipping this quality check."
            )
            return None

        unit = quality_rule.unit or "rows"
        if unit not in ("rows", "percent"):
            logger.warning(
                f"Unrecognized unit '{sanitize_for_logging(unit)}' on type: library invalidValues entry on "
                f"property '{sanitize_for_logging(property_name)}' in schema "
                f"'{sanitize_for_logging(schema_name)}'; expected 'rows' or 'percent'. Skipping this quality "
                "check."
            )
            return None

        invalid_condition = self._invalid_values_condition_sql(property_name, valid_values, pattern)
        if unit == "percent":
            return self._invalid_values_percent_check(quality_rule, invalid_condition)
        return self._invalid_values_rows_check(quality_rule, invalid_condition)

    def _invalid_values_rows_check(self, quality_rule: DataQuality, invalid_condition: str) -> tuple[str, dict] | None:
        """Build the unit: rows (default) invalid-row-count check: count(*) over invalid rows.

        Always returns a check dict in practice -- callers only reach this after
        _has_any_threshold_field confirms at least one of the eight fields is set -- but the
        return type stays Optional so mypy can verify the final mustNotBeBetween branch without an
        unreachable-code assertion.
        """
        if quality_rule.mustBe is not None:
            return "mustBe", self._invalid_values_aggregate_check(
                "is_aggr_equal", invalid_condition, quality_rule.mustBe
            )
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._invalid_values_aggregate_check(
                "is_aggr_not_equal", invalid_condition, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._invalid_values_aggregate_check(
                "is_aggr_not_less_than", invalid_condition, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._invalid_values_aggregate_check(
                "is_aggr_not_greater_than", invalid_condition, quality_rule.mustBeLessOrEqualTo
            )
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT COUNT(*) <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}",
                row_filter=invalid_condition,
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT COUNT(*) >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}",
                row_filter=invalid_condition,
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}",
                row_filter=invalid_condition,
            )
        if quality_rule.mustNotBeBetween is not None:
            min_val, max_val = quality_rule.mustNotBeBetween
            return "mustNotBeBetween", self._library_sql_query_check(
                f"SELECT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}",
                row_filter=invalid_condition,
            )
        return None

    def _invalid_values_percent_check(
        self, quality_rule: DataQuality, invalid_condition: str
    ) -> tuple[str, dict] | None:
        """Build the unit: percent invalid-row-percentage check.

        Uses a CASE WHEN indicator SQL expression (100.0 when invalid, else 0.0), passed as the
        *column* argument (resolved via F.expr at check-execution time), with aggr_type="avg" for
        the exact-fit thresholds -- is_aggr_*'s row_filter+"*" mechanism can only count rows, not
        express a percentage. A raw Column object is deliberately avoided here: DQEngine.validate_checks'
        semantic conflict detection isn't Column-aware and raises when a check argument is one. No
        row_filter is used for the percentage fallback either, so the denominator stays the full
        row count rather than shrinking to just the invalid rows. Always returns a check dict in
        practice, per _invalid_values_rows_check's own docstring note about the Optional return type.
        """
        indicator_sql = f"CASE WHEN {invalid_condition} THEN 100.0 ELSE 0.0 END"

        if quality_rule.mustBe is not None:
            return "mustBe", self._invalid_values_percent_aggregate_check(
                "is_aggr_equal", indicator_sql, quality_rule.mustBe
            )
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._invalid_values_percent_aggregate_check(
                "is_aggr_not_equal", indicator_sql, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._invalid_values_percent_aggregate_check(
                "is_aggr_not_less_than", indicator_sql, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._invalid_values_percent_aggregate_check(
                "is_aggr_not_greater_than", indicator_sql, quality_rule.mustBeLessOrEqualTo
            )

        percent_expr = f"AVG({indicator_sql})"
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT {percent_expr} <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT {percent_expr} >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT ({percent_expr} > {min_val} AND {percent_expr} < {max_val}) "
                f"AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustNotBeBetween is not None:
            min_val, max_val = quality_rule.mustNotBeBetween
            return "mustNotBeBetween", self._library_sql_query_check(
                f"SELECT ({percent_expr} > {min_val} AND {percent_expr} < {max_val}) "
                f"AS condition FROM {{{{ input_view }}}}"
            )
        return None

    @staticmethod
    def _invalid_values_aggregate_check(
        function: str,
        row_filter: str,
        limit: Any,  # limit: the contract's own mustBe/mustNotBe/etc. value (int | float)
    ) -> dict:
        """Build a dataset-level invalid-row-count aggregate check dict (is_aggr_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": "*", "limit": limit, "aggr_type": "count", "row_filter": row_filter},
        }

    @staticmethod
    def _invalid_values_percent_aggregate_check(
        function: str, indicator_sql: str, limit: Any  # limit: the contract's own threshold value (int | float)
    ) -> dict:
        """Build a dataset-level invalid-row-percentage aggregate check dict (is_aggr_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": indicator_sql, "limit": limit, "aggr_type": "avg"},
        }

    def _invalid_values_condition_sql(self, property_name: str, valid_values: list | None, pattern: str | None) -> str:
        """Build the SQL boolean condition matching an "invalid" row for property_name.

        NOT IN the allowlist and/or NOT RLIKE the pattern, OR'd together when both mechanisms are
        present; each is independently null-tolerant (NULL NOT IN (...) and NOT (NULL RLIKE ...)
        are both NULL, not TRUE), matching the row-level is_in_list/regex_match checks' own
        null-tolerant semantics.
        """
        quoted_column = self._safe_sql_identifier(property_name)
        clauses = []
        if valid_values is not None:
            escaped_values = ", ".join(self._invalid_values_sql_literal(value) for value in valid_values)
            clauses.append(f"{quoted_column} NOT IN ({escaped_values})")
        if pattern is not None:
            clauses.append(f"NOT ({quoted_column} RLIKE {self._invalid_values_sql_literal(pattern)})")
        return " OR ".join(clauses)

    @staticmethod
    def _invalid_values_sql_literal(value: Any) -> str:  # value: any contract-supplied scalar (str, number, bool)
        """Render a value as a single-quoted SQL string literal, doubling embedded quotes."""
        return "'" + str(value).replace("'", "''") + "'"

    # missingValues ODCS threshold field -> (threshold_field, DQX check dict) builder, tried in
    # order. missingValues is distinct from nullValues: it combines real NULLs with a
    # contract-supplied sentinel list (arguments.missingValues, e.g. [null, "", "N/A"]) read via
    # _read_library_argument. mustBe: 0 splits into up to two independent row-level rules (one
    # is_not_null, one is_not_in_list) -- present only for the sentinel kinds actually listed --
    # OR'd via DQX's own per-row _errors/_warnings union, the same treatment invalidValues uses
    # for its allowed-list/pattern split. Every other threshold routes through a dataset-level
    # aggregate over the "col IS NULL [OR col IN (...)]" SQL condition, for the entry's unit: rows
    # (row_filter+"*"+count, matching nullValues/invalidValues) or percent. The percent path uses
    # an `AVG(CASE WHEN ... THEN 100.0 ELSE 0.0 END)` SQL *string* expression passed as `column`,
    # deliberately not an F.when(...) Column: ChecksSemanticValidator._conflict_key evaluates
    # `arguments.get("column") or ...`, and Column.__bool__ raises PySparkValueError, which crashes
    # rule generation outright. A string routes through F.expr() at apply time instead, producing
    # the identical Spark expression without tripping that truthiness check. Strict inequalities
    # and both range forms (both bounds exclusive per ODCS) fall back to the dataset-level
    # sql_query escape hatch for both units. See
    # .scratch/odcs-library-metrics/issues/03-missingvalues-mapping.md for the full mapping.

    def _build_missing_values_rules(
        self,
        quality_rule: DataQuality,
        property_name: str | None,
        schema_name: str,
        odcs: OpenDataContractStandard,
        default_criticality: str,
    ) -> list[dict]:
        """Build the DQX rule(s) for an ODCS missingValues library entry.

        missingValues is a property-level ODCS metric; a schema-level entry (no property) is skipped.
        """
        if not property_name:
            logger.warning(
                f"type: library missingValues entry in schema '{sanitize_for_logging(schema_name)}' has no "
                "property; missingValues is a property-level metric. Skipping this quality check."
            )
            return []

        sentinel_list = self._read_library_argument(quality_rule.arguments, "missingValues", list)
        if sentinel_list is None:
            return []

        has_null = None in sentinel_list
        non_null_sentinels = [value for value in sentinel_list if value is not None]

        contract_metadata = {
            "contract_id": odcs.id or "unknown",
            "contract_version": odcs.version or "unknown",
            "odcs_version": odcs.apiVersion or "unknown",
            "schema": schema_name,
            "rule_type": "metric",
            "metric": "missingValues",
            "unit": quality_rule.unit or "rows",
            "dimension": self._library_dimension(quality_rule, "missingValues"),
            "field": property_name,
            **self._library_severity_metadata(quality_rule),
        }

        if quality_rule.mustBe == 0:
            return self._missing_values_row_level_rules(
                has_null, non_null_sentinels, property_name, contract_metadata, default_criticality
            )

        resolved = self._missing_values_check(quality_rule, property_name, schema_name, non_null_sentinels)
        if resolved is None:
            return []
        threshold_field, check_dict = resolved

        return [
            {
                "check": check_dict,
                "name": f"{property_name}_missingValues",
                "criticality": default_criticality,
                "user_metadata": {**contract_metadata, "threshold_field": threshold_field},
            }
        ]

    def _missing_values_row_level_rules(
        self,
        has_null: bool,
        non_null_sentinels: list,
        property_name: str,
        contract_metadata: dict,
        default_criticality: str,
    ) -> list[dict]:
        """Build the row-level rule(s) for mustBe: 0 -- one per present sentinel kind (null and/or
        non-null sentinels). Emitting both as separate rules (sharing identical user_metadata, only
        `name` differs) reproduces "missing if either criterion fails" via DQX's own per-row
        _errors/_warnings union: is_not_in_list's `forbidden` list can never itself catch a real
        SQL NULL (`x IN (...)` is NULL, not TRUE, whenever x IS NULL), so the two conditions cannot
        be folded into a single check.
        """
        user_metadata = {**contract_metadata, "threshold_field": "mustBe"}
        rules = []
        if has_null:
            rules.append(
                {
                    "check": {"function": "is_not_null", "arguments": {"column": property_name}},
                    "name": f"{property_name}_missingValues_null",
                    "criticality": default_criticality,
                    "user_metadata": dict(user_metadata),
                }
            )
        if non_null_sentinels:
            rules.append(
                {
                    "check": {
                        "function": "is_not_in_list",
                        "arguments": {
                            "column": property_name,
                            "forbidden": [F.lit(value) for value in non_null_sentinels],
                            "case_sensitive": True,
                        },
                    },
                    "name": f"{property_name}_missingValues_sentinel",
                    "criticality": default_criticality,
                    "user_metadata": dict(user_metadata),
                }
            )
        return rules

    def _missing_values_check(
        self, quality_rule: DataQuality, property_name: str, schema_name: str, non_null_sentinels: list
    ) -> tuple[str, dict] | None:
        """Resolve a non-zero-threshold missingValues entry to (threshold_field, check dict).

        Routes through the missing-row-count (unit: rows) or missing-row-percentage (unit:
        percent) mechanism. Returns None (after logging) when no threshold field is set, or when
        unit is missing/unrecognized.
        """
        if not self._has_any_threshold_field(quality_rule):
            logger.warning(
                f"type: library missingValues entry on property '{sanitize_for_logging(property_name)}' in "
                f"schema '{sanitize_for_logging(schema_name)}' has no recognized threshold field set (mustBe, "
                "mustNotBe, mustBeGreaterOrEqualTo, mustBeLessOrEqualTo, mustBeGreaterThan, mustBeLessThan, "
                "mustBeBetween, mustNotBeBetween); skipping this quality check."
            )
            return None

        unit = quality_rule.unit or "rows"
        if unit not in ("rows", "percent"):
            logger.warning(
                f"Unrecognized unit '{sanitize_for_logging(unit)}' on type: library missingValues entry on "
                f"property '{sanitize_for_logging(property_name)}' in schema "
                f"'{sanitize_for_logging(schema_name)}'; expected 'rows' or 'percent'. Skipping this quality "
                "check."
            )
            return None

        missing_condition = self._missing_values_condition_sql(property_name, non_null_sentinels)
        if unit == "percent":
            return self._missing_values_percent_check(quality_rule, missing_condition)
        return self._missing_values_rows_check(quality_rule, missing_condition)

    def _missing_values_rows_check(self, quality_rule: DataQuality, missing_condition: str) -> tuple[str, dict]:
        """Build the unit: rows (default) missing-row-count check: count(*) over missing rows."""
        if quality_rule.mustBe is not None:
            return "mustBe", self._missing_values_aggregate_check(
                "is_aggr_equal", missing_condition, quality_rule.mustBe
            )
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._missing_values_aggregate_check(
                "is_aggr_not_equal", missing_condition, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._missing_values_aggregate_check(
                "is_aggr_not_less_than", missing_condition, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._missing_values_aggregate_check(
                "is_aggr_not_greater_than", missing_condition, quality_rule.mustBeLessOrEqualTo
            )
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT COUNT(*) <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}",
                row_filter=missing_condition,
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT COUNT(*) >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}",
                row_filter=missing_condition,
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}",
                row_filter=missing_condition,
            )
        assert quality_rule.mustNotBeBetween is not None  # only remaining field per _has_any_threshold_field
        min_val, max_val = quality_rule.mustNotBeBetween
        return "mustNotBeBetween", self._library_sql_query_check(
            f"SELECT (COUNT(*) > {min_val} AND COUNT(*) < {max_val}) AS condition FROM {{{{ input_view }}}}",
            row_filter=missing_condition,
        )

    def _missing_values_percent_check(self, quality_rule: DataQuality, missing_condition: str) -> tuple[str, dict]:
        """Build the unit: percent missing-row-percentage check.

        Uses an `AVG(CASE WHEN ... THEN 100.0 ELSE 0.0 END)` SQL string expression passed as the
        `column` argument (not an F.when(...) Column -- see the class comment above this metric's
        section for why) for the exact-fit thresholds, since is_aggr_*'s row_filter+"*" mechanism
        can only count rows, not express a percentage. No row_filter is used for the percentage
        fallback either, so the denominator stays the full row count rather than shrinking to just
        the missing rows.
        """
        indicator_expr = f"CASE WHEN {missing_condition} THEN 100.0 ELSE 0.0 END"

        if quality_rule.mustBe is not None:
            return "mustBe", self._missing_values_percent_aggregate_check(
                "is_aggr_equal", indicator_expr, quality_rule.mustBe
            )
        if quality_rule.mustNotBe is not None:
            return "mustNotBe", self._missing_values_percent_aggregate_check(
                "is_aggr_not_equal", indicator_expr, quality_rule.mustNotBe
            )
        if quality_rule.mustBeGreaterOrEqualTo is not None:
            return "mustBeGreaterOrEqualTo", self._missing_values_percent_aggregate_check(
                "is_aggr_not_less_than", indicator_expr, quality_rule.mustBeGreaterOrEqualTo
            )
        if quality_rule.mustBeLessOrEqualTo is not None:
            return "mustBeLessOrEqualTo", self._missing_values_percent_aggregate_check(
                "is_aggr_not_greater_than", indicator_expr, quality_rule.mustBeLessOrEqualTo
            )

        percent_expr = f"AVG({indicator_expr})"
        if quality_rule.mustBeGreaterThan is not None:
            return "mustBeGreaterThan", self._library_sql_query_check(
                f"SELECT {percent_expr} <= {quality_rule.mustBeGreaterThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeLessThan is not None:
            return "mustBeLessThan", self._library_sql_query_check(
                f"SELECT {percent_expr} >= {quality_rule.mustBeLessThan} AS condition FROM {{{{ input_view }}}}"
            )
        if quality_rule.mustBeBetween is not None:
            min_val, max_val = quality_rule.mustBeBetween
            return "mustBeBetween", self._library_sql_query_check(
                f"SELECT NOT ({percent_expr} > {min_val} AND {percent_expr} < {max_val}) "
                f"AS condition FROM {{{{ input_view }}}}"
            )
        assert quality_rule.mustNotBeBetween is not None  # only remaining field per _has_any_threshold_field
        min_val, max_val = quality_rule.mustNotBeBetween
        return "mustNotBeBetween", self._library_sql_query_check(
            f"SELECT ({percent_expr} > {min_val} AND {percent_expr} < {max_val}) AS condition FROM {{{{ input_view }}}}"
        )

    @staticmethod
    def _missing_values_aggregate_check(
        function: str, row_filter: str, limit: Any  # limit: the contract's own threshold value (int | float)
    ) -> dict:
        """Build a dataset-level missing-row-count aggregate check dict (is_aggr_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": "*", "limit": limit, "aggr_type": "count", "row_filter": row_filter},
        }

    @staticmethod
    def _missing_values_percent_aggregate_check(
        function: str, indicator_expr: str, limit: Any  # limit: the contract's own threshold value (int | float)
    ) -> dict:
        """Build a dataset-level missing-row-percentage aggregate check dict (is_aggr_equal / etc.)."""
        return {
            "function": function,
            "arguments": {"column": indicator_expr, "limit": limit, "aggr_type": "avg"},
        }

    def _missing_values_condition_sql(self, property_name: str, non_null_sentinels: list) -> str:
        """Build the SQL boolean condition matching a "missing" row for property_name: real NULL,
        plus an IN (...) match against any contract-supplied non-null sentinel values.
        """
        quoted_column = self._safe_sql_identifier(property_name)
        condition = f"{quoted_column} IS NULL"
        if non_null_sentinels:
            escaped_values = ", ".join(self._missing_values_sql_literal(value) for value in non_null_sentinels)
            condition += f" OR {quoted_column} IN ({escaped_values})"
        return condition

    @staticmethod
    def _missing_values_sql_literal(value: Any) -> str:  # value: any contract-supplied scalar (str, number, bool)
        """Render a non-null sentinel value as a single-quoted SQL string literal, doubling embedded quotes."""
        return "'" + str(value).replace("'", "''") + "'"

    def _read_library_argument(
        self, arguments: dict[str, Any] | None, key: str, expected_type: type[_T], *, allow_empty: bool = False
    ) -> _T | None:
        """Validate and read arguments[key] from a type: library quality entry's arguments dict.

        Returns None (after logging a targeted warning) if *arguments* isn't a dict, *key* is
        absent, the value isn't an instance of *expected_type*, or (unless allow_empty) the value
        is empty. Callers treat None as "skip this quality entry."
        """
        if not isinstance(arguments, dict):
            logger.warning(f"Missing or malformed 'arguments' for library metric argument '{key}'; expected a dict.")
            return None

        value = arguments.get(key)
        if not isinstance(value, expected_type):
            logger.warning(
                f"Missing or malformed 'arguments.{key}': expected {expected_type.__name__}, "
                f"got {type(value).__name__}."
            )
            return None

        if not allow_empty and hasattr(value, "__len__") and len(value) == 0:
            logger.warning(f"'arguments.{key}' must not be empty.")
            return None

        return value

    @classmethod
    def _safe_sql_identifier(cls, column_path: str) -> str:
        """Quote column_path for safe interpolation into a row_filter/sql_query/PARTITION BY fragment.

        Splits on '.', backtick-quotes any segment that isn't a bare identifier
        (`^[a-zA-Z_][a-zA-Z0-9_]*$`), escaping any embedded backtick first, and rejoins. Every
        input is handled by quoting rather than rejecting, so this never raises.
        """
        segments = column_path.split(".")
        quoted_segments = [
            segment if cls._SAFE_SQL_IDENTIFIER_PATTERN.match(segment) else f"`{segment.replace('`', '``')}`"
            for segment in segments
        ]
        return ".".join(quoted_segments)

    @classmethod
    def _is_library_pattern_safe(cls, pattern: str) -> bool:
        """Best-effort, generation-time-only ReDoS guard for a contract-supplied arguments.pattern.

        Rejects patterns over 200 characters, nested-quantifier (`(a+)+`-shaped) or
        alternation-plus-quantifier (`(a|a)*`-shaped) structures, and anything that fails to
        compile. This is a heuristic, not a formal linear-time guarantee.
        """
        if len(pattern) > cls._MAX_LIBRARY_PATTERN_LENGTH:
            return False
        if cls._NESTED_QUANTIFIER_PATTERN.search(pattern) or cls._ALTERNATION_QUANTIFIER_PATTERN.search(pattern):
            return False
        try:
            re.compile(pattern)
        except re.error:
            return False
        return True

    def _library_dimension(self, quality_rule: DataQuality, metric: str) -> str:
        """Return the contract's own dimension when present, else the per-metric ODCS default."""
        return quality_rule.dimension or self._LIBRARY_METRIC_DEFAULT_DIMENSIONS[metric]

    @staticmethod
    def _library_severity_metadata(quality_rule: DataQuality) -> dict[str, str]:
        """Return {"severity": ...} verbatim when the contract sets DataQuality.severity, else {}.

        ODCS defines no vocabulary for severity, so it is recorded as-is for audit purposes and
        never used to derive Criticality.
        """
        if quality_rule.severity:
            return {"severity": quality_rule.severity}
        return {}
