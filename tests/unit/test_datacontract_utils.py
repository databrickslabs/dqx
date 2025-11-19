"""
Shared test helpers for data contract tests.
"""

import tempfile

import yaml

from databricks.labs.dqx.engine import DQEngine


def assert_valid_rule_structure(rule: dict) -> None:
    """
    Assert that a rule dictionary has the expected structure using DQEngine's validation.

    A valid rule must contain:
        - "check": a dictionary with "function" and "arguments" keys
        - "criticality": the rule's criticality level
        - "user_metadata": metadata associated with the rule (optional)
    """
    # Use DQEngine's validate_checks method
    validation_status = DQEngine.validate_checks([rule], validate_custom_check_functions=False)
    assert not validation_status.has_errors, f"Rule validation failed: {validation_status.errors}"

    # Additionally check user_metadata if present (this is contract-specific, not in DQEngine validation)
    if "user_metadata" in rule:
        assert isinstance(rule["user_metadata"], dict), "user_metadata must be a dictionary"


def assert_valid_contract_metadata(metadata: dict) -> None:
    """
    Assert that the metadata dictionary contains the expected contract information.

    The metadata dictionary must have the following keys:
        - contract_id: Unique identifier for the contract (str)
        - contract_version: Version of the contract (str)
        - odcs_version: ODCS API version (str)
        - schema: Name of the schema the contract applies to (str)
        - field: Name of the field the contract applies to (str) - optional
        - rule_type: Type of rule, must be one of {"predefined", "explicit", "text_llm"} (str)

    Args:
        metadata: The contract metadata to validate.

    Raises:
        AssertionError: If any required key is missing or if rule_type is invalid.
    """
    assert "contract_id" in metadata
    assert "contract_version" in metadata
    assert "odcs_version" in metadata
    assert "schema" in metadata
    # field is optional (not present for schema-level rules)
    assert "rule_type" in metadata
    assert metadata["rule_type"] in {"predefined", "explicit", "text_llm"}


def assert_rules_have_valid_structure(rules: list[dict]) -> None:
    """
    Validate that all rules in the list have the expected structure.

    Args:
        rules: List of rule dictionaries to validate.
    """
    assert len(rules) > 0
    for rule in rules:
        assert_valid_rule_structure(rule)


def assert_rules_have_valid_metadata(rules: list[dict]) -> None:
    """
    Validate that all rules contain valid contract metadata.

    Args:
        rules: List of rule dictionaries to validate.
    """
    for rule in rules:
        assert_valid_contract_metadata(rule["user_metadata"])


def create_basic_contract(
    schema_name: str = "test_table",
    properties: list[dict] | None = None,
    contract_id: str = "test",
    contract_version: str = "1.0.0",
) -> dict:
    """
    Create a basic ODCS v3.x contract dictionary with specified properties.

    Args:
        schema_name: Name of the schema.
        properties: List of ODCS v3.x property dictionaries. Each property should have:
                   - name: property name
                   - logicalType: data type (string, number, integer, etc.)
                   - required: boolean (optional)
                   - unique: boolean (optional)
                   - logicalTypeOptions: dict with pattern, minimum, maximum, etc. (optional)
                   - quality: list of quality checks (optional)
        contract_id: Contract ID.
        contract_version: Contract version.

    Returns:
        An ODCS v3.x contract dictionary.
    """
    if properties is None:
        properties = [{"name": "user_id", "logicalType": "string", "required": True}]

    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.2",
        "id": contract_id,
        "name": contract_id,
        "version": contract_version,
        "status": "active",
        "schema": [{"name": schema_name, "physicalType": "table", "properties": properties}],
    }


def create_contract_with_quality(
    property_name: str,
    logical_type: str,
    quality_checks: list[dict],
    schema_name: str = "test_table",
) -> dict:
    """
    Create an ODCS v3.x contract with quality checks for a single property.

    Args:
        property_name: Name of the property.
        logical_type: Logical type of the property (string, number, etc.).
        quality_checks: List of ODCS v3.x quality check dictionaries.
                       For DQX rules, use format:
                       {
                           "type": "custom",
                           "engine": "dqx",
                           "implementation": {
                               "name": "rule_name",
                               "criticality": "error|warn",
                               "check": {"function": "...", "arguments": {...}}
                           }
                       }
        schema_name: Name of the schema.

    Returns:
        An ODCS v3.x contract dictionary with quality checks.
    """
    # Convert legacy 'specification' format to 'implementation' for tests still using old format
    converted_quality: list[dict] = []
    for check in quality_checks:
        if check.get("type") == "custom" and check.get("engine") == "dqx" and "specification" in check:
            # Convert old specification format to ODCS v3.x implementation format
            spec = check["specification"]
            converted_check: dict = {
                "type": "custom",
                "engine": "dqx",
                "implementation": {
                    "name": spec.get("name", "unnamed_rule"),
                    "check": spec["check"],
                },
            }
            if "criticality" in spec:
                converted_check["implementation"]["criticality"] = spec["criticality"]  # type: ignore[index]
            converted_quality.append(converted_check)
        else:
            # Already in ODCS v3.x format or other type
            converted_quality.append(check)

    property_def = {"name": property_name, "logicalType": logical_type, "quality": converted_quality}

    return create_basic_contract(schema_name=schema_name, properties=[property_def])


def create_test_contract_file(
    user_id_pattern: str = "^USER-[0-9]{4}$",
    age_min: int = 0,
    age_max: int = 120,
    status_values: list[str] | None = None,
    custom_contract: dict | None = None,
) -> str:
    """
    Create a temporary ODCS v3.x contract file for testing.

    Args:
        user_id_pattern: Pattern for user_id property.
        age_min: Minimum age value.
        age_max: Maximum age value.
        status_values: List of valid status values.
        custom_contract: Optional custom contract dict to use instead of default.

    Returns:
        Path to the temporary contract file.
    """
    if custom_contract:
        contract_dict = custom_contract
    else:
        if status_values is None:
            status_values = ["active", "inactive"]

        # Create ODCS v3.x properties directly
        properties: list[dict] = [
            {
                "name": "user_id",
                "logicalType": "string",
                "required": True,
                "logicalTypeOptions": {"pattern": user_id_pattern},
            },
            {
                "name": "age",
                "logicalType": "integer",
                "logicalTypeOptions": {"minimum": age_min, "maximum": age_max},
            },
            {
                "name": "status",
                "logicalType": "string",
                "logicalTypeOptions": {"pattern": f"^({'|'.join(status_values)})$"},  # enum as pattern
            },
        ]

        contract_dict = create_basic_contract(schema_name="users", properties=properties)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(contract_dict, f)
        return f.name
