"""
Shared test helpers for data contract tests.
"""

import tempfile

import yaml


def assert_valid_rule_structure(rule: dict) -> None:
    """Assert that a rule has the expected structure."""
    assert "check" in rule
    assert "function" in rule["check"]
    assert "arguments" in rule["check"]
    assert "criticality" in rule
    assert "user_metadata" in rule


def assert_valid_contract_metadata(metadata: dict) -> None:
    """Assert that metadata contains expected contract information."""
    assert "contract_id" in metadata
    assert "contract_version" in metadata
    assert "model" in metadata
    assert "field" in metadata
    assert "rule_type" in metadata
    assert metadata["rule_type"] in {"implicit", "explicit", "text_llm"}


def assert_rules_have_valid_structure(rules: list[dict]) -> None:
    """Assert that all rules have valid structure."""
    assert len(rules) > 0
    for rule in rules:
        assert_valid_rule_structure(rule)


def assert_rules_have_valid_metadata(rules: list[dict]) -> None:
    """Assert that all rules have valid contract metadata."""
    for rule in rules:
        assert_valid_contract_metadata(rule["user_metadata"])


def create_basic_contract(
    model_name: str = "test_table",
    fields: dict | None = None,
    contract_id: str = "test",
    contract_version: str = "1.0.0",
) -> dict:
    """
    Create a basic contract dictionary with specified fields.

    Args:
        model_name: Name of the model.
        fields: Dictionary of field definitions.
        contract_id: Contract ID.
        contract_version: Contract version.

    Returns:
        A contract dictionary.
    """
    if fields is None:
        fields = {"user_id": {"type": "string", "required": True}}

    return {
        "dataContractSpecification": "0.9.3",
        "id": contract_id,
        "info": {"title": contract_id, "version": contract_version},
        "models": {model_name: {"fields": fields}},
    }


def create_contract_with_quality(
    field_name: str,
    field_type: str,
    quality_checks: list[dict],
    model_name: str = "test_table",
) -> dict:
    """
    Create a contract with quality checks for a single field.

    Args:
        field_name: Name of the field.
        field_type: Type of the field.
        quality_checks: List of quality check dictionaries.
        model_name: Name of the model.

    Returns:
        A contract dictionary with quality checks.
    """
    return create_basic_contract(
        model_name=model_name,
        fields={field_name: {"type": field_type, "quality": quality_checks}},
    )


def create_test_contract_file(
    user_id_pattern: str = "^USER-[0-9]{4}$",
    age_min: int = 0,
    age_max: int = 120,
    status_values: list[str] | None = None,
    custom_contract: dict | None = None,
) -> str:
    """
    Create a temporary contract file for testing.

    Args:
        user_id_pattern: Pattern for user_id field.
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

        contract_dict = create_basic_contract(
            model_name="users",
            fields={
                "user_id": {"type": "string", "required": True, "pattern": user_id_pattern},
                "age": {"type": "integer", "minimum": age_min, "maximum": age_max},
                "status": {"type": "string", "enum": status_values},
            },
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(contract_dict, f)
        return f.name
