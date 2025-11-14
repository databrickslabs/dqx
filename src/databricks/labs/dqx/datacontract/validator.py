"""
ODCS contract validation using JSON Schema.

This module provides validation for ODCS (Open Data Contract Standard) contracts
against the official JSON Schema specification.
"""

import json
import logging
from pathlib import Path
from typing import Any

from jsonschema import Draft7Validator

logger = logging.getLogger(__name__)


class ODCSValidationError(ValueError):
    """Raised when an ODCS contract fails validation."""

    def __init__(self, message: str, errors: list[str] | None = None):
        super().__init__(message)
        self.errors = errors or []


class ODCSValidator:
    """Validator for ODCS contracts using JSON Schema."""

    # Supported ODCS schema versions
    SUPPORTED_VERSIONS = {
        'v3.0.0': 'odcs-json-schema-v3.0.0.json',
        'v3.0.1': 'odcs-json-schema-v3.0.1.json',
        'v3.0.2': 'odcs-json-schema-v3.0.2.json',
    }

    def __init__(self):
        """Initialize the validator."""
        self._schemas: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, Draft7Validator] = {}

    def _get_schema_version(self, contract: dict[str, Any]) -> str:
        """
        Determine which schema version to use based on the contract's apiVersion.

        Args:
            contract: The ODCS contract.

        Returns:
            The schema version to use.

        Raises:
            ODCSValidationError: If apiVersion is missing or unsupported.
        """
        api_version = contract.get('apiVersion')

        if not api_version:
            raise ODCSValidationError(
                "Missing required field: 'apiVersion'",
                errors=["Field 'apiVersion' is required (e.g., 'v3.0.2')"]
            )

        # Check if we support this version
        if api_version in self.SUPPORTED_VERSIONS:
            return api_version

        # Unsupported version
        supported = ', '.join(self.SUPPORTED_VERSIONS.keys())
        raise ODCSValidationError(
            f"Unsupported apiVersion: '{api_version}'",
            errors=[f"Supported versions: {supported}"]
        )

    def _load_schema(self, version: str) -> dict[str, Any]:
        """
        Load the ODCS JSON Schema for a specific version.

        Args:
            version: The schema version (e.g., 'v3.0.2').

        Returns:
            The loaded JSON Schema.

        Raises:
            FileNotFoundError: If the schema file is not found.
        """
        if version in self._schemas:
            return self._schemas[version]

        schema_filename = self.SUPPORTED_VERSIONS[version]
        schema_path = Path(__file__).parent / "schemas" / schema_filename

        if not schema_path.exists():
            raise FileNotFoundError(
                f"ODCS JSON Schema not found: {schema_path}. "
                f"Expected bundled schema file for version {version}."
            )

        try:
            with open(schema_path, encoding='utf-8') as f:
                schema = json.load(f)
                self._schemas[version] = schema
                return schema
        except (OSError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Failed to load ODCS schema from {schema_path}: {e}") from e

    def validate(self, contract: dict[str, Any]) -> None:
        """
        Validate an ODCS contract against the JSON Schema.

        Args:
            contract: The ODCS contract to validate.

        Raises:
            ODCSValidationError: If the contract is invalid.
        """
        # Determine which schema version to use (validates apiVersion)
        version = self._get_schema_version(contract)

        # Load the appropriate schema
        try:
            schema = self._load_schema(version)
        except (FileNotFoundError, RuntimeError) as e:
            raise ODCSValidationError(f"Failed to load ODCS schema: {e}") from e

        # Get or create validator for this version
        if version not in self._validators:
            self._validators[version] = Draft7Validator(schema)

        validator = self._validators[version]

        # Perform JSON Schema validation
        errors = list(validator.iter_errors(contract))
        if errors:
            error_messages = self._format_validation_errors(errors)
            raise ODCSValidationError(
                f"ODCS contract validation failed with {len(errors)} error(s)",
                errors=error_messages
            )

    @staticmethod
    def _format_validation_errors(errors: list) -> list[str]:
        """
        Format JSON Schema validation errors into readable messages.

        Args:
            errors: List of jsonschema ValidationError objects.

        Returns:
            List of formatted error messages.
        """
        formatted_errors = []
        for error in errors:
            path = ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
            formatted_errors.append(f"At '{path}': {error.message}")
        return formatted_errors


# Singleton instance for reusing loaded schemas
_validator_instance: ODCSValidator | None = None


def validate_contract(contract: dict[str, Any]) -> None:
    """
    Validate an ODCS contract against its JSON Schema specification.

    Args:
        contract: Dictionary representation of an ODCS contract.

    Raises:
        ODCSValidationError: If the contract is invalid.
    """
    global _validator_instance
    if _validator_instance is None:
        _validator_instance = ODCSValidator()
    _validator_instance.validate(contract)

