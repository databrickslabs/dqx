"""
ODCS (Open Data Contract Standard) v3.0.x format support.

This module provides classes and utilities for parsing and working with
ODCS data contracts.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from databricks.labs.dqx.datacontract.formats.base import BaseContract, BaseProperty


@dataclass(frozen=True)
class ODCSProperty(BaseProperty):
    """
    Represents a property/column from an ODCS (Open Data Contract Standard) schema.

    This dataclass encapsulates all relevant metadata and quality constraints for a single
    property defined in an ODCS v3.0.x data contract.
    """

    logical_type: str | None = None
    physical_type: str | None = None
    unique: bool = False
    not_null: bool = False
    not_empty: bool = False
    min_value: Any = None
    max_value: Any = None
    valid_values: list[Any] | None = None
    pattern: str | None = None
    format: str | None = None
    quality: dict | None = None

    def get_quality_constraints(self) -> dict[str, Any]:
        """
        Return quality constraints for this property.

        Returns:
            Dictionary of constraint type to constraint value.
        """
        constraints: dict[str, Any] = {}
        if self.required or self.not_null:
            constraints['not_null'] = True
        if self.not_empty:
            constraints['not_empty'] = True
        if self.unique:
            constraints['unique'] = True
        if self.min_value is not None:
            constraints['min_value'] = self.min_value
        if self.max_value is not None:
            constraints['max_value'] = self.max_value
        if self.valid_values is not None:
            constraints['valid_values'] = self.valid_values
        if self.pattern is not None:
            constraints['pattern'] = self.pattern
        if self.format is not None:
            constraints['format'] = self.format
        return constraints


@dataclass(frozen=True)
class ODCSContract(BaseContract):
    """
    Represents a parsed ODCS (Open Data Contract Standard) data contract.

    This dataclass provides structured access to the contract metadata and schema properties,
    supporting ODCS v3.0.x specification format.
    """

    api_version: str
    name: str
    version: str
    domain: str | None = None
    data_product: str | None = None
    properties: list[ODCSProperty] = field(default_factory=list)
    dataset_quality: dict | None = None

    @classmethod
    def from_dict(cls, data: dict) -> 'ODCSContract':
        """
        Parse an ODCS data contract from a dictionary.

        Args:
            data: Dictionary representation of an ODCS contract (typically loaded from YAML).

        Returns:
            Parsed ODCSContract instance.

        Raises:
            ValueError: If required fields are missing or invalid from the contract.
        """
        # Validate required fields
        if 'name' not in data:
            raise ValueError("Failed to parse ODCS contract: Missing required field 'name'")
        if 'version' not in data:
            raise ValueError("Failed to parse ODCS contract: Missing required field 'version'")

        # Parse schema properties
        properties = []
        schema = data.get('schema', {})

        # Handle both table/column format and object/property format
        if 'columns' in schema or 'properties' in schema:
            props = schema.get('columns', schema.get('properties', {}))
            for prop_name, prop_data in props.items():
                properties.append(cls._parse_property(prop_name, prop_data))

        # Extract dataset-level quality rules
        quality_section = data.get('quality', {})
        dataset_quality = quality_section if isinstance(quality_section, dict) else None

        return cls(
            api_version=data.get('apiVersion', ''),
            name=data['name'],
            version=data['version'],
            domain=data.get('domain'),
            data_product=data.get('dataProduct'),
            properties=properties,
            dataset_quality=dataset_quality,
        )

    @staticmethod
    def _parse_property(name: str, data: dict) -> ODCSProperty:
        """
        Parse a single property/column from ODCS schema.

        Args:
            name: Property name.
            data: Property definition dictionary.

        Returns:
            Parsed ODCSProperty instance.
        """
        quality_rules = data.get('quality', {})

        # Extract quality constraints from both top-level and quality section
        # Use explicit None checks to handle 0 values correctly
        min_value = data.get('minValue')
        if min_value is None:
            min_value = quality_rules.get('minValue')

        max_value = data.get('maxValue')
        if max_value is None:
            max_value = quality_rules.get('maxValue')

        return ODCSProperty(
            name=name,
            logical_type=data.get('logicalType'),
            physical_type=data.get('physicalType'),
            description=data.get('description'),
            required=data.get('required', False),
            unique=data.get('unique', False),
            not_null=quality_rules.get('notNull', data.get('notNull', False)),
            not_empty=quality_rules.get('notEmpty', data.get('notEmpty', False)),
            min_value=min_value,
            max_value=max_value,
            valid_values=data.get('validValues') or quality_rules.get('validValues'),
            pattern=data.get('pattern') or quality_rules.get('pattern'),
            format=data.get('format'),
            quality=quality_rules if quality_rules else None,
        )

    def get_properties(self) -> Sequence[BaseProperty]:
        """
        Get all properties from the contract.

        Returns:
            Sequence of ODCSProperty instances (as BaseProperty).
        """
        return self.properties
