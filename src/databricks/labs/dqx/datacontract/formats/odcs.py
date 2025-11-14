"""
ODCS (Open Data Contract Standard) v3.0.x format support.

This module provides classes and utilities for parsing and working with
ODCS data contracts.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from databricks.labs.dqx.datacontract.formats.base import BaseContract, BaseProperty

logger = logging.getLogger(__name__)


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
        # Validate contract against ODCS JSON Schema
        from databricks.labs.dqx.datacontract.validator import validate_contract, ODCSValidationError

        try:
            validate_contract(data)
        except ODCSValidationError as e:
            # Provide detailed error message with all validation errors
            error_details = "\n  - ".join(e.errors) if e.errors else str(e)
            raise ValueError(f"ODCS contract validation failed:\n  - {error_details}") from e

        # Parse schema properties
        # ODCS v3.0.x uses array of schema objects format
        properties = []
        schema_list = data.get('schema', [])
        
        if not isinstance(schema_list, list):
            raise ValueError("ODCS schema must be an array of schema objects")

        # Iterate through schema objects (typically one table/dataset)
        for schema_obj in schema_list:
            if not isinstance(schema_obj, dict):
                continue
                
            # Get properties array from schema object
            props_list = schema_obj.get('properties', [])
            if not isinstance(props_list, list):
                logger.warning(f"Schema object properties must be an array, got {type(props_list)}")
                continue
                
            # Parse each property
            for prop_data in props_list:
                if not isinstance(prop_data, dict):
                    continue
                    
                prop_name = prop_data.get('name')
                if not prop_name:
                    logger.warning("Property missing 'name' field, skipping")
                    continue
                    
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

        ODCS v3.0.x format has quality as an array of quality check objects.
        Each quality check has a 'type' field: text, library, sql, or custom.

        Args:
            name: Property name.
            data: Property definition dictionary.

        Returns:
            Parsed ODCSProperty instance.
        """
        # ODCS v3.0.x: quality is an array of quality checks
        quality_array = data.get('quality', [])
        
        if not isinstance(quality_array, list):
            quality_array = []

        # Extract constraints from quality array
        not_null = False
        not_empty = False
        min_value = data.get('minValue')
        max_value = data.get('maxValue')
        valid_values = data.get('validValues')
        pattern = data.get('pattern')
        
        # Parse quality checks to extract constraints
        for quality_check in quality_array:
            if not isinstance(quality_check, dict):
                continue
                
            check_type = quality_check.get('type')
            
            # For custom checks with DQX engine, extract from implementation
            if check_type == 'custom' and quality_check.get('engine') == 'dqx':
                implementation = quality_check.get('implementation', {})
                if isinstance(implementation, dict):
                    if 'notNull' in implementation:
                        not_null = implementation.get('notNull', False)
                    if 'notEmpty' in implementation:
                        not_empty = implementation.get('notEmpty', False)
                    if 'minValue' in implementation and min_value is None:
                        min_value = implementation.get('minValue')
                    if 'maxValue' in implementation and max_value is None:
                        max_value = implementation.get('maxValue')
            # Extract library-type quality attributes
            elif check_type == 'library':
                attribute = quality_check.get('attribute', '')
                if attribute == 'notNull':
                    not_null = True
                elif attribute == 'notEmpty':
                    not_empty = True

        return ODCSProperty(
            name=name,
            logical_type=data.get('logicalType'),
            physical_type=data.get('physicalType'),
            description=data.get('description'),
            required=data.get('required', False),
            unique=data.get('unique', False),
            not_null=not_null or data.get('notNull', False),
            not_empty=not_empty or data.get('notEmpty', False),
            min_value=min_value,
            max_value=max_value,
            valid_values=valid_values,
            pattern=pattern,
            format=data.get('format'),
            quality=quality_array if quality_array else None,
        )

    def get_properties(self) -> Sequence[BaseProperty]:
        """
        Get all properties from the contract.

        Returns:
            Sequence of ODCSProperty instances (as BaseProperty).
        """
        return self.properties
