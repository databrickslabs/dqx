"""
Base classes for data contract formats.

This module provides abstract base classes that can be extended to support
different data contract specifications beyond ODCS.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BaseProperty(ABC):
    """
    Abstract base class for a property/column in a data contract.

    This can be extended for different contract formats (ODCS, Protobuf, etc.)
    """

    name: str
    description: str | None = None
    required: bool = False

    @abstractmethod
    def get_quality_constraints(self) -> dict[str, Any]:
        """
        Return quality constraints for this property.

        Returns:
            Dictionary of constraint type to constraint value.
        """


@dataclass(frozen=True)
class BaseContract(ABC):
    """
    Abstract base class for a data contract.

    This can be extended for different contract formats (ODCS, Data Contract CLI, etc.)
    """

    name: str
    version: str

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict) -> 'BaseContract':
        """
        Parse a contract from a dictionary representation.

        Args:
            data: Dictionary representation of the contract.

        Returns:
            Parsed contract instance.
        """

    @abstractmethod
    def get_properties(self) -> Sequence[BaseProperty]:
        """
        Get all properties/columns from the contract.

        Returns:
            Sequence of properties.
        """
