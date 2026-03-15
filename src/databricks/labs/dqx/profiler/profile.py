from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType


@dataclass(frozen=True)
class DQProfile:
    """Data quality profile class representing a data quality rule candidate."""

    name: str
    column: str
    description: str | None = None
    parameters: dict[str, Any] | None = None
    filter: str | None = None


@dataclass(frozen=True)
class DQProfileType:
    """Data quality profile type class representing a data quality rule candidate type."""

    name: str
    builder: Callable[[DataFrame, str, DataType, dict[str, Any], dict[str, Any]], DQProfile | None]
