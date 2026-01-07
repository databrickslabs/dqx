"""Data models for SDP migration converter."""

from dataclasses import dataclass
from typing import Any

from databricks.labs.dqx.rule import DQRowRule


@dataclass
class SDPMigrationResult:
    """Result of converting a DQRowRule to SDP Expectation format.

    Attributes:
        name: The name of the expectation (sanitized and valid identifier).
        expression: The SQL expression for the expectation.
        original_rule: The original DQRowRule or dict that was converted.
        supported: Whether the conversion was successful.
        error_message: Optional error message if conversion failed.
    """

    name: str
    expression: str
    original_rule: DQRowRule | dict[str, Any]
    supported: bool
    error_message: str | None = None
