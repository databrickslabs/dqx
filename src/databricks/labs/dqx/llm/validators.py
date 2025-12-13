import json
import logging
from collections.abc import Callable
from dataclasses import dataclass

from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LLMValidationScoreWeights:
    """Weights for rule validation scoring."""

    json_parsing: float = 0.2
    rule_validation: float = 0.8

    def __post_init__(self):
        """Validate weights sum to 1.0."""
        total = self.json_parsing + self.rule_validation
        if not 0.99 <= total <= 1.01:  # Allow for floating point precision
            raise InvalidParameterError(f"Weights must sum to 1.0, got {total}")


class RuleValidator:
    """
    Validates generated data quality rules using DQX engine.
    """

    def __init__(
        self,
        custom_check_functions: dict[str, Callable] | None = None,
        score_weights: LLMValidationScoreWeights | None = None,
    ):
        """
        Initialize the rule validator.

        Args:
            custom_check_functions: Optional custom check functions to include in validation.
            score_weights: Weights for scoring different aspects of validation.
        """
        self._custom_check_functions = custom_check_functions
        self._score_weights = score_weights or LLMValidationScoreWeights()

    def validate(self, rules_json: str) -> float:
        """
        Validate generated rules with granular scoring.

        Scoring breakdown:
            - JSON parsing: Checks if the output can be parsed as valid JSON.
            - Rules validation: Ensures rules pass DQX checks validation.

        Args:
            rules_json: JSON string of the generated rules.

        Returns:
            Score between 0.0 and 1.0 representing the quality of the generated rules.
        """
        total_score = 0.0

        # JSON parsing score
        json_score = self._validate_json_parsing(rules_json)
        total_score += json_score * self._score_weights.json_parsing

        if json_score == 0.0:
            # Early return if we can't parse JSON
            return total_score

        # Rules validation score
        try:
            rules = json.loads(rules_json)
            rules_score = self._validate_rules_structure(rules)
            total_score += rules_score * self._score_weights.rule_validation
        except json.JSONDecodeError:
            # Should not happen since we already validated, but be defensive
            pass

        logger.debug(f"Final validation score: {total_score:.2f}")
        return total_score

    def _validate_json_parsing(self, rules_json: str) -> float:
        """
        Validate that the rules can be parsed as JSON.

        Args:
            rules_json: JSON string to validate.

        Returns:
            1.0 if valid JSON, 0.0 otherwise.
        """
        try:
            json.loads(rules_json)
            logger.debug("✓ JSON parsing successful")
            return 1.0
        except json.JSONDecodeError as e:
            logger.warning(f"✗ JSON parsing failed: {e}")
            logger.debug(f"  Raw output: {repr(rules_json[:200])}")
            return 0.0

    def _validate_rules_structure(self, rules: list[dict]) -> float:
        """
        Validate that rules pass DQX structure validation.

        Args:
            rules: Parsed rules as list of dictionaries.

        Returns:
            1.0 if validation passes, 0.0 otherwise.
        """
        validation_status = DQEngineCore.validate_checks(rules, self._custom_check_functions)
        if not validation_status.has_errors:
            logger.debug("✓ Rules validation passed")
            return 1.0

        logger.warning(f"✗ Rules validation errors: {validation_status.errors}")
        return 0.0
