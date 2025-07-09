import logging

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.common import val_maybe_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile

logger = logging.getLogger(__name__)


class DQGenerator(DQEngineBase):

    def generate_dq_rules(self, profiles: list[DQProfile] | None = None, level: str = "error") -> list[dict]:
        """
        Generates a list of data quality rules based on the provided dq profiles.

        :param profiles: A list of data quality profiles to generate rules for.
        :param level: The criticality level of the rules (default is "error").
        :return: A list of dictionaries representing the data quality rules.
        """
        if profiles is None:
            profiles = []
        dq_rules = []
        for profile in profiles:
            rule_name = profile.name
            column = profile.column
            params = profile.parameters or {}
            if rule_name not in self._checks_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = self._checks_mapping[rule_name](column, level, **params)
            if expr:
                dq_rules.append(expr)

        status = DQEngine.validate_checks(dq_rules)
        assert not status.has_errors

        return dq_rules

    @staticmethod
    def dq_generate_is_in(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is in a specified list.

        :param column: The name of the column to check.
        :param level: The criticality level of the rule (default is "error").
        :param params: Additional parameters, including the list of values to check against.
        :return: A dictionary representing the data quality rule.
        """
        return {
            "check": {"function": "is_in_list", "arguments": {"column": column, "allowed": params["in"]}},
            "name": f"{column}_other_value",
            "criticality": level,
        }

    @staticmethod
    def dq_generate_min_max(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is within a specified range.

        :param column: The name of the column to check.
        :param level: The criticality level of the rule (default is "error").
        :param params: Additional parameters, including the minimum and maximum values.
        :return: A dictionary representing the data quality rule, or None if no limits are provided.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")

        if not isinstance(min_limit, int) or not isinstance(max_limit, int):
            return None  # TODO handle timestamp and dates: https://github.com/databrickslabs/dqx/issues/71

        if min_limit is not None and max_limit is not None:
            return {
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": column,
                        "min_limit": val_maybe_to_str(min_limit, include_sql_quotes=False),
                        "max_limit": val_maybe_to_str(max_limit, include_sql_quotes=False),
                    },
                },
                "name": f"{column}_isnt_in_range",
                "criticality": level,
            }

        if max_limit is not None:
            return {
                "check": {
                    "function": "is_not_greater_than",
                    "arguments": {
                        "column": column,
                        "limit": val_maybe_to_str(max_limit, include_sql_quotes=False),
                    },
                },
                "name": f"{column}_not_greater_than",
                "criticality": level,
            }

        if min_limit is not None:
            return {
                "check": {
                    "function": "is_not_less_than",
                    "arguments": {
                        "column": column,
                        "limit": val_maybe_to_str(min_limit, include_sql_quotes=False),
                    },
                },
                "name": f"{column}_not_less_than",
                "criticality": level,
            }

        return None

    @staticmethod
    def dq_generate_is_not_null(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null.

        :param column: The name of the column to check.
        :param level: The criticality level of the rule (default is "error").
        :param params: Additional parameters.
        :return: A dictionary representing the data quality rule.
        """
        params = params or {}
        return {
            "check": {"function": "is_not_null", "arguments": {"column": column}},
            "name": f"{column}_is_null",
            "criticality": level,
        }

    @staticmethod
    def dq_generate_is_not_null_or_empty(column: str, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null or empty.

        :param column: The name of the column to check.
        :param level: The criticality level of the rule (default is "error").
        :param params: Additional parameters, including whether to trim strings.
        :return: A dictionary representing the data quality rule.
        """
        return {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": column, "trim_strings": params.get("trim_strings", True)},
            },
            "name": f"{column}_is_null_or_empty",
            "criticality": level,
        }

    _checks_mapping = {
        "is_not_null": dq_generate_is_not_null,
        "is_in": dq_generate_is_in,
        "min_max": dq_generate_min_max,
        "is_not_null_or_empty": dq_generate_is_not_null_or_empty,
    }
