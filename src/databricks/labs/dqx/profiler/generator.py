import logging

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.common import val_maybe_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile
from databricks.labs.dqx.telemetry import telemetry_logger

logger = logging.getLogger(__name__)


class DQGenerator(DQEngineBase):
    @telemetry_logger("generator", "generate_dq_rules")
    def generate_dq_rules(self, profiles: list[DQProfile] | None = None, level: str = "error") -> list[dict]:
        """
        Generates a list of data quality rules based on the provided dq profiles.

        Args:
                profiles: A list of data quality profiles to generate rules for.
                level: The criticality level of the rules (default is "error").

        Returns:
                A list of dictionaries representing the data quality rules.
        """
        if profiles is None:
            profiles = []
        dq_rules = []

        for profile in profiles:
            rule_name = profile.name
            column = profile.column
            params = profile.parameters or {}
            filter = profile.filter
            if rule_name not in self._checks_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = self._checks_mapping[rule_name](column, filter, level, **params)
            if expr:
                dq_rules.append(expr)

        status = DQEngine.validate_checks(dq_rules)
        assert not status.has_errors

        return dq_rules

    @staticmethod
    def dq_generate_is_in(column: str, filter: str | None, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is in a specified list.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters, including the list of values to check against.

        Returns:
                A dictionary representing the data quality rule.
        """
        return {
<<<<<<< HEAD
            "check": {"function": "is_in_list", "arguments": {"column": column, "allowed": params["in"]}},
            "filter": filter,
=======
            "check": {"function": "is_in_list", "arguments": {"column": column, "allowed": params["in"],"filter": params.get("filter", None)}},
>>>>>>> 07d81a3 (Refactor filter implementation in the profiler class. Changing from inserting key, value pair in parameter attribute to adding new class attribute called filter.)
            "name": f"{column}_other_value",
            "criticality": level            
        }

    @staticmethod
    def dq_generate_min_max(column: str, filter: str | None, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is within a specified range.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters, including the minimum and maximum values.

        Returns:
                A dictionary representing the data quality rule, or None if no limits are provided.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")
        filter = params.get("filter", None)

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
                        "filter": params.get("filter", None)
                    },
                },
                "filter": filter,
                "name": f"{column}_isnt_in_range",
                "criticality": level                
            }

        if max_limit is not None:
            return {
                "check": {
                    "function": "is_not_greater_than",
<<<<<<< HEAD
                    "arguments": {"column": column, "limit": val_maybe_to_str(max_limit, include_sql_quotes=False)},
=======
                    "arguments": {
                        "column": column,
                        "limit": val_maybe_to_str(max_limit, include_sql_quotes=False),
                        "filter": params.get("filter", None)
                    },
>>>>>>> 07d81a3 (Refactor filter implementation in the profiler class. Changing from inserting key, value pair in parameter attribute to adding new class attribute called filter.)
                },
                "filter": filter,
                "name": f"{column}_not_greater_than",
                "criticality": level              
            }

        if min_limit is not None:
            return {
                "check": {
                    "function": "is_not_less_than",
<<<<<<< HEAD
                    "arguments": {"column": column, "limit": val_maybe_to_str(min_limit, include_sql_quotes=False)},
=======
                    "arguments": {
                        "column": column,
                        "limit": val_maybe_to_str(min_limit, include_sql_quotes=False),
                        "filter": params.get("filter", None)
                    },
>>>>>>> 07d81a3 (Refactor filter implementation in the profiler class. Changing from inserting key, value pair in parameter attribute to adding new class attribute called filter.)
                },
                "filter": filter,
                "name": f"{column}_not_less_than",
                "criticality": level                
            }

        return None

    @staticmethod
    def dq_generate_is_not_null(column: str, filter: str | None, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters.

        Returns:
                A dictionary representing the data quality rule.
        """
        params = params or {}
<<<<<<< HEAD
<<<<<<< HEAD

=======
        filter = params.get("dataset_filter_expression", None)
>>>>>>> 0377f1e (Modifying DQGenerator class to show filter in the generate_dq_rules method. Added test in the test_rules_generator.py file.)
        return {
            "check": {"function": "is_not_null", "arguments": {"column": column}},
            "filter": filter,
=======
        filter = params.get("filter", None)
        return {
            "check": {"function": "is_not_null", "arguments": {"column": column, "filter": params.get("filter", None)}},
>>>>>>> 07d81a3 (Refactor filter implementation in the profiler class. Changing from inserting key, value pair in parameter attribute to adding new class attribute called filter.)
            "name": f"{column}_is_null",
            "criticality": level            
        }

    @staticmethod
    def dq_generate_is_not_null_or_empty(column: str, filter: str | None, level: str = "error", **params: dict):
        """
        Generates a data quality rule to check if a column's value is not null or empty.

        Args:
                column: The name of the column to check.
                level: The criticality level of the rule (default is "error").
                params: Additional parameters, including whether to trim strings.

        Returns:
                A dictionary representing the data quality rule.
        """
<<<<<<< HEAD
<<<<<<< HEAD

=======
        filter = params.get("dataset_filter_expression", None)
>>>>>>> 0377f1e (Modifying DQGenerator class to show filter in the generate_dq_rules method. Added test in the test_rules_generator.py file.)
=======
        filter = params.get("filter", None)
>>>>>>> 07d81a3 (Refactor filter implementation in the profiler class. Changing from inserting key, value pair in parameter attribute to adding new class attribute called filter.)
        return {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": column, "trim_strings": params.get("trim_strings", True), "filter": params.get("filter", None)},
            },
            "filter": filter,
            "name": f"{column}_is_null_or_empty",
            "criticality": level            
        }

    _checks_mapping = {
        "is_not_null": dq_generate_is_not_null,
        "is_in": dq_generate_is_in,
        "min_max": dq_generate_min_max,
        "is_not_null_or_empty": dq_generate_is_not_null_or_empty,
    }
