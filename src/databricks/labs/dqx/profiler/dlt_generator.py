import json
import re
import logging

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.profiler.common import val_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile

__name_sanitize_re__ = re.compile(r"[^a-zA-Z0-9]+")
logger = logging.getLogger(__name__)


class DQDltGenerator(DQEngineBase):

    def generate_dlt_rules(
        self, rules: list[DQProfile], action: str | None = None, language: str = "SQL"
    ) -> list[str] | str | dict:
        """
        Generates Lakeflow Pipelines (formerly Delta Live Table (DLT)) rules in the specified language.

        :param rules: A list of data quality profiles to generate rules for.
        :param action: The action to take on rule violation (e.g., "drop", "fail").
        :param language: The language to generate the rules in ("SQL", "Python" or "Python_Dict").
        :return: A list of strings representing the Lakeflow Pipelines rules in SQL, a string representing
        the Lakeflow Pipelines rules in Python, or dictionary with expressions.
        :raises ValueError: If the specified language is not supported.
        """

        lang = language.lower()

        if lang == "sql":
            return self._generate_dlt_rules_sql(rules, action)

        if lang == "python":
            return self._generate_dlt_rules_python(rules, action)

        if lang == "python_dict":
            return self._generate_dlt_rules_python_dict(rules)

        raise ValueError(f"Unsupported language '{language}'. Only 'SQL' and 'Python' are supported.")

    @staticmethod
    def _dlt_generate_is_in(column: str, **params: dict):
        """
        Generates a Lakeflow Pipelines (formerly Delta Live Table (DLT)) rule to check if a column's value is in
        a specified list.

        :param column: The name of the column to check.
        :param params: Additional parameters, including the list of values to check against.
        :return: A string representing the Lakeflow Pipelines rule.
        """
        in_str = ", ".join([val_to_str(v) for v in params["in"]])
        return f"{column} in ({in_str})"

    @staticmethod
    def _dlt_generate_min_max(column: str, **params: dict):
        """
        Generates a Lakeflow Pipelines (formerly Delta Live Table (DLT)) rule to check if a column's value is within
        a specified range.

        :param column: The name of the column to check.
        :param params: Additional parameters, including the minimum and maximum values.
        :return: A string representing the Lakeflow Pipelines rule.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")
        if min_limit is not None and max_limit is not None:
            # We can generate `col between(min, max)`,
            # but this one is easier to modify if you need to remove some of the bounds
            return f"{column} >= {val_to_str(min_limit)} and {column} <= {val_to_str(max_limit)}"

        if max_limit is not None:
            return f"{column} <= {val_to_str(max_limit)}"

        if min_limit is not None:
            return f"{column} >= {val_to_str(min_limit)}"

        return ""

    @staticmethod
    def _dlt_generate_is_not_null_or_empty(column: str, **params: dict):
        """
        Generates a Lakeflow Pipelines (formerly Delta Live Table (DLT)) rule to check if a column's value is
        not null or empty.

        :param column: The name of the column to check.
        :param params: Additional parameters, including whether to trim strings.
        :return: A string representing the Lakeflow Pipelines rule.
        """
        trim_strings = params.get("trim_strings", True)
        msg = f"{column} is not null and "
        if trim_strings:
            msg += "trim("
        msg += column
        if trim_strings:
            msg += ")"
        msg += " <> ''"
        return msg

    _checks_mapping = {
        "is_not_null": lambda column, **params: f"{column} is not null",
        "is_in": _dlt_generate_is_in,
        "min_max": _dlt_generate_min_max,
        "is_not_null_or_empty": _dlt_generate_is_not_null_or_empty,
    }

    def _generate_dlt_rules_python_dict(self, rules: list[DQProfile]) -> dict:
        """
        Generates a Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules as Python dictionary.

        :param rules: A list of data quality profiles to generate rules for.
        :return: A dict representing the Lakeflow Pipelines rules in Python.
        """
        expectations = {}
        for rule in rules or []:
            rule_name = rule.name
            column = rule.column
            params = rule.parameters or {}
            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = function_mapping[rule_name](column, **params)
            if expr == "":
                logger.info("Empty expression was generated for rule '{nm}' for column '{cl}'")
                continue
            exp_name = re.sub(__name_sanitize_re__, "_", f"{column}_{rule_name}")
            expectations[exp_name] = expr

        return expectations

    def _generate_dlt_rules_python(self, rules: list[DQProfile], action: str | None = None) -> str:
        """
        Generates a Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules in Python.

        :param rules: A list of data quality profiles to generate rules for.
        :param action: The action to take on rule violation (e.g., "drop", "fail").
        :return: A string representing the Lakeflow Pipelines rules in Python.
        """
        expectations = self._generate_dlt_rules_python_dict(rules)

        if len(expectations) == 0:
            return ""

        json_expectations = json.dumps(expectations)
        expectations_mapping = {
            "drop": "@dlt.expect_all_or_drop",
            "fail": "@dlt.expect_all_or_fail",
            None: "@dlt.expect_all",
        }
        decorator = expectations_mapping.get(action, "@dlt.expect_all")

        return f"""{decorator}(
{json_expectations}
)"""

    def _generate_dlt_rules_sql(self, rules: list[DQProfile], action: str | None = None) -> list[str]:
        """
        Generates a Lakeflow Pipeline (formerly Delta Live Table (DLT)) rules in sql.

        :param rules: A list of data quality profiles to generate rules for.
        :param action: The action to take on rule violation (e.g., "drop", "fail").
        :return: A list of Lakeflow Pipelines rules.
        :raises ValueError: If the specified language is not supported.
        """
        dlt_rules = []
        act_str = ""
        if action == "drop":
            act_str = " ON VIOLATION DROP ROW"
        elif action == "fail":
            act_str = " ON VIOLATION FAIL UPDATE"
        for rule in rules or []:
            rule_name = rule.name
            column = rule.column
            params = rule.parameters or {}
            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{column}'. skipping...")
                continue
            expr = function_mapping[rule_name](column, **params)
            if expr == "":
                logger.info("Empty expression was generated for rule '{nm}' for column '{cl}'")
                continue
            dlt_rule = f"CONSTRAINT {column}_{rule_name} EXPECT ({expr}){act_str}"
            dlt_rules.append(dlt_rule)

        return dlt_rules
