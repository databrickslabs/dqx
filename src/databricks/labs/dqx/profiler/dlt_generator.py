import json
import re

from databricks.labs.dqx.profiler.common import val_to_str
from databricks.labs.dqx.profiler.profiler import DQRule

__name_sanitize_re__ = re.compile(r"[^a-zA-Z0-9]+")


def dlt_generate_is_in(col_name, **params: dict):
    in_str = ", ".join([val_to_str(v) for v in params["in"]])
    return f"{col_name} in ({in_str})"


def dlt_generate_min_max(col_name, **params: dict):
    min_limit = params.get("min")
    max_limit = params.get("max")
    if min_limit is not None and max_limit is not None:
        # We can generate `col between(min, max)`, but this one is easier to modify if you need to remove some of the bounds
        return f"{col_name} >= {val_to_str(min_limit)} and {col_name} <= {val_to_str(max_limit)}"

    if max_limit is not None:
        return f"{col_name} <= {val_to_str(max_limit)}"

    if min_limit is not None:
        return f"{col_name} >= {val_to_str(min_limit)}"

    return ""


def dlt_generate_is_not_null_or_empty(col_name, **params: dict):
    trim_strings = params.get("trim_strings", True)
    msg = f"{col_name} is not null and "
    if trim_strings:
        msg += "trim("
    msg += col_name
    if trim_strings:
        msg += ")"
    msg += " <> ''"
    return msg


dlt_mapping = {
    "is_not_null": lambda col_name, **params: f"{col_name} is not null",
    "is_in": dlt_generate_is_in,
    "min_max": dlt_generate_min_max,
    "is_not_null_or_empty": dlt_generate_is_not_null_or_empty,
}


def generate_dlt_rules_python(rules: list[DQRule], action: str | None = None) -> str:
    if rules is None or len(rules) == 0:
        return ""

    expectations = {}
    for rule in rules:
        rule_name = rule.name
        col_name = rule.column
        params = rule.parameters or {}
        if rule_name not in dlt_mapping:
            print(f"No rule '{rule_name}' for column '{col_name}'. skipping...")
            continue
        expr = dlt_mapping[rule_name](col_name, **params)
        if expr == "":
            print("Empty expression was generated for rule '{nm}' for column '{cl}'")
            continue
        exp_name = re.sub(__name_sanitize_re__, "_", f"{col_name}_{rule_name}")
        expectations[exp_name] = expr

    if len(expectations) == 0:
        return ""

    json_expectations = json.dumps(expectations)
    if action == "drop":
        exp_str = f"""@dlt.expect_all_or_drop(
{json_expectations}
)"""
    elif action == "fail":
        exp_str = f"""@dlt.expect_all_or_fail(
{json_expectations}
)"""
    else:
        exp_str = f"""@dlt.expect_all(
{json_expectations}
)"""
    return exp_str


def generate_dlt_rules_sql(rules: list[DQRule], action: str | None = None) -> list[str]:
    if rules is None or len(rules) == 0:
        return []

    dlt_rules = []
    act_str = ""
    if action == "drop":
        act_str = " ON VIOLATION DROP ROW"
    elif action == "fail":
        act_str = " ON VIOLATION FAIL UPDATE"
    for rule in rules:
        rule_name = rule.name
        col_name = rule.column
        params = rule.parameters or {}
        if rule_name not in dlt_mapping:
            print(f"No rule '{rule_name}' for column '{col_name}'. skipping...")
            continue
        expr = dlt_mapping[rule_name](col_name, **params)
        if expr == "":
            print("Empty expression was generated for rule '{nm}' for column '{cl}'")
            continue
        # TODO: generate constraint name in lower_case, etc.
        dlt_rule = f"CONSTRAINT {col_name}_{rule_name} EXPECT ({expr}){act_str}"
        dlt_rules.append(dlt_rule)

    return dlt_rules


def generate_dlt_rules(rules: list[DQRule], action: str | None = None, language: str = "SQL") -> list[str] | str:
    lang = language.lower()

    if lang == "sql":
        return generate_dlt_rules_sql(rules, action)

    if lang == "python":
        return generate_dlt_rules_python(rules, action)

    raise ValueError(f"Unsupported language '{language}'")
