import json
import re
from typing import Union

from databricks.labs.dqx.profiler.common import val_to_str
from databricks.labs.dqx.profiler.profiler import DQRule

__name_sanitize_re__ = re.compile(r"[^a-zA-Z0-9]+")


def dlt_generate_is_in(cl, **params: dict):
    in_str = ", ".join([val_to_str(v) for v in params["in"]])
    return f"{cl} in ({in_str})"


def dlt_generate_min_max(cl, **params: dict):
    mn = params.get("min")
    mx = params.get("max")
    if mn is not None and mx is not None:
        # We can generate `col between(min, max)`, but this one is easier to modify if you need to remove some of the bounds
        return f"{cl} >= {val_to_str(mn)} and {cl} <= {val_to_str(mx)}"
    elif mx is not None:
        return f"{cl} <= {val_to_str(mx)}"
    elif mn is not None:
        return f"{cl} >= {val_to_str(mn)}"

    return ""


def dlt_generate_is_not_null_or_empty(cl, **params: dict):
    trim_strings = params.get("trim_strings", True)
    s = f"{cl} is not null and "
    if trim_strings:
        s += "trim("
    s += cl
    if trim_strings:
        s += ")"
    s += " <> ''"
    return s


dlt_mapping = {
    "is_not_null": lambda cl, **params: f"{cl} is not null",
    "is_in": dlt_generate_is_in,
    "min_max": dlt_generate_min_max,
    "is_not_null_or_empty": dlt_generate_is_not_null_or_empty,
}


def generate_dlt_rules_python(rules: list[DQRule], action: str | None = None) -> str:
    if rules is None or len(rules) == 0:
        return ""

    expectations = {}
    for rule in rules:
        nm = rule.name
        cl = rule.column
        params = rule.parameters or {}
        if nm not in dlt_mapping:
            print(f"No rule '{nm}' for column '{cl}'. skipping...")
            continue
        expr = dlt_mapping[nm](cl, **params)
        if expr == "":
            print("Empty expression was generated for rule '{nm}' for column '{cl}'")
            continue
        exp_name = re.sub(__name_sanitize_re__, "_", f"{cl}_{nm}")
        expectations[exp_name] = expr

    if len(expectations) == 0:
        return ""

    t = json.dumps(expectations)
    if action == "drop":
        exp_str = f"""@dlt.expect_all_or_drop(
{t}
)"""
    elif action == "fail":
        exp_str = f"""@dlt.expect_all_or_fail(
{t}
)"""
    else:
        exp_str = f"""@dlt.expect_all(
{t}
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
        nm = rule.name
        cl = rule.column
        params = rule.parameters or {}
        if nm not in dlt_mapping:
            print(f"No rule '{nm}' for column '{cl}'. skipping...")
            continue
        expr = dlt_mapping[nm](cl, **params)
        if expr == "":
            print("Empty expression was generated for rule '{nm}' for column '{cl}'")
            continue
        # TODO: generate constraint name in lower_case, etc.
        dlt_rule = f"CONSTRAINT {cl}_{nm} EXPECT ({expr}){act_str}"
        dlt_rules.append(dlt_rule)

    return dlt_rules


def generate_dlt_rules(rules: list[DQRule], action: str | None = None, language: str = "SQL") -> Union[list[str], str]:
    lang = language.lower()
    if lang == "sql":
        return generate_dlt_rules_sql(rules, action)
    elif lang == "python":
        return generate_dlt_rules_python(rules, action)
    else:
        raise Exception(f"Unsupported language '{language}'")
