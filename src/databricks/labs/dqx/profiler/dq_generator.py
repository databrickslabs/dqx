from typing import Optional

from databricks.labs.dqx.profiler.common import val_maybe_to_str
from databricks.labs.dqx.profiler.profiler import DQRule


def dq_generate_is_in(cl: str, level: str = "error", **params: dict):
    return {
        "check": {"function": "col_value_is_in_list", "arguments": {"col_name": cl, "allowed": params["in"]}},
        "name": f"{cl}_other_value",
        "criticality": level,
    }


# TODO: rewrite it
def dq_generate_min_max(cl: str, level: str = "error", **params: dict):
    mn = params.get("min")
    mx = params.get("max")

    if mn is not None and mx is not None:
        return {
            "check": {
                "function": "col_is_in_range",
                "arguments": {
                    "col_name": cl,
                    "min_limit": val_maybe_to_str(mn, include_sql_quotes=False),
                    "max_limit": val_maybe_to_str(mx, include_sql_quotes=False),
                },
            },
            "name": f"{cl}_isnt_in_range",
            "criticality": level,
        }
    elif mx is not None:
        return {
            "check": {
                "function": "col_not_greater_than",
                "arguments": {
                    "col_name": cl,
                    "val": val_maybe_to_str(mx, include_sql_quotes=False),
                },
            },
            "name": f"{cl}_not_greater_than",
            "criticality": level,
        }
    elif mn is not None:
        return {
            "check": {
                "function": "col_not_less_than",
                "arguments": {
                    "col_name": cl,
                    "val": val_maybe_to_str(mn, include_sql_quotes=False),
                },
            },
            "name": f"{cl}_not_less_than",
            "criticality": level,
        }

    return None


def dq_generate_is_not_null(cl: str, level: str = "error", **params: dict):
    return {
        "check": {"function": "col_is_not_null", "arguments": {"col_name": cl}},
        "name": f"{cl}_is_null",
        "criticality": level,
    }


def dq_generate_is_not_null_or_empty(cl: str, level: str = "error", **params: dict):
    return {
        "check": {
            "function": "col_is_not_null_and_not_empty",
            "arguments": {"col_name": cl, "trim_strings": params.get("trim_strings", True)},
        },
        "name": f"{cl}_is_null_or_empty",
        "criticality": level,
    }


dq_mapping = {
    "is_not_null": dq_generate_is_not_null,
    "is_in": dq_generate_is_in,
    "min_max": dq_generate_min_max,
    "is_not_null_or_empty": dq_generate_is_not_null_or_empty,
}


def generate_dq_rules(rules: Optional[list[DQRule]] = None, level: str = "error") -> list[dict]:
    if rules is None:
        rules = []
    dq_rules = []
    for rule in rules:
        nm = rule.name
        cl = rule.column
        params = rule.parameters or {}
        if nm not in dq_mapping:
            print(f"No rule '{nm}' for column '{cl}'. skipping...")
            continue
        expr = dq_mapping[nm](cl, level, **params)
        if expr:
            dq_rules.append(expr)

    return dq_rules
