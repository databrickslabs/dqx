from databricks.labs.dqx.profiler.common import val_maybe_to_str
from databricks.labs.dqx.profiler.profiler import DQRule


def dq_generate_is_in(col_name: str, level: str = "error", **params: dict):
    return {
        "check": {"function": "col_value_is_in_list", "arguments": {"col_name": col_name, "allowed": params["in"]}},
        "name": f"{col_name}_other_value",
        "criticality": level,
    }


# TODO: rewrite it
def dq_generate_min_max(col_name: str, level: str = "error", **params: dict):
    min_limit = params.get("min")
    max_limit = params.get("max")

    if min_limit is not None and max_limit is not None:
        return {
            "check": {
                "function": "col_is_in_range",
                "arguments": {
                    "col_name": col_name,
                    "min_limit": val_maybe_to_str(min_limit, include_sql_quotes=False),
                    "max_limit": val_maybe_to_str(max_limit, include_sql_quotes=False),
                },
            },
            "name": f"{col_name}_isnt_in_range",
            "criticality": level,
        }

    if max_limit is not None:
        return {
            "check": {
                "function": "col_not_greater_than",
                "arguments": {
                    "col_name": col_name,
                    "val": val_maybe_to_str(max_limit, include_sql_quotes=False),
                },
            },
            "name": f"{col_name}_not_greater_than",
            "criticality": level,
        }

    if min_limit is not None:
        return {
            "check": {
                "function": "col_not_less_than",
                "arguments": {
                    "col_name": col_name,
                    "val": val_maybe_to_str(min_limit, include_sql_quotes=False),
                },
            },
            "name": f"{col_name}_not_less_than",
            "criticality": level,
        }

    return None


def dq_generate_is_not_null(col_name: str, level: str = "error", **params: dict):  # pylint: disable=unused-argument
    return {
        "check": {"function": "col_is_not_null", "arguments": {"col_name": col_name}},
        "name": f"{col_name}_is_null",
        "criticality": level,
    }


def dq_generate_is_not_null_or_empty(col_name: str, level: str = "error", **params: dict):
    return {
        "check": {
            "function": "col_is_not_null_and_not_empty",
            "arguments": {"col_name": col_name, "trim_strings": params.get("trim_strings", True)},
        },
        "name": f"{col_name}_is_null_or_empty",
        "criticality": level,
    }


dq_mapping = {
    "is_not_null": dq_generate_is_not_null,
    "is_in": dq_generate_is_in,
    "min_max": dq_generate_min_max,
    "is_not_null_or_empty": dq_generate_is_not_null_or_empty,
}


def generate_dq_rules(rules: list[DQRule] | None = None, level: str = "error") -> list[dict]:
    if rules is None:
        rules = []
    dq_rules = []
    for rule in rules:
        rule_name = rule.name
        col_name = rule.column
        params = rule.parameters or {}
        if rule_name not in dq_mapping:
            print(f"No rule '{rule_name}' for column '{col_name}'. skipping...")
            continue
        expr = dq_mapping[rule_name](col_name, level, **params)
        if expr:
            dq_rules.append(expr)

    return dq_rules
