"""Shared helpers for data contract (ODCS) tests."""


def get_schema_validation_rules(rules):
    """Return rules that are has_valid_schema schema_validation rules."""
    return [
        rule
        for rule in rules
        if rule.get("check", {}).get("function") == "has_valid_schema"
        and rule.get("user_metadata", {}).get("rule_type") == "schema_validation"
    ]
