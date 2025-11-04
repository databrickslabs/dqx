import pyspark.sql.functions as F
from databricks.labs.dqx.llm.validators import RuleValidator, LLMValidationScoreWeights
from databricks.labs.dqx.check_funcs import make_condition, register_rule


def test_llm_validate_valid_json_valid_rules():
    rules_json = '[{"check": {"function": "is_not_null", "arguments": {"column": "customer_id"}}}]'
    score = RuleValidator().validate(rules_json)
    assert score == 1.0


def test_llm_validate_valid_json_invalid_rules():
    rules_json = '[{"check": {"function": "is_not_null"}}]'
    score = RuleValidator().validate(rules_json)
    assert score == 0.2


def test_llm_validate_invalid_json():
    rules_json = (
        '{"check": {"function": "is_not_null", "arguments": {"column": "customer_id"}}'  # Missing closing bracket
    )
    score = RuleValidator().validate(rules_json)
    assert score == 0.0


def test_llm_validate_empty_json():
    rules_json = ''
    score = RuleValidator().validate(rules_json)
    assert score == 0.0


def test_llm_validate_with_custom_weights():
    weights = LLMValidationScoreWeights(json_parsing=0.5, rule_validation=0.5)
    validator = RuleValidator(score_weights=weights)
    rules_json = '[{"check": {"function": "is_not_null", "arguments": {"column": "customer_id"}}}]'
    score = validator.validate(rules_json)
    assert score == 1.0


@register_rule("row")
def dummy_custom_check_function_test(column: str, suffix: str):
    """
    Test the custom check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
    )


def test_llm_validate_valid_json_valid_custom_rules():
    rules_json = '[{"check": {"function": "dummy_custom_check_function_test", "arguments": {"column": "customer_id"}}}]'
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}
    score = RuleValidator(custom_check_functions=custom_check_functions).validate(rules_json)
    assert score == 1.0
