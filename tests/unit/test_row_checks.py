from typing import cast
import pytest
import pyspark.sql.functions as F
from databricks.labs.dqx.utils import get_column_name_or_alias
from databricks.labs.dqx.check_funcs import (
    is_equal_to,
    is_not_equal_to,
    is_in_range,
    is_not_in_range,
    is_not_greater_than,
    is_not_less_than,
    is_in_list,
    is_not_null_and_is_in_list,
    is_aggr_not_greater_than,
    has_valid_string_case,
    is_ipv4_address_in_cidr,
    is_ipv6_address_in_cidr,
    sql_expression,
)
from databricks.labs.dqx.pii.pii_detection_funcs import does_not_contain_pii
from databricks.labs.dqx.errors import MissingParameterError, InvalidParameterError

LIMIT_VALUE_ERROR = "Limit is not provided"


@pytest.mark.parametrize("min_limit, max_limit", [(None, 1), (1, None)])
def test_col_is_in_range_missing_limits(min_limit, max_limit):
    with pytest.raises(MissingParameterError, match=LIMIT_VALUE_ERROR):
        is_in_range("a", min_limit, max_limit)


@pytest.mark.parametrize("min_limit, max_limit", [(None, 1), (1, None)])
def test_col_is_not_in_range_missing_limits(min_limit, max_limit):
    with pytest.raises(MissingParameterError, match=LIMIT_VALUE_ERROR):
        is_not_in_range("a", min_limit, max_limit)


def test_col_not_greater_than_missing_limit():
    with pytest.raises(MissingParameterError, match=LIMIT_VALUE_ERROR):
        is_not_greater_than("a", limit=None)


def test_col_not_less_than_missing_limit():
    with pytest.raises(MissingParameterError, match=LIMIT_VALUE_ERROR):
        is_not_less_than("a", limit=None)


def test_col_is_not_null_and_is_in_list_missing_allowed_list():
    with pytest.raises(InvalidParameterError, match="allowed list must not be empty."):
        is_not_null_and_is_in_list("a", allowed=[])


def test_col_is_in_list_missing_allowed_list():
    with pytest.raises(InvalidParameterError, match="allowed list must not be empty."):
        is_in_list("a", allowed=[])


@pytest.mark.parametrize(
    "case, expected_message",
    [
        ("camel", "'case' must be one of ['lower', 'sentence', 'title', 'upper'], got 'camel'"),
        ("", "'case' must be one of ['lower', 'sentence', 'title', 'upper'], got ''"),
        ("Upper", "'case' must be one of ['lower', 'sentence', 'title', 'upper'], got 'Upper'"),
        (None, "'case' must be a string, got <class 'NoneType'> instead."),
        (1, "'case' must be a string, got <class 'int'> instead."),
    ],
)
def test_has_valid_string_case_rejects_invalid_case(case: object, expected_message: str):
    with pytest.raises(InvalidParameterError) as error:
        has_valid_string_case("a", cast(str, case))

    assert str(error.value) == expected_message


@pytest.mark.parametrize(
    "value, expected_valid",
    [
        ("Notes From IEEE Meeting", True),
        ("An Ordinary Title", True),
        ("Notes from IEEE Meeting", False),
        ("", True),
        ("   ", True),
        ("123!?", True),
        ("hello-world", False),
    ],
)
def test_has_valid_string_case_title_expression(value: str, expected_valid: bool):
    normalized = " ".join(f"{word[:1].upper()}{word[1:]}" for word in value.split(" "))

    assert (value == normalized) is expected_valid

    expression = str(has_valid_string_case(F.lit(value), "title"))
    assert "initcap" not in expression
    assert "transform(split(" in expression
    assert "lower(" not in expression


def test_incorrect_aggr_type():
    # With new implementation, invalid aggr_type triggers a warning (not immediate error)
    # The error occurs at runtime when the apply function is called
    with pytest.warns(UserWarning, match="non-curated.*invalid"):
        condition, apply_fn = is_aggr_not_greater_than("a", 1, aggr_type="invalid")

    # Function should return successfully (error will happen at runtime when applied to DataFrame)
    assert condition is not None
    assert apply_fn is not None


def test_col_is_ipv4_address_in_cidr_missing_cidr_block():
    with pytest.raises(MissingParameterError, match="'cidr_block' is not provided."):
        is_ipv4_address_in_cidr("a", cidr_block=None)


def test_col_is_ipv4_address_in_cidr_empty_cidr_block():
    with pytest.raises(InvalidParameterError, match="'cidr_block' must be a non-empty string."):
        is_ipv4_address_in_cidr("a", cidr_block="")


def test_col_is_ipv4_address_in_cidr_invalid_cidr_block():
    with pytest.raises(InvalidParameterError, match="CIDR block 'invalid' is not a valid IPv4 CIDR block."):
        is_ipv4_address_in_cidr("a", cidr_block="invalid")


def test_col_is_ipv6_address_in_cidr_missing_cidr_block():
    with pytest.raises(MissingParameterError, match="'cidr_block' is not provided."):
        is_ipv6_address_in_cidr("a", cidr_block=None)


def test_col_is_ipv6_address_in_cidr_empty_cidr_block():
    with pytest.raises(InvalidParameterError, match="'cidr_block' must be a non-empty string."):
        is_ipv6_address_in_cidr("a", cidr_block="")


def test_col_is_ipv6_address_in_cidr_invalid_cidr_block():
    with pytest.raises(InvalidParameterError, match="CIDR block 'invalid' is not a valid IPv6 CIDR block."):
        is_ipv6_address_in_cidr("a", cidr_block="invalid")


def test_col_does_not_contain_pii_invalid_engine_config():
    nlp_engine_config = "'model': 'my_model'"
    with pytest.raises(
        InvalidParameterError, match=f"Invalid type provided for 'nlp_engine_config': {type(nlp_engine_config)}"
    ):
        does_not_contain_pii("a", nlp_engine_config=nlp_engine_config)


def test_col_does_not_contain_pii_missing_nlp_engine_name_in_config():
    nlp_engine_config = {
        "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
    }
    with pytest.raises(MissingParameterError, match="Missing 'nlp_engine_name' key in nlp_engine_config"):
        does_not_contain_pii("a", nlp_engine_config=nlp_engine_config)


@pytest.mark.parametrize("threshold", [-10.0, -0.1, 1.1, 10.0])
def test_col_does_not_contain_pii_invalid_threshold(threshold: float):
    with pytest.raises(InvalidParameterError, match=f"Provided threshold {threshold} must be between 0.0 and 1.0"):
        does_not_contain_pii("a", threshold=threshold)


@pytest.mark.parametrize("cidr_block", ['192.1', 'test', '::1/xyz', '1234:5678:9abc:def0:1234:5678:9abc:defg/300'])
def test_col_is_ipv6_address_in_cidr_invalid_cidr(cidr_block: str):
    with pytest.raises(InvalidParameterError, match=f"CIDR block '{cidr_block}' is not a valid IPv6 CIDR block."):
        is_ipv6_address_in_cidr("a", cidr_block=cidr_block)


def test_is_equal_to_missing_value():
    with pytest.raises(MissingParameterError, match=LIMIT_VALUE_ERROR):
        is_equal_to("a", value=None)


def test_is_not_equal_to_missing_value():
    with pytest.raises(MissingParameterError, match=LIMIT_VALUE_ERROR):
        is_not_equal_to("a", value=None)


def test_sql_expression_complex_exists_auto_name():
    expression = "EXISTS (SELECT 1 FROM cfg WHERE cfg.val = STATUS)"
    result = sql_expression(expression)
    assert get_column_name_or_alias(result) == "not_exists_select_1_from_cfg_where_cfg_val_status"


def test_sql_expression_complex_exists_negate_auto_name():
    expression = "EXISTS (SELECT 1 FROM cfg WHERE cfg.val = STATUS)"
    result = sql_expression(expression, negate=True)
    assert get_column_name_or_alias(result) == "exists_select_1_from_cfg_where_cfg_val_status"
