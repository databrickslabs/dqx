from typing import cast
import pytest
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
    is_valid_national_id,
    is_valid_country_code,
    is_valid_currency_code,
    is_valid_subdivision_code,
    is_valid_language_code,
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


def test_col_is_ipv4_address_in_cidr_trailing_newline_cidr_block():
    # Regression for issue #1440: an otherwise-valid CIDR with a trailing newline must be rejected.
    # Validated by re.match (the \Z anchor produced by _pattern_for_python_re), so this raises before
    # any Spark call - keep it at the fast unit layer.
    with pytest.raises(InvalidParameterError, match="is not a valid IPv4 CIDR block"):
        is_ipv4_address_in_cidr("a", cidr_block="192.168.1.0/24\n")


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


def test_is_valid_national_id_default_country_auto_name():
    result = is_valid_national_id("a")
    assert get_column_name_or_alias(result) == "a_does_not_match_pattern_ssn_us"


def test_is_valid_national_id_explicit_us_auto_name():
    result = is_valid_national_id("a", country="US")
    assert get_column_name_or_alias(result) == "a_does_not_match_pattern_ssn_us"


def test_is_valid_national_id_country_is_case_insensitive():
    result = is_valid_national_id("a", country="us")
    assert get_column_name_or_alias(result) == "a_does_not_match_pattern_ssn_us"


def test_is_valid_national_id_missing_country():
    with pytest.raises(MissingParameterError, match="'country' is not provided."):
        is_valid_national_id("a", country=None)


def test_is_valid_national_id_non_string_country():
    with pytest.raises(InvalidParameterError, match="'country' must be a string"):
        is_valid_national_id("a", country=123)


def test_is_valid_national_id_unsupported_country():
    with pytest.raises(InvalidParameterError, match="Unsupported country code for national ID validation"):
        is_valid_national_id("a", country="ZZ")


def test_is_valid_country_code_default_format_auto_name():
    result = is_valid_country_code("a")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_country_code"


def test_is_valid_country_code_alpha_3_format_auto_name():
    result = is_valid_country_code("a", code_format="alpha-3")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_country_code"


def test_is_valid_country_code_numeric_format_auto_name():
    result = is_valid_country_code("a", code_format="numeric")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_country_code"


def test_is_valid_country_code_format_is_case_insensitive():
    result = is_valid_country_code("a", code_format="Alpha-2")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_country_code"


def test_is_valid_country_code_missing_code_format():
    with pytest.raises(MissingParameterError, match="'code_format' is not provided."):
        is_valid_country_code("a", code_format=None)


def test_is_valid_country_code_non_string_code_format():
    with pytest.raises(InvalidParameterError, match="'code_format' must be a string"):
        is_valid_country_code("a", code_format=123)


def test_is_valid_country_code_unsupported_code_format():
    with pytest.raises(InvalidParameterError, match="Unsupported code_format for country code validation"):
        is_valid_country_code("a", code_format="alpha-4")


def test_is_valid_country_code_case_insensitive_auto_name():
    result = is_valid_country_code("a", case_sensitive=False)
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_country_code"


def test_is_valid_currency_code_default_format_auto_name():
    result = is_valid_currency_code("a")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_currency_code"


def test_is_valid_currency_code_numeric_format_auto_name():
    result = is_valid_currency_code("a", code_format="numeric")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_currency_code"


def test_is_valid_currency_code_format_is_case_insensitive():
    result = is_valid_currency_code("a", code_format="Alphabetic")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_currency_code"


def test_is_valid_currency_code_missing_code_format():
    with pytest.raises(MissingParameterError, match="'code_format' is not provided."):
        is_valid_currency_code("a", code_format=None)


def test_is_valid_currency_code_non_string_code_format():
    with pytest.raises(InvalidParameterError, match="'code_format' must be a string"):
        is_valid_currency_code("a", code_format=123)


def test_is_valid_currency_code_unsupported_code_format():
    with pytest.raises(InvalidParameterError, match="Unsupported code_format for currency code validation"):
        is_valid_currency_code("a", code_format="alpha-3")


def test_is_valid_currency_code_case_insensitive_auto_name():
    result = is_valid_currency_code("a", case_sensitive=False)
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_currency_code"


def test_is_valid_subdivision_code_default_auto_name():
    result = is_valid_subdivision_code("a")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_subdivision_code"


def test_is_valid_subdivision_code_case_insensitive_auto_name():
    result = is_valid_subdivision_code("a", case_sensitive=False)
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_subdivision_code"


def test_is_valid_subdivision_code_with_country_column_auto_name():
    result = is_valid_subdivision_code("a", country_column="b")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_subdivision_code"


def test_is_valid_language_code_default_format_auto_name():
    result = is_valid_language_code("a")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_language_code"


def test_is_valid_language_code_alpha_3_format_auto_name():
    result = is_valid_language_code("a", code_format="alpha-3")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_language_code"


def test_is_valid_language_code_format_is_case_insensitive():
    result = is_valid_language_code("a", code_format="Alpha-3")
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_language_code"


def test_is_valid_language_code_missing_code_format():
    with pytest.raises(MissingParameterError, match="'code_format' is not provided."):
        is_valid_language_code("a", code_format=None)


def test_is_valid_language_code_non_string_code_format():
    with pytest.raises(InvalidParameterError, match="'code_format' must be a string"):
        is_valid_language_code("a", code_format=123)


def test_is_valid_language_code_unsupported_code_format():
    with pytest.raises(InvalidParameterError, match="Unsupported code_format for language code validation"):
        is_valid_language_code("a", code_format="numeric")


def test_is_valid_language_code_case_insensitive_auto_name():
    result = is_valid_language_code("a", case_sensitive=False)
    assert get_column_name_or_alias(result) == "a_is_not_a_valid_language_code"
