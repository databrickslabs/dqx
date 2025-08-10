import pytest
from databricks.labs.dqx.check_funcs import (
    is_in_range,
    is_not_in_range,
    is_not_greater_than,
    is_not_less_than,
    is_in_list,
    is_not_null_and_is_in_list,
    is_aggr_not_greater_than,
    is_ipv4_address_in_cidr,
)
from databricks.labs.dqx.pii.pii_detection_funcs import contains_pii

LIMIT_VALUE_ERROR = "Limit is not provided"


@pytest.mark.parametrize("min_limit, max_limit", [(None, 1), (1, None)])
def test_col_is_in_range_missing_limits(min_limit, max_limit):
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_in_range("a", min_limit, max_limit)


@pytest.mark.parametrize("min_limit, max_limit", [(None, 1), (1, None)])
def test_col_is_not_in_range_missing_limits(min_limit, max_limit):
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_not_in_range("a", min_limit, max_limit)


def test_col_not_greater_than_missing_limit():
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_not_greater_than("a", limit=None)


def test_col_not_less_than_missing_limit():
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_not_less_than("a", limit=None)


def test_col_is_not_null_and_is_in_list_missing_allowed_list():
    with pytest.raises(ValueError, match="allowed list is not provided"):
        is_not_null_and_is_in_list("a", allowed=[])


def test_col_is_in_list_missing_allowed_list():
    with pytest.raises(ValueError, match="allowed list is not provided"):
        is_in_list("a", allowed=[])


def test_incorrect_aggr_type():
    with pytest.raises(ValueError, match="Unsupported aggregation type"):
        is_aggr_not_greater_than("a", 1, aggr_type="invalid")


def test_col_is_ipv4_address_in_cidr_missing_cidr_block():
    with pytest.raises(ValueError, match="'cidr_block' must be a non-empty string"):
        is_ipv4_address_in_cidr("a", cidr_block=None)


def test_col_is_ipv4_address_in_cidr_empty_cidr_block():
    with pytest.raises(ValueError, match="'cidr_block' must be a non-empty string"):
        is_ipv4_address_in_cidr("a", cidr_block="")


def test_col_contains_pii_invalid_environment():
    """
    We restrict running `contains_pii` using Databricks Connect due to limitations on
    the size of UDF dependencies. Because tests are run from a Databricks Connect session,
    we can only validate that the correct error is raised.

    Complete testing of `contains_pii` and its options has been added to e2e tests.
    We run many scenarios in `test_pii_detection_checks` to validate `contains_pii` from
    a Databricks workspace.
    """
    with pytest.raises(
        ImportError, match="'contains_pii' is not supported when running checks with Databricks Connect"
    ):
        contains_pii("a")


def test_col_contains_pii_invalid_engine_config():
    nlp_engine_config = "'model': 'my_model'"
    with pytest.raises(ValueError, match=f"Invalid type provided for 'nlp_engine_config': {type(nlp_engine_config)}"):
        contains_pii("a", nlp_engine_config=nlp_engine_config)


@pytest.mark.parametrize("threshold", [-10.0, -0.1, 1.1, 10.0])
def test_col_contains_pii_invalid_threshold(threshold: float):
    with pytest.raises(ValueError, match=f"Provided threshold {threshold} must be between 0.0 and 1.0"):
        contains_pii("a", threshold=threshold)
