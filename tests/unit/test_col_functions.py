import pytest
from databricks.labs.dqx.col_functions import (
    is_in_range,
    is_not_in_range,
    is_not_greater_than,
    is_not_less_than,
    is_in_list,
    is_not_null_and_is_in_list,
)
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
