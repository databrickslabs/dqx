import pytest
import pyspark.sql.functions as F
from databricks.labs.dqx.col_functions import is_in_range, is_not_in_range, not_greater_than, not_less_than

LIMIT_VALUE_ERROR = "Limit value or limit column expression is required."


@pytest.mark.parametrize("min_limit, max_limit", [(None, 1), (1, None)])
def test_col_range_missing_limits(min_limit, max_limit):
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_in_range("a", min_limit=min_limit, max_limit=max_limit)
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_not_in_range("a", min_limit=min_limit, max_limit=max_limit)


@pytest.mark.parametrize("min_limit_col_expr, max_limit_col_expr", [(None, F.expr("b")), (F.expr("a"), None)])
def test_col_range_missing_limit_col_exprs(min_limit_col_expr, max_limit_col_expr):
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_in_range("a", min_limit_col_expr=min_limit_col_expr, max_limit_col_expr=max_limit_col_expr)
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        is_not_in_range("a", min_limit_col_expr=min_limit_col_expr, max_limit_col_expr=max_limit_col_expr)


def test_col_not_greater_or_less_than_missing_limit_value():
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        not_greater_than("a", limit=None)
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        not_less_than("a", limit=None)


def test_col_not_greater_less_than_missing_limit_col_expr():
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        not_greater_than("a", limit_col_expr=None)
    with pytest.raises(ValueError, match=LIMIT_VALUE_ERROR):
        not_less_than("a", limit_col_expr=None)
