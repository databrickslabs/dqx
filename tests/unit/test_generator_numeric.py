from decimal import Decimal

from databricks.labs.dqx.profiler.generator import DQGenerator


def test_decimal_both_bounds_is_in_range():
    """Test min_max generator with Decimal type for both bounds."""
    result = DQGenerator.dq_generate_min_max("price_col", **{"min": Decimal("0.01"), "max": Decimal("999.99")})
    assert result["check"]["function"] == "is_in_range"
    args = result["check"]["arguments"]
    assert args["column"] == "price_col"
    assert args["min_limit"] == Decimal("0.01")
    assert args["max_limit"] == Decimal("999.99")


def test_decimal_only_min_is_not_less_than():
    """Test min_max generator with Decimal type for minimum bound only."""
    result = DQGenerator.dq_generate_min_max("amount_col", **{"min": Decimal("10.50"), "max": None})
    assert result["check"]["function"] == "is_not_less_than"
    args = result["check"]["arguments"]
    assert args["column"] == "amount_col"
    assert args["limit"] == Decimal("10.50")


def test_decimal_only_max_is_not_greater_than():
    """Test min_max generator with Decimal type for maximum bound only."""
    result = DQGenerator.dq_generate_min_max("total_col", **{"min": None, "max": Decimal("1000.00")})
    assert result["check"]["function"] == "is_not_greater_than"
    args = result["check"]["arguments"]
    assert args["column"] == "total_col"
    assert args["limit"] == Decimal("1000.00")


def test_int_both_bounds_is_in_range():
    """Test min_max generator with int type for both bounds."""
    result = DQGenerator.dq_generate_min_max("age_col", **{"min": 0, "max": 120})
    assert result["check"]["function"] == "is_in_range"
    args = result["check"]["arguments"]
    assert args["column"] == "age_col"
    assert args["min_limit"] == 0
    assert args["max_limit"] == 120


def test_float_both_bounds_is_in_range():
    """Test min_max generator with float type for both bounds."""
    result = DQGenerator.dq_generate_min_max("temperature_col", **{"min": -273.15, "max": 1000.0})
    assert result["check"]["function"] == "is_in_range"
    args = result["check"]["arguments"]
    assert args["column"] == "temperature_col"
    assert args["min_limit"] == -273.15
    assert args["max_limit"] == 1000.0


def test_mixed_int_and_decimal_is_in_range():
    """Test that mixing int and Decimal produces is_in_range since both are numeric."""
    result = DQGenerator.dq_generate_min_max("mixed_col", **{"min": 10, "max": Decimal("100.00")})
    assert result is not None
    assert result["check"]["function"] == "is_in_range"
    args = result["check"]["arguments"]
    assert args["column"] == "mixed_col"
    assert args["min_limit"] == 10
    assert args["max_limit"] == Decimal("100.00")
