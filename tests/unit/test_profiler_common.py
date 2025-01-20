import datetime
from decimal import Decimal

from databricks.labs.dqx.profiler.common import val_maybe_to_str, val_to_str


def test_val_to_str():
    # Test with datetime
    date_time_val = datetime.datetime(2023, 10, 1, 12, 0, 0)
    assert val_to_str(date_time_val) == "'2023-10-01T12:00:00.000000'"
    assert val_to_str(date_time_val, include_sql_quotes=False) == "2023-10-01T12:00:00.000000"

    # Test with date
    date_val = datetime.date(2023, 10, 1)
    assert val_to_str(date_val) == "'2023-10-01'"
    assert val_to_str(date_val, include_sql_quotes=False) == "2023-10-01"

    # Test with int
    assert val_to_str(123) == "123"

    # Test with float
    assert val_to_str(123.45) == "123.45"

    # Test with Decimal
    assert val_to_str(Decimal("123.45")) == "123.45"

    # Test with string
    assert val_to_str("test") == "'test'"
    assert val_to_str("test", include_sql_quotes=False) == "test"

    # Test with string containing special characters
    assert val_to_str("test'string") == "'test\\'string'"
    assert val_to_str("test\\string") == "'test\\\\string'"

    # Test with None
    assert val_to_str(None) == "'None'"
    assert val_to_str(None, include_sql_quotes=False) == "None"


def test_val_maybe_to_str():
    # Test with datetime
    date_time_val = datetime.datetime(2023, 10, 1, 12, 0, 0)
    assert val_maybe_to_str(date_time_val) == "'2023-10-01T12:00:00.000000'"
    assert val_maybe_to_str(date_time_val, include_sql_quotes=False) == "2023-10-01T12:00:00.000000"

    # Test with date
    date_val = datetime.date(2023, 10, 1)
    assert val_maybe_to_str(date_val) == "'2023-10-01'"
    assert val_maybe_to_str(date_val, include_sql_quotes=False) == "2023-10-01"

    # Test with int
    assert val_maybe_to_str(123) == 123

    # Test with float
    assert val_maybe_to_str(123.45) == 123.45

    # Test with string
    assert val_maybe_to_str("test") == "test"

    # Test with None
    assert val_maybe_to_str(None) is None
