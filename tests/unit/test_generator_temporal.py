import datetime

from databricks.labs.dqx.profiler.generator import DQGenerator


def test_date_both_bounds_is_in_range():
    result = DQGenerator.dq_generate_min_max(
        "dcol", **{"min": datetime.date(2020, 1, 1), "max": datetime.date(2020, 12, 31)}
    )
    assert result["check"]["function"] == "is_in_range"
    args = result["check"]["arguments"]
    assert args["column"] == "dcol"
    assert args["min_limit"] == datetime.date(2020, 1, 1)
    assert args["max_limit"] == datetime.date(2020, 12, 31)


def test_timestamp_only_min_is_not_less_than():
    timestamp = datetime.datetime(2024, 6, 1, 12, 0, 0)
    result = DQGenerator.dq_generate_min_max("tscol", **{"min": timestamp, "max": None})
    assert result["check"]["function"] == "is_not_less_than"
    args = result["check"]["arguments"]
    assert args["column"] == "tscol"
    assert args["limit"] == timestamp


def test_timestamp_only_max_is_not_greater_than():
    timestamp = datetime.datetime(2024, 6, 30, 23, 59, 59)
    result = DQGenerator.dq_generate_min_max("tscol", **{"min": None, "max": timestamp})
    assert result["check"]["function"] == "is_not_greater_than"
    args = result["check"]["arguments"]
    assert args["column"] == "tscol"
    assert args["limit"] == timestamp
