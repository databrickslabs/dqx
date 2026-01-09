import datetime

from databricks.labs.dqx.profiler.generator import DQGenerator


def test_date_both_bounds_is_in_range(set_utc_timezone):
    result = DQGenerator.dq_generate_min_max(
        "dcol", **{"min": datetime.date(2020, 1, 1), "max": datetime.date(2020, 12, 31)}
    )
    assert result["check"]["function"] == "is_in_range"
    args = result["check"]["arguments"]
    assert args["column"] == "dcol"
    assert args["min_limit"] == "2020-01-01"
    assert args["max_limit"] == "2020-12-31"


def test_timestamp_only_min_is_not_less_than(set_utc_timezone):
    timestamp = datetime.datetime(2024, 6, 1, 12, 0, 0)
    result = DQGenerator.dq_generate_min_max("tscol", **{"min": timestamp, "max": None})
    assert result["check"]["function"] == "is_not_less_than"
    args = result["check"]["arguments"]
    assert args["column"] == "tscol"
    assert args["limit"] == "2024-06-01T12:00:00.000000"


def test_timestamp_only_max_is_not_greater_than(set_utc_timezone):
    timestamp = datetime.datetime(2024, 6, 30, 23, 59, 59)
    result = DQGenerator.dq_generate_min_max("tscol", **{"min": None, "max": timestamp})
    assert result["check"]["function"] == "is_not_greater_than"
    args = result["check"]["arguments"]
    assert args["column"] == "tscol"
    assert args["limit"] == "2024-06-30T23:59:59.000000"
