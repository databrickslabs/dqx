"""Row Scope planning: which column a time window uses, and how it reads it.

The column types mirror real tables: true ``TIMESTAMP`` columns (compared as
instants, never shifted by a time zone), ``TIMESTAMP_NTZ``/string date-times
(read in the schedule's zone), and ``DATE`` columns (compared by day, so a
sub-day window doesn't silently match nothing). Applying the window needs a
Spark session, so these tests cover the planning, which decides all of that.
"""

import importlib
from collections.abc import Callable
from datetime import date, datetime, timezone
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from databricks_labs_dqx_app.backend import sql_utils

_TASKS_SRC = Path(__file__).resolve().parent.parent / "tasks" / "src"
if str(_TASKS_SRC) not in sys.path:
    sys.path.insert(0, str(_TASKS_SRC))


@pytest.fixture(scope="module")
def row_scope() -> ModuleType:
    return importlib.import_module("dqx_task_runner.row_scope")


def _df(*fields: StructField) -> MagicMock:
    df = MagicMock()
    df.schema = StructType(list(fields))
    return df


# ---------------------------------------------------------------------------
# detect_time_column
# ---------------------------------------------------------------------------


def test_a_real_timestamp_beats_a_string_earlier_in_the_name_list(row_scope: ModuleType) -> None:
    # Prod delivr tables: STRING ingestdate (day values) next to TIMESTAMP updatetime.
    schema = StructType([StructField("ingestdate", StringType()), StructField("updatetime", TimestampType())])

    assert row_scope.detect_time_column(schema) == ("updatetime", "timestamp")


def test_a_string_is_used_when_no_typed_column_matches(row_scope: ModuleType) -> None:
    schema = StructType([StructField("ingestdate", StringType()), StructField("name", StringType())])

    assert row_scope.detect_time_column(schema) == ("ingestdate", "string")


def test_pinned_column_wins_and_reports_its_type(row_scope: ModuleType) -> None:
    schema = StructType([StructField("updatetime", TimestampType()), StructField("ingestdate", DateType())])

    assert row_scope.detect_time_column(schema, pinned="INGESTDATE") == ("ingestdate", "date")


def test_missing_pinned_column_does_not_guess(row_scope: ModuleType) -> None:
    schema = StructType([StructField("updatetime", TimestampType())])

    assert row_scope.detect_time_column(schema, pinned="eventtime") is None


def test_empty_candidate_list_uses_any_typed_column(row_scope: ModuleType) -> None:
    schema = StructType([StructField("processed_ts", TimestampType()), StructField("created_at", TimestampType())])

    assert row_scope.detect_time_column(schema, name_priority=[]) == ("processed_ts", "timestamp")


def test_no_time_column_at_all(row_scope: ModuleType) -> None:
    assert row_scope.detect_time_column(StructType([StructField("name", StringType())])) is None


# ---------------------------------------------------------------------------
# plan_time_window
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("minutes", [None, 0, -60])
def test_no_window_without_a_positive_lookback(row_scope: ModuleType, minutes: int | None) -> None:
    assert row_scope.plan_time_window(_df(StructField("updatetime", TimestampType())), minutes) is None


def test_timestamp_column_is_an_instant_and_ignores_the_zone(row_scope: ModuleType) -> None:
    window = row_scope.plan_time_window(_df(StructField("update_ts", TimestampType())), 1440, "America/New_York")

    assert (window.column, window.kind, window.timezone) == ("update_ts", "instant", "America/New_York")


def test_date_column_is_compared_by_day(row_scope: ModuleType) -> None:
    window = row_scope.plan_time_window(
        _df(StructField("ingestdate", DateType())), 1440, "America/New_York", pinned="ingestdate"
    )

    assert window.kind == "day"


def test_unset_zone_means_utc(row_scope: ModuleType) -> None:
    window = row_scope.plan_time_window(_df(StructField("updatetime", TimestampType())), 60)

    assert window.timezone == "UTC"


def test_invalid_zone_fails_the_plan(row_scope: ModuleType) -> None:
    with pytest.raises(ValueError, match="Invalid time zone"):
        row_scope.plan_time_window(_df(StructField("updatetime", TimestampType())), 60, "America/New_york")


def test_no_window_when_no_column_fits(row_scope: ModuleType) -> None:
    assert row_scope.plan_time_window(_df(StructField("name", StringType())), 60) is None


@pytest.mark.parametrize(
    "field", [StructField("dateTime", TimestampNTZType()), StructField("ingestdate", StringType())]
)
def test_zoneless_columns_are_wall_clock_decided_per_row(row_scope: ModuleType, field: StructField) -> None:
    # Their values may be whole days (midnight) — decided per row when the
    # window is applied, never by sampling the column.
    window = row_scope.plan_time_window(_df(field), 360, "America/Los_Angeles", pinned=field.name)

    assert window.kind == "wall_clock"
    df_never_sampled = _df(field)
    row_scope.plan_time_window(df_never_sampled, 360, pinned=field.name)
    df_never_sampled.select.assert_not_called()


# ---------------------------------------------------------------------------
# validate_timezone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "UTC",
        "America/New_York",
        "America/Los_Angeles",
        "Asia/Kolkata",
        "Etc/GMT+5",
        # Single-word IANA zones are real, DST-aware where the region is.
        "Singapore",
        "Japan",
        "Portugal",
        "GB",
        "Eire",
    ],
)
def test_iana_zone_names_are_accepted(row_scope: ModuleType, name: str) -> None:
    assert row_scope.validate_timezone(name) == name


@pytest.mark.parametrize(
    "name", ["EST", "MST", "HST", "PST", "Factory", "America/New_york", "Not/AZone", "", "America/New_York\n"]
)
def test_abbreviations_typos_and_junk_are_rejected(row_scope: ModuleType, name: str) -> None:
    with pytest.raises(ValueError):
        row_scope.validate_timezone(name)


def test_the_app_and_the_task_runner_accept_the_same_zones(row_scope: ModuleType) -> None:
    """The two wheels each keep a copy of the rule; they must never drift apart."""
    assert sql_utils.VALID_TIMEZONES == row_scope.VALID_TIMEZONES
    for name in ("Singapore", "EST", "America/New_York", "Not/AZone"):
        app_ok = _accepts(sql_utils.validate_timezone, name)
        assert app_ok == _accepts(row_scope.validate_timezone, name), name


def _accepts(validate: Callable[[str], str], name: str) -> bool:
    try:
        validate(name)
    except ValueError:
        return False
    return True


# ---------------------------------------------------------------------------
# sample_interval_columns and the "latest N" tiebreaker
# ---------------------------------------------------------------------------


def test_a_column_list_given_as_a_string_is_rejected(row_scope: ModuleType) -> None:
    # Iterating "updated_at" would try 'u', 'p', 'd', ... and silently fall back.
    with pytest.raises(ValueError, match="list of column names"):
        row_scope.plan_time_window(_df(StructField("updated_at", TimestampType())), 60, name_priority="updated_at")


def test_the_tiebreak_hash_skips_maps_at_any_depth(row_scope: ModuleType) -> None:
    df = _df(
        StructField("id", IntegerType()),
        StructField("tags", MapType(StringType(), StringType())),
        StructField("meta", StructType([StructField("attrs", MapType(StringType(), StringType()))])),
        StructField("events", ArrayType(MapType(StringType(), StringType()))),
        StructField("point", StructType([StructField("x", IntegerType())])),
        StructField("names", ArrayType(StringType())),
    )

    assert row_scope.hashable_columns(df) == ["id", "point", "names"]


# ---------------------------------------------------------------------------
# A string column is only a time column when its values read as date-times
# ---------------------------------------------------------------------------


def _probed_df(*fields: StructField, string_reads_as_time: bool) -> MagicMock:
    """A DataFrame whose string probe finds date-times, or doesn't."""
    df = _df(*fields)
    probe = MagicMock()
    probe.select.return_value = probe.where.return_value = probe.limit.return_value = probe
    probe.count.return_value = 1 if string_reads_as_time else 0
    df.select.return_value = probe
    return df


def test_candidates_are_named_typed_then_named_strings_then_other_typed(row_scope: ModuleType) -> None:
    schema = StructType(
        [
            StructField("recorded", TimestampType()),
            StructField("date", StringType()),
            StructField("updated_at", TimestampNTZType()),
        ]
    )

    assert row_scope.time_column_candidates(schema) == [
        ("updated_at", "timestamp_ntz"),
        ("date", "string"),
        ("recorded", "timestamp"),
    ]


def test_a_free_text_string_named_like_a_time_column_is_passed_over(row_scope: ModuleType) -> None:
    # 'date' holds values like 'Q3 FY26'; windowing on it would match no rows.
    df = _probed_df(
        StructField("date", StringType()), StructField("event_dt", TimestampType()), string_reads_as_time=False
    )

    window = row_scope.plan_time_window(df, 60)

    assert (window.column, window.kind) == ("event_dt", "instant")


def test_a_string_holding_date_times_is_still_used(row_scope: ModuleType) -> None:
    df = _probed_df(
        StructField("ingestdate", StringType()), StructField("event_dt", TimestampType()), string_reads_as_time=True
    )

    window = row_scope.plan_time_window(df, 60)

    assert (window.column, window.kind) == ("ingestdate", "wall_clock")


# ---------------------------------------------------------------------------
# window_bounds: one fixed cutoff per run
# ---------------------------------------------------------------------------


def _window(row_scope: ModuleType, minutes: int, zone: str) -> object:
    return row_scope.TimeWindow(column="c", kind="wall_clock", source_type="string", minutes=minutes, timezone=zone)


def test_the_cutoff_is_fixed_at_the_given_time_in_utc(row_scope: ModuleType) -> None:
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)

    bounds = row_scope.window_bounds(_window(row_scope, 60, "America/New_York"), now)

    assert bounds.cutoff_utc == datetime(2026, 10, 16, 13, 0)
    assert bounds.first_day == date(2026, 10, 16)


@pytest.mark.parametrize(
    ("zone", "first_day"),
    [("America/New_York", date(2026, 10, 16)), ("Asia/Tokyo", date(2026, 10, 17)), ("UTC", date(2026, 10, 16))],
)
def test_the_first_day_is_the_cutoffs_day_in_the_windows_zone(
    row_scope: ModuleType, zone: str, first_day: date
) -> None:
    # 02:00Z on the 17th minus 3 hours is 23:00Z on the 16th: 19:00 the 16th in
    # New York, 08:00 the 17th in Tokyo.
    now = datetime(2026, 10, 17, 2, 0, tzinfo=timezone.utc)

    assert row_scope.window_bounds(_window(row_scope, 180, zone), now).first_day == first_day


def test_a_day_window_reaches_back_to_yesterday(row_scope: ModuleType) -> None:
    # A 24-hour window at 10:00 in New York starts at 10:00 yesterday.
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)

    assert row_scope.window_bounds(_window(row_scope, 24 * 60, "America/New_York"), now).first_day == date(2026, 10, 15)
