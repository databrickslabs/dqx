"""Row Scope planning: which column a time window uses, and how it reads it.

The column types mirror real tables: true ``TIMESTAMP`` columns (compared as
instants, never shifted by a time zone), ``TIMESTAMP_NTZ``/string date-times
(read in the schedule's zone), and ``DATE`` columns (compared by day, so a
sub-day window doesn't silently match nothing). Applying the window needs a
Spark session, so these tests cover the planning, which decides all of that.
"""

import importlib
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampNTZType, TimestampType

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


@pytest.mark.parametrize("name", ["UTC", "America/New_York", "America/Los_Angeles", "Asia/Kolkata", "Etc/GMT+5"])
def test_region_names_are_accepted(row_scope: ModuleType, name: str) -> None:
    assert row_scope.validate_timezone(name) == name


@pytest.mark.parametrize("name", ["EST", "PST", "America/New_york", "Not/AZone", "", "America/New_York\n"])
def test_abbreviations_typos_and_junk_are_rejected(row_scope: ModuleType, name: str) -> None:
    with pytest.raises(ValueError):
        row_scope.validate_timezone(name)
