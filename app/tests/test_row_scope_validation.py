"""Row Scope settings are checked when a schedule is saved or a run is requested.

A bad time zone would otherwise fail every scheduled run (Spark rejects it),
an abbreviation like ``EST`` would silently ignore daylight saving time, and
a negative lookback would put the cutoff in the future and validate nothing.
"""

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.models import BatchRunFromCatalogIn, ScheduleConfigIn


def _schedule(**row_scope: object) -> ScheduleConfigIn:
    return ScheduleConfigIn(schedule_name="nightly", config={"frequency": "daily", **row_scope})


@pytest.mark.parametrize(
    "row_scope",
    [
        {},
        {"sample_interval_minutes": 1440, "sample_interval_timezone": "America/New_York"},
        {"sample_interval_minutes": 0},
        {"sample_interval_minutes": None, "sample_interval_timezone": ""},
    ],
)
def test_valid_row_scope_is_saved(row_scope: dict) -> None:
    assert _schedule(**row_scope).config["frequency"] == "daily"


@pytest.mark.parametrize(
    "row_scope",
    [
        {"sample_interval_minutes": -60},
        {"sample_interval_minutes": "60"},
        {"sample_interval_minutes": True},
        {"sample_interval_minutes": 60, "sample_interval_timezone": "EST"},
        {"sample_interval_minutes": 60, "sample_interval_timezone": "America/New_york"},
    ],
)
def test_invalid_row_scope_is_rejected(row_scope: dict) -> None:
    with pytest.raises(ValidationError):
        _schedule(**row_scope)


def test_run_now_rejects_a_negative_lookback() -> None:
    with pytest.raises(ValidationError):
        BatchRunFromCatalogIn(table_fqns=["c.s.t"], sample_interval_minutes=-1)


def test_run_now_rejects_an_abbreviated_zone() -> None:
    with pytest.raises(ValidationError):
        BatchRunFromCatalogIn(table_fqns=["c.s.t"], sample_interval_minutes=60, sample_interval_timezone="EST")


def test_run_now_accepts_a_region_zone() -> None:
    body = BatchRunFromCatalogIn(
        table_fqns=["c.s.t"], sample_interval_minutes=60, sample_interval_timezone="America/Los_Angeles"
    )

    assert body.sample_interval_timezone == "America/Los_Angeles"
