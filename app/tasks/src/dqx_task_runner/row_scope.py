"""Row Scope: restrict a run to the rows of a recent time window.

A schedule (or a "Run Now") can ask for "the last N minutes" of a table. That
needs a column saying when each row happened, and how to read it:

* ``instant`` — a ``TIMESTAMP`` column already holds a true instant, so it is
  compared with the cutoff as-is. The configured time zone never applies to it
  (applying one would shift every value by the zone's offset).
* ``wall_clock`` — a ``TIMESTAMP_NTZ`` column, or a string holding date-times,
  records a local clock reading with no zone. It is read in the configured
  time zone (UTC when none is set) and converted to an instant. Many such
  columns only hold days (every value at midnight, e.g. an ``ingestdate``
  string); a minute cutoff would exclude whole days, so a value at exactly
  midnight also counts when its day is one the window touches. Deciding this
  per row, not by sampling the column, stays right when a column's format
  changed over the years.
* ``day`` — a ``DATE`` column only knows the day. A minute-level cutoff would
  exclude whole days (a 24-hour window at 10:00 starts at 10:00 yesterday,
  after yesterday's midnight), so it is compared by day instead: every day the
  window touches, in the configured time zone, is included.

The task runner pins its Spark session to UTC (see :func:`pin_session_to_utc`)
so these conversions don't depend on a workspace's session time-zone setting.
"""

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from zoneinfo import available_timezones

logger = logging.getLogger("dqx_task_runner.row_scope")

# Preferred column names for "when this row happened", checked in this order.
# A real TIMESTAMP match is preferred over a string match anywhere in the list
# (see detect_time_column).
TIME_COLUMN_NAME_PRIORITY: tuple[str, ...] = (
    "updated_at",
    "update_ts",
    "updatetime",
    "update_time",
    "last_updated",
    "last_modified",
    "modified_at",
    "ingestdate",
    "event_time",
    "eventtime",
    "event_timestamp",
    "ingestion_time",
    "ingested_at",
    "ingest_ts",
    "load_time",
    "loaded_at",
    "processed_timestamp",
    "processed_at",
    "record_timestamp",
    "timestamp",
    "created_at",
    "create_ts",
    "createtime",
    "create_time",
    "insert_time",
    "inserted_at",
    "ts",
    "date",
)
_ZONE_NAME = re.compile(r"\A[A-Za-z0-9_+\-]+(/[A-Za-z0-9_+\-]+)+\Z")


def validate_timezone(name: str) -> str:
    """Return *name* when it is an IANA region name (``Area/City``) or ``UTC``.

    Abbreviations such as ``EST`` are rejected even though the tz database
    knows them: they are fixed offsets, so ``EST`` silently ignores daylight
    saving time. Raises ValueError otherwise.
    """
    if name == "UTC" or (_ZONE_NAME.match(name) and name in available_timezones()):
        return name
    raise ValueError(
        f"Invalid time zone {name!r}: use a region name such as 'America/New_York' or 'UTC' "
        "(abbreviations like 'EST' ignore daylight saving time)."
    )


def pin_session_to_utc(spark: Any) -> None:
    """Make timestamp casts and date extraction mean the same on every workspace.

    Casting a ``TIMESTAMP_NTZ``/string to ``TIMESTAMP`` and taking the date of a
    ``TIMESTAMP`` both use the session time zone. Serverless defaults to UTC,
    but a workspace can override it; pinning keeps Row Scope windows exact.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")


@dataclass(frozen=True)
class TimeWindow:
    """How a run's rows are restricted to the last *minutes*."""

    column: str
    kind: str  # "instant" | "wall_clock" | "day"
    source_type: str  # "timestamp" | "timestamp_ntz" | "date" | "string"
    minutes: int
    timezone: str


def detect_time_column(
    schema: Any,
    name_priority: Sequence[str] = TIME_COLUMN_NAME_PRIORITY,
    pinned: str | None = None,
) -> tuple[str, str] | None:
    """Pick the column that says when a row happened: ``(name, source_type)``.

    A *pinned* column (a schedule's explicit choice) wins when it exists; when
    it doesn't, nothing is guessed — None, so the caller skips the window
    rather than windowing on a different column. Otherwise *name_priority* is
    walked twice: first for a real date/time-typed column, then for a string
    one, so a ``TIMESTAMP`` named ``updatetime`` beats a string ``ingestdate``.
    With no name match, the first date/time-typed column is used. Returns None
    when nothing fits.
    """
    from pyspark.sql.types import DateType, StringType, TimestampNTZType, TimestampType

    types = {TimestampType: "timestamp", TimestampNTZType: "timestamp_ntz", DateType: "date"}
    typed = {}
    strings = {}
    for field in schema.fields:
        kind = next((v for t, v in types.items() if isinstance(field.dataType, t)), None)
        if kind:
            typed[field.name.lower()] = (field.name, kind)
        elif isinstance(field.dataType, StringType):
            strings[field.name.lower()] = (field.name, "string")

    if pinned:
        found = typed.get(pinned.lower()) or strings.get(pinned.lower())
        if found is None:
            logger.warning("Pinned time column %r not found on table — skipping the time window", pinned)
        return found

    for candidates in (typed, strings):
        for name in name_priority:
            if name.lower() in candidates:
                return candidates[name.lower()]
    return next(iter(typed.values()), None)


def plan_time_window(
    df: Any,
    minutes: int | None,
    timezone: str | None = None,
    name_priority: Sequence[str] | None = None,
    pinned: str | None = None,
) -> TimeWindow | None:
    """Decide how to apply a *minutes* lookback to *df*, or None for no window.

    No window when *minutes* isn't a positive number, or no time column fits
    (logged). Raises ValueError for an invalid *timezone*.
    """
    if not minutes or minutes <= 0:
        return None
    zone = validate_timezone(timezone) if timezone else "UTC"
    detected = detect_time_column(
        df.schema,
        name_priority=TIME_COLUMN_NAME_PRIORITY if name_priority is None else name_priority,
        pinned=pinned,
    )
    if detected is None:
        logger.warning("Row Scope: no date/time column found — taking an unordered sample instead of a window")
        return None
    column, source_type = detected
    kind = {"timestamp": "instant", "date": "day"}.get(source_type, "wall_clock")
    logger.info("Row Scope: last %d minutes by %s column '%s' (%s, zone %s)", minutes, source_type, column, kind, zone)
    return TimeWindow(column=column, kind=kind, source_type=source_type, minutes=int(minutes), timezone=zone)


def _hashable_columns(df: Any) -> list[str]:
    """Columns a row hash can include (maps and variants can't be hashed)."""
    return [f.name for f in df.schema.fields if f.dataType.typeName() not in ("map", "variant")]


def apply_time_window(df: Any, window: TimeWindow, sample_size: int = 0) -> Any:
    """Keep the rows of *df* inside *window*; with *sample_size*, the latest that many.

    "Latest N" ties (rows sharing a timestamp, or a whole day) are broken by a
    hash of the row so repeated runs pick the same rows.
    """
    from pyspark.sql import functions as F

    cutoff = F.current_timestamp() - F.expr(f"INTERVAL {int(window.minutes)} MINUTES")
    first_day = F.to_date(F.from_utc_timestamp(cutoff, window.timezone))
    value = F.col(window.column)
    if window.kind == "day":
        order_by = value
        df = df.where(value >= first_day)
    elif window.kind == "wall_clock":
        local = value.cast("timestamp")
        order_by = local if window.timezone == "UTC" else F.to_utc_timestamp(local, window.timezone)
        is_day = (local == F.date_trunc("day", local)) & (F.to_date(local) >= first_day)
        df = df.where((order_by >= cutoff) | is_day)
    else:
        order_by = value
        df = df.where(value >= cutoff)
    if not sample_size:
        return df
    hashable = _hashable_columns(df)
    tiebreak = [F.xxhash64(*[F.col(c) for c in hashable]).asc()] if hashable else []
    return df.orderBy(order_by.desc(), *tiebreak).limit(sample_size)
