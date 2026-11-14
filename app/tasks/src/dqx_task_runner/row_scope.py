"""Row Scope: restrict a run to the rows of a recent time window.

A schedule (or a "Run Now") can ask for "the last N minutes" of a table. That
needs a column saying when each row happened, and how to read it:

* ``instant`` — a ``TIMESTAMP`` column already holds a true instant, so it is
  compared with the cutoff as-is. The configured time zone never applies to it
  (applying one would shift every value by the zone's offset).
* ``wall_clock`` — a ``TIMESTAMP_NTZ`` column, or a string holding date-times,
  records a local clock reading with no zone. It is read in the configured
  time zone (UTC when none is set) and converted to an instant. A string that
  carries its own zone (``...Z`` or ``...+02:00``) is that instant already, so
  the configured zone doesn't apply to it. Many such columns only hold days
  (every value at midnight, e.g. an ``ingestdate`` string); a minute cutoff
  would exclude whole days, so a value at exactly midnight also counts when
  its day is one the window touches. Deciding this per row, not by sampling
  the column, stays right when a column's format changed over the years.
* ``day`` — a ``DATE`` column only knows the day. A minute-level cutoff would
  exclude whole days (a 24-hour window at 10:00 starts at 10:00 yesterday,
  after yesterday's midnight), so it is compared by day instead: every day the
  window touches, in the configured time zone, is included.

The cutoff is computed once, when the window is applied, and enters the plan
as a fixed value, so every action of a run (counts, the invalid-row sample,
quarantine writes) sees the same window. Every value is converted to UTC
explicitly, so the window doesn't depend on, or change, the Spark session's
time zone: the run's own checks keep the workspace's setting.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, available_timezones

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DataType,
    DateType,
    MapType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
    VariantType,
)

logger = logging.getLogger("dqx_task_runner.row_scope")

# Preferred column names for "when this row happened", checked in this order.
# A real TIMESTAMP match is preferred over a string match anywhere in the list
# (see time_column_candidates).
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
# Built once: available_timezones() scans the whole tz database. The backend
# keeps an identical rule (sql_utils.validate_timezone); a test checks they agree.
_FIXED_OFFSET_ABBREVIATIONS = frozenset({"EST", "MST", "HST"})
_NOT_ZONES = frozenset({"Factory", "localtime", "posixrules"})
VALID_TIMEZONES: frozenset[str] = frozenset(available_timezones()) - _FIXED_OFFSET_ABBREVIATIONS - _NOT_ZONES

# A date-time string that ends in its own zone: ``Z`` or a ``+hh:mm`` / ``-hhmm`` offset.
_EXPLICIT_ZONE = r"(?i)\d{2}:\d{2}(:\d{2}(\.\d+)?)?\s*(z|utc|[+-]\d{2}(:?\d{2})?)$"
# Non-null values read to decide whether a string column holds date-times.
_PROBE_ROWS = 20

_TYPED_KINDS: tuple[tuple[type[DataType], str], ...] = (
    (TimestampType, "timestamp"),
    (TimestampNTZType, "timestamp_ntz"),
    (DateType, "date"),
)


def validate_timezone(name: str) -> str:
    """Return *name* when it is a usable time zone; raise ValueError otherwise.

    Any IANA zone name is accepted (``America/New_York``, ``Singapore``, ``UTC``)
    except the fixed-offset abbreviations ``EST``, ``MST`` and ``HST``: they
    silently ignore daylight saving time where the region observes it.
    """
    if name in VALID_TIMEZONES:
        return name
    raise ValueError(
        f"Invalid time zone {name!r}: use a region name such as 'America/New_York' or 'UTC' "
        "(abbreviations like 'EST' ignore daylight saving time)."
    )


@dataclass(frozen=True)
class TimeWindow:
    """How a run's rows are restricted to the last *minutes*."""

    column: str
    kind: str  # "instant" | "wall_clock" | "day"
    source_type: str  # "timestamp" | "timestamp_ntz" | "date" | "string"
    minutes: int
    timezone: str


@dataclass(frozen=True)
class WindowBounds:
    """A window's fixed bounds for one run."""

    cutoff_utc: datetime  # naive, a UTC wall-clock reading
    first_day: date  # the first day the window touches, in the window's zone


def window_bounds(window: TimeWindow, now: datetime) -> WindowBounds:
    """The cutoff of *window* at *now* (an aware instant), and the first day it touches."""
    cutoff = now.astimezone(timezone.utc) - timedelta(minutes=window.minutes)
    return WindowBounds(
        cutoff_utc=cutoff.replace(tzinfo=None),
        first_day=cutoff.astimezone(ZoneInfo(window.timezone)).date(),
    )


def _source_type(data_type: DataType) -> str | None:
    if isinstance(data_type, StringType):
        return "string"
    return next((kind for cls, kind in _TYPED_KINDS if isinstance(data_type, cls)), None)


def time_column_candidates(
    schema: StructType,
    name_priority: Sequence[str] = TIME_COLUMN_NAME_PRIORITY,
    pinned: str | None = None,
) -> list[tuple[str, str]]:
    """Columns that may say when a row happened, best first: ``[(name, source_type), ...]``.

    A *pinned* column (a schedule's explicit choice) is the only candidate; when
    it doesn't exist there is none, so the caller skips the window rather than
    windowing on a different column. Otherwise: date/time-typed columns named in
    *name_priority* (in its order), then string columns named there, then any
    other date/time-typed column. A string candidate is only used when its
    values read as date-times (see :func:`plan_time_window`).
    """
    columns = {f.name.lower(): (f.name, kind) for f in schema.fields if (kind := _source_type(f.dataType))}
    if pinned:
        found = columns.get(pinned.lower())
        if found is None:
            logger.warning("Pinned time column %r not found on table — skipping the time window", pinned)
        return [found] if found else []
    named = [columns[n.lower()] for n in name_priority if n.lower() in columns]
    typed = [c for c in named if c[1] != "string"]
    strings = [c for c in named if c[1] == "string"]
    others = [c for c in columns.values() if c[1] != "string" and c not in typed]
    return typed + strings + others


def detect_time_column(
    schema: StructType,
    name_priority: Sequence[str] = TIME_COLUMN_NAME_PRIORITY,
    pinned: str | None = None,
) -> tuple[str, str] | None:
    """The best candidate from :func:`time_column_candidates` by schema alone, or None."""
    return next(iter(time_column_candidates(schema, name_priority, pinned)), None)


def _reads_as_time(df: DataFrame, column: str) -> bool:
    """Whether a string *column* holds date-times: any of its first non-null values reads as one."""
    value = F.col(column)
    probe = df.select(value).where(value.isNotNull()).limit(_PROBE_ROWS)
    return probe.where(value.try_cast("timestamp").isNotNull()).limit(1).count() > 0


def plan_time_window(
    df: DataFrame,
    minutes: int | None,
    timezone_name: str | None = None,
    name_priority: Sequence[str] | None = None,
    pinned: str | None = None,
) -> TimeWindow | None:
    """Decide how to apply a *minutes* lookback to *df*, or None for no window.

    No window when *minutes* isn't a positive number, or no time column fits
    (logged). A string candidate whose values don't read as date-times (free
    text such as ``'Q3 FY26'``) is passed over for the next one, so it can't
    turn the window into "no rows". Raises ValueError for an invalid
    *timezone_name*.
    """
    if not minutes or minutes <= 0:
        return None
    if name_priority is not None and (
        isinstance(name_priority, str) or not all(isinstance(n, str) for n in name_priority)
    ):
        raise ValueError("sample_interval_columns must be a list of column names")
    zone = validate_timezone(timezone_name) if timezone_name else "UTC"
    candidates = time_column_candidates(
        df.schema,
        name_priority=TIME_COLUMN_NAME_PRIORITY if name_priority is None else name_priority,
        pinned=pinned,
    )
    for column, source_type in candidates:
        if source_type == "string" and not pinned and not _reads_as_time(df, column):
            logger.info("Row Scope: string column '%s' doesn't hold date-times — trying the next column", column)
            continue
        kind = {"timestamp": "instant", "date": "day"}.get(source_type, "wall_clock")
        logger.info(
            "Row Scope: last %d minutes by %s column '%s' (%s, zone %s)", minutes, source_type, column, kind, zone
        )
        return TimeWindow(column=column, kind=kind, source_type=source_type, minutes=int(minutes), timezone=zone)
    logger.warning("Row Scope: no date/time column found — taking an unordered sample instead of a window")
    return None


def _contains_unhashable(data_type: DataType) -> bool:
    """Whether *data_type* is, or nests, a map or variant (which xxhash64 rejects)."""
    if isinstance(data_type, (MapType, VariantType)):
        return True
    if isinstance(data_type, StructType):
        return any(_contains_unhashable(f.dataType) for f in data_type.fields)
    if isinstance(data_type, ArrayType):
        return _contains_unhashable(data_type.elementType)
    return False


def hashable_columns(df: DataFrame) -> list[str]:
    """Columns a row hash can include: none that is or nests a map or variant."""
    return [f.name for f in df.schema.fields if not _contains_unhashable(f.dataType)]


def _as_utc(window: TimeWindow, value: Column) -> tuple[Column, Column | None]:
    """*value* as a UTC wall-clock ``TIMESTAMP_NTZ``, and its local wall-clock reading when it has one.

    Both are independent of the session time zone: an instant is converted to
    UTC, and a wall-clock reading is converted from the window's zone.
    """
    zone, utc = F.lit(window.timezone), F.lit("UTC")
    if window.kind == "instant":
        return F.convert_timezone(None, utc, value), None
    if window.source_type == "timestamp_ntz":
        return F.convert_timezone(zone, utc, value), value
    # A string: one that ends in its own zone is an instant; any other is a
    # wall-clock reading in the window's zone. try_cast leaves a value that
    # doesn't parse NULL, so it is simply outside the window.
    explicit = value.rlike(_EXPLICIT_ZONE)
    local = F.when(~explicit, value.try_cast("timestamp_ntz"))
    instant = F.convert_timezone(None, utc, value.try_cast("timestamp"))
    return F.when(explicit, instant).otherwise(F.convert_timezone(zone, utc, local)), local


def apply_time_window(
    df: DataFrame, window: TimeWindow, sample_size: int = 0, now: datetime | None = None
) -> DataFrame:
    """Keep the rows of *df* inside *window*; with *sample_size*, the latest that many.

    The cutoff is fixed at *now* (default: the current time). "Latest N" ties
    (rows sharing a timestamp, or a whole day) are broken by a hash of the row
    so repeated runs pick the same rows.
    """
    bounds = window_bounds(window, now or datetime.now(timezone.utc))
    cutoff = F.lit(bounds.cutoff_utc.isoformat(sep=" ")).cast("timestamp_ntz")
    first_day = F.lit(bounds.first_day)
    value = F.col(window.column)
    if window.kind == "day":
        order_by = value
        df = df.where(value >= first_day)
    else:
        order_by, local = _as_utc(window, value)
        inside = order_by >= cutoff
        if local is not None:
            # A wall-clock value at exactly midnight stands for its whole day.
            day = F.to_date(local)
            inside = inside | ((local == day.cast("timestamp_ntz")) & (day >= first_day))
        df = df.where(inside)
    if not sample_size:
        return df
    hashable = hashable_columns(df)
    tiebreak = [F.xxhash64(*[F.col(c) for c in hashable]).asc()] if hashable else []
    return df.orderBy(order_by.desc(), *tiebreak).limit(sample_size)
