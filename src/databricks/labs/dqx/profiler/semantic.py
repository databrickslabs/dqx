"""Semantic-type classification for the DQProfiler.

Introduces a lightweight classification stage between metric collection and check
generation so a column receives one consistent family of checks (e.g. an enum
column no longer receives both *is_in* and *min_max*).

All three mutually-dependent public models — *DQSemanticType*,
*DQProfileContext*, and *DQSemanticTypeDetector* — live in this single module
to keep the forward-reference cycle
(*DQSemanticTypeDetector.detect* depends on *DQProfileContext*;
*DQProfileContext.semantic_type* depends on *DQSemanticType*) resolvable at
class-creation time without quoted forward refs or *model_rebuild()*.

Public surface:
    * models: *DQSemanticType*, *DQProfileContext*, *DQSemanticTypeDetector*
    * registry: *SemanticRegistry*
    * detectors: *DEFAULT_ENUM_DETECTOR*, *DEFAULT_KEY_DETECTOR*,
      *DEFAULT_MEASUREMENT_DETECTOR*, *DEFAULT_TEXT_DETECTOR*,
      *default_semantic_detectors()*
    * thresholds: *ENUM_MAX_CARDINALITY_RATIO*, *KEY_MIN_DENSITY_RATIO*,
      *KEY_MIN_LENGTH_STABILITY_RATIO*
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import DataType

from databricks.labs.dqx.profiler.profile_options import PROFILE_OPTION_MAX_IN_COUNT

logger = logging.getLogger(__name__)


Scalar = str | int | float | bool
PropertyValue = Scalar | tuple[Scalar, ...]


# TODO (IK): Make properties `BaseModel` too.
class DQSemanticType(BaseModel):
    """Classification of a column's semantic meaning.

    Attributes:
        name: Globally unique identifier (e.g. *key*, *enum*, *measurement*).
            Used in generated check metadata and for detector lookup.
        description: Optional human-readable description.
        properties: Detector-specific properties (e.g. distribution family,
            enum values). Sequence values must be *tuple* (the model's value
            type is *Scalar | tuple[Scalar, ...]*), so individual entries
            are immutable; the *frozen* model config blocks reassignment of
            the *properties* field itself.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    description: str | None = None
    properties: Mapping[str, PropertyValue] = Field(default_factory=dict)


class DQProfileContext(BaseModel):
    """Immutable context passed to semantic detectors and profile builders.

    Attributes:
        df: DataFrame for this column with non-null rows only. Strings are
            trimmed when *profiler_options["trim_strings"]* is True.
        column_name: Name of the column being profiled.
        column_type: Spark DataType of the column.
        metrics: Column-level statistics (count, count_null, empty_count,
            count_non_null, ...). Same shape as *summary_stats[column_name]*.
        options: Profiler options for this run (max_null_ratio, max_empty_ratio,
            max_in_count, trim_strings, filter, ...).
        metadata: Table/column metadata sourced from Unity Catalog when the
            profile is derived from a Delta table. Keys surfaced:
            *table_name*, *table_comment*, *column_comment*. Tags are
            intentionally excluded. The mapping is empty when profiling a
            raw DataFrame with no table origin.
        semantic_type: Detected semantic type for this column, or *None* if
            no detector matched. Populated only for profile builders — always
            *None* for semantic detectors.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    df: DataFrame
    column_name: str
    column_type: DataType
    metrics: Mapping[str, Any] = Field(default_factory=dict)
    options: Mapping[str, Any] = Field(default_factory=dict)
    metadata: Mapping[str, Any] = Field(default_factory=dict)
    semantic_type: DQSemanticType | None = None


class DQSemanticTypeDetector(BaseModel):
    """Named detector that classifies a column's semantic type.

    Attributes:
        name: Detector identifier (also used as the produced type's name when
            the detector emits a single canonical type).
        detect: Callable receiving a *DQProfileContext* and returning a
            *DQSemanticType* when the column matches, otherwise *None*.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    detect: Callable[[DQProfileContext], DQSemanticType | None]


# Enum: cardinality / count_non_null must be at or below this ratio for a
# column to be classified as *enum* (in addition to
# cardinality < max_in_count). A higher ratio ceiling is more permissive
# — it lets slightly less-repeated columns still register as enum-like
# while still rejecting near-unique identifier columns.
ENUM_MAX_CARDINALITY_RATIO = 0.95

# Key (numeric): count_distinct / (max - min + 1) must be at or above this
# ratio for a numeric column to be classified as *key*. Tolerates small
# gaps while rejecting continuous distributions where max - min swamps
# count_distinct.
KEY_MIN_DENSITY_RATIO = 0.99

# Key (string): min(length) / max(length) over non-null values must be at
# or above this ratio for a string column to be classified as *key*.
# Real-world string keys (UUIDs, SKUs, hashes) have near-uniform length.
KEY_MIN_LENGTH_STABILITY_RATIO = 0.95


# TODO (IK): Use standard PySpark numeric types for this.
_NUMERIC_TYPES: tuple[type[DataType], ...] = (
    T.IntegerType,
    T.LongType,
    T.ShortType,
    T.DoubleType,
    T.FloatType,
    T.DecimalType,
    T.ByteType,
)

# TODO (IK): Reuse `TEXT_TYPES` from profile_builder.py
_TEXT_TYPES: tuple[type[DataType], ...] = (T.StringType, T.CharType, T.VarcharType)


def _is_numeric(column_type: DataType) -> bool:
    return isinstance(column_type, _NUMERIC_TYPES)


def _is_text(column_type: DataType) -> bool:
    return isinstance(column_type, _TEXT_TYPES)


def _detect_enum(ctx: DQProfileContext) -> DQSemanticType | None:
    """Detect *enum*-like columns: low cardinality and highly repeated values.

    The detector is the sole owner of the enum value collection: when it
    fires it runs a single distinct-collect on *ctx.df* and hands the result
    to downstream builders through *ctx.semantic_type.properties["values"]*.
    """
    column_type = ctx.column_type
    if not (_is_text(column_type) or isinstance(column_type, (T.IntegerType, T.LongType, T.ShortType))):
        return None

    count_non_null = ctx.metrics.get("count_non_null", 0)
    if count_non_null == 0:
        return None

    cardinality = ctx.metrics.get("count_distinct", 0)
    if cardinality == 0:
        return None

    max_in_count = ctx.options.get(PROFILE_OPTION_MAX_IN_COUNT, 0)
    if cardinality >= max_in_count:
        return None

    if (cardinality / count_non_null) > ENUM_MAX_CARDINALITY_RATIO:
        return None

    col = ctx.df.columns[0]
    distinct_rows = ctx.df.select(col).distinct().collect()
    distinct_values = [row[0] for row in distinct_rows]
    try:
        sorted_distinct = sorted(distinct_values)
    except TypeError:
        sorted_distinct = distinct_values

    return DQSemanticType(name="enum", properties={"values": tuple(sorted_distinct)})


def _detect_key(ctx: DQProfileContext) -> DQSemanticType | None:
    """Detect *key*-like columns using two positive signals.

    Distinctness (>= 0.99) is the required primary signal for all types; a
    second type-specific signal must also fire:

    * Numeric columns — density = count_distinct / (max - min + 1) must be
      at or above *KEY_MIN_DENSITY_RATIO*.
    * String columns — length_stability = min_length / max_length must be at
      or above *KEY_MIN_LENGTH_STABILITY_RATIO*.
    """
    count_non_null = ctx.metrics.get("count_non_null", 0)
    if count_non_null == 0:
        return None

    cardinality = ctx.metrics.get("count_distinct", 0)
    if cardinality == 0:
        return None

    distinctness = cardinality / count_non_null
    if distinctness < 0.99:
        return None

    column_type = ctx.column_type

    if isinstance(column_type, (T.IntegerType, T.LongType, T.ShortType)):
        min_value = ctx.metrics.get("min")
        max_value = ctx.metrics.get("max")
        if min_value is None or max_value is None:
            return None
        span = max_value - min_value + 1
        if span <= 0:
            return None
        density = cardinality / span
        if density < KEY_MIN_DENSITY_RATIO:
            return None
        return DQSemanticType(name="key", properties={"signal": "density"})

    if _is_text(column_type):
        col = ctx.df.columns[0]
        agg = ctx.df.select(
            F.min(F.length(F.col(col))).alias("min_len"),
            F.max(F.length(F.col(col))).alias("max_len"),
        ).first()
        if agg is None:
            return None
        min_len = agg["min_len"]
        max_len = agg["max_len"]
        if min_len is None or max_len is None:
            return None
        if max_len == 0:
            # Empty-string-only column — division by zero would follow. Not a key.
            return None
        length_stability = min_len / max_len
        if length_stability < KEY_MIN_LENGTH_STABILITY_RATIO:
            return None
        return DQSemanticType(name="key", properties={"signal": "length_stability"})

    return None


def _detect_measurement(ctx: DQProfileContext) -> DQSemanticType | None:
    """Fallback numeric detector: emit *measurement* with a best-effort distribution guess."""
    if not _is_numeric(ctx.column_type):
        return None
    if ctx.metrics.get("count_non_null", 0) == 0:
        return None

    mean = ctx.metrics.get("mean")
    stddev = ctx.metrics.get("stddev")
    min_value = ctx.metrics.get("min")
    max_value = ctx.metrics.get("max")

    distribution = "unknown"
    if mean is not None and stddev is not None and min_value is not None and max_value is not None:
        try:
            span = float(max_value) - float(min_value)
            stddev_f = float(stddev)
            mean_f = float(mean)
        except (TypeError, ValueError):
            span = 0.0
            stddev_f = 0.0
            mean_f = 0.0
        if span <= 0:
            distribution = "constant"
        elif stddev_f == 0:
            distribution = "constant"
        elif abs(mean_f) > 0 and stddev_f / max(abs(mean_f), 1e-12) > 1.0:
            distribution = "exponential"
        elif stddev_f / span < 0.15:
            distribution = "uniform"
        else:
            distribution = "normal"

    return DQSemanticType(name="measurement", properties={"distribution": distribution})


def _detect_text(ctx: DQProfileContext) -> DQSemanticType | None:
    """Fallback string detector: any string column not otherwise consumed."""
    if not _is_text(ctx.column_type):
        return None
    if ctx.metrics.get("count_non_null", 0) == 0:
        return None
    return DQSemanticType(name="text")


DEFAULT_ENUM_DETECTOR = DQSemanticTypeDetector(name="enum", detect=_detect_enum)
DEFAULT_KEY_DETECTOR = DQSemanticTypeDetector(name="key", detect=_detect_key)
DEFAULT_MEASUREMENT_DETECTOR = DQSemanticTypeDetector(name="measurement", detect=_detect_measurement)
DEFAULT_TEXT_DETECTOR = DQSemanticTypeDetector(name="text", detect=_detect_text)


def default_semantic_detectors() -> tuple[DQSemanticTypeDetector, ...]:
    """Return the built-in detector chain in first-match-wins order.

    Order: enum → key (statistical shape) → measurement (numeric fallback)
    → text (string fallback).
    """
    return (
        DEFAULT_ENUM_DETECTOR,
        DEFAULT_KEY_DETECTOR,
        DEFAULT_MEASUREMENT_DETECTOR,
        DEFAULT_TEXT_DETECTOR,
    )


class SemanticRegistry(BaseModel):
    """Immutable, ordered, name-unique collection of semantic-type detectors.

    Detectors are consulted in insertion order; the first non-None match wins.
    The registry is immutable — the model is frozen (Pydantic rejects field
    assignment) and the *detectors* field is a tuple, so external callers
    cannot mutate the chain by aliasing. All construction paths (direct
    instantiation, *default()*, *prepend()*, *replace()*) route through the
    model validator, which enforces detector-name uniqueness.

    Attributes:
        detectors: Ordered tuple of detectors in first-match-wins order.
            Defaults to an empty tuple; use *SemanticRegistry.default()* to
            obtain the built-in chain.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    detectors: tuple[DQSemanticTypeDetector, ...] = ()

    @model_validator(mode="after")
    def _validate_unique_names(self) -> "SemanticRegistry":
        seen: set[str] = set()
        for detector in self.detectors:
            if detector.name in seen:
                raise ValueError(f"Detector {detector.name!r} is registered more than once")
            seen.add(detector.name)
        return self

    @classmethod
    def default(cls) -> "SemanticRegistry":
        """Return a registry populated with *default_semantic_detectors()*."""
        return cls(detectors=default_semantic_detectors())

    def prepend(self, detector: DQSemanticTypeDetector) -> "SemanticRegistry":
        """Return a new registry with *detector* at position 0.

        The current instance is left unchanged. Re-invokes the constructor
        so the *_validate_unique_names* model-validator runs on the derived
        instance — using *model_copy(update=...)* would skip validation and
        allow duplicate detector names.
        """
        return type(self)(detectors=(detector, *self.detectors))

    def replace(self, detectors: Sequence[DQSemanticTypeDetector]) -> "SemanticRegistry":
        """Return a new registry whose chain is *detectors*.

        The current instance is left unchanged. Re-invokes the constructor
        so the uniqueness validator runs on the derived instance.
        """
        return type(self)(detectors=tuple(detectors))
