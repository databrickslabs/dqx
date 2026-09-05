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
    * properties: *DQSemanticTypeProperties*, *EnumProperties*, *MeasurementProperties*
    * registry: *SemanticRegistry*
      — construction: *SemanticRegistry.default()*, *SemanticRegistry.of(...)*,
      constructor *SemanticRegistry(detectors=(...))*
      — composition: *prepend*, *append*, *insert(name, detector)*,
      *replace(name, detector)*, *remove(name)*
    * detectors: *DEFAULT_ENUM_DETECTOR*, *DEFAULT_KEY_DETECTOR*,
      *DEFAULT_MEASUREMENT_DETECTOR*, *DEFAULT_TEXT_DETECTOR*,
      *default_semantic_detectors()*
    * thresholds: *ENUM_MAX_CARDINALITY_RATIO*, *KEY_MIN_DENSITY_RATIO*,
      *KEY_MIN_LENGTH_STABILITY_RATIO*
"""

import logging
from collections.abc import Callable, Mapping
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import DataType

from databricks.labs.dqx.profiler.common import TEXT_TYPES
from databricks.labs.dqx.profiler.profile_options import PROFILE_OPTION_DISTINCT_RATIO, PROFILE_OPTION_MAX_IN_COUNT

logger = logging.getLogger(__name__)


Scalar = str | int | float | bool


class DQSemanticTypeProperties(BaseModel):
    """Base class for detector-specific properties emitted alongside a *DQSemanticType*.

    Extension pattern: user-defined detectors subclass this and pass an instance
    as the *properties* field of the *DQSemanticType* they emit. The model is
    frozen and forbids extra fields, matching *DQSemanticType* itself.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


class EnumProperties(DQSemanticTypeProperties):
    """Properties for the built-in *enum* semantic type.

    Attributes:
        values: Collected distinct values. Homogeneous by construction — the
            enum detector only fires on a single scalar column type, so the
            set carries a single scalar type per instance.
    """

    values: set[str] | set[int]


class MeasurementProperties(DQSemanticTypeProperties):
    """Properties for the built-in *measurement* semantic type.

    Attributes:
        distribution: Best-effort distribution family guess. *unknown* when
            statistics were insufficient to classify.
    """

    distribution: Literal["constant", "uniform", "normal", "exponential", "unknown"] = "unknown"


_PropertiesT = TypeVar("_PropertiesT", bound=DQSemanticTypeProperties)


class DQSemanticType(BaseModel):
    """Classification of a column's semantic meaning.

    Attributes:
        name: Globally unique identifier (e.g. *key*, *enum*, *measurement*).
            Used in generated check metadata and for detector lookup.
        description: Optional human-readable description.
        properties: Detector-specific properties as a *DQSemanticTypeProperties*
            subclass instance, or *None* when the detector emits no properties.
            The *frozen* model config blocks reassignment of the field itself;
            each properties subclass is frozen too, so the returned instance
            is fully immutable.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    description: str | None = None
    properties: DQSemanticTypeProperties | None = None

    def get_typed_properties(self, properties_type: type[_PropertiesT]) -> _PropertiesT | None:
        """Return *self.properties* narrowed to *properties_type*, or *None* when the type does not match.

        Args:
            properties_type: Concrete *DQSemanticTypeProperties* subclass to cast to.

        Returns:
            The properties instance typed as *properties_type* when it is an
            instance of that class, else *None*.
        """
        return self.properties if isinstance(self.properties, properties_type) else None


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
    semantic_type: DQSemanticType | None = None

    def with_metrics(self, metrics: Mapping[str, Any]) -> "DQProfileContext":
        """Return a new context whose *metrics* is a fresh snapshot of the supplied mapping.

        The model is frozen and Pydantic materializes *metrics* as a copy on construction, so
        contextual profile builders further down the chain would otherwise not observe write-backs
        made to the outer mutable metrics dict between builder invocations (e.g. the min/max
        write-back performed by *DQProfiler._build_profiles_for_column* after the *min_max*
        builder resolves outlier-adjusted bounds). Callers pass the current mutable dict here to
        obtain a new frozen context that wraps its snapshot; all other fields are preserved.

        Args:
            metrics: Column-level metrics to snapshot into the returned context.

        Returns:
            A new *DQProfileContext* instance with *metrics* replaced by a fresh dict copy of
            the supplied mapping.
        """
        return self.model_copy(update={"metrics": dict(metrics)})


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


# Enum: fallback ceiling for cardinality / count_non_null used only when
# *PROFILE_OPTION_DISTINCT_RATIO* is absent from *ctx.options*. Kept aligned
# with the *is_in* builder's default *distinct_ratio* so semantic profiling
# and legacy profiling agree on which columns qualify as enum-like. The
# effective gate is strict *>=* (mirrors *is_in*'s *distinct_ratio <
# max_distinct_ratio* emission condition).
ENUM_MAX_CARDINALITY_RATIO = 0.05

# Key (numeric): count_distinct / (max - min + 1) must be at or above this
# ratio for a numeric column to be classified as *key*. Tolerates small
# gaps while rejecting continuous distributions where max - min swamps
# count_distinct.
KEY_MIN_DENSITY_RATIO = 0.99

# Key (string): min(length) / max(length) over non-null values must be at
# or above this ratio for a string column to be classified as *key*.
# Real-world string keys (UUIDs, SKUs, hashes) have near-uniform length.
KEY_MIN_LENGTH_STABILITY_RATIO = 0.95


def _is_numeric(column_type: DataType) -> bool:
    return isinstance(column_type, T.NumericType)


def _is_text(column_type: DataType) -> bool:
    return isinstance(column_type, TEXT_TYPES)


def _detect_enum(ctx: DQProfileContext) -> DQSemanticType | None:
    """Detect *enum*-like columns: low cardinality and highly repeated values.

    The detector is the sole owner of the enum value collection: when it
    fires it runs a single distinct-collect on *ctx.df* and hands the result
    to downstream builders through *ctx.semantic_type.properties.values*.
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

    distinct_ratio_threshold = ctx.options.get(PROFILE_OPTION_DISTINCT_RATIO, ENUM_MAX_CARDINALITY_RATIO)
    if (cardinality / count_non_null) >= distinct_ratio_threshold:
        return None

    col = ctx.df.columns[0]
    distinct_rows = ctx.df.select(col).distinct().collect()
    distinct_values = {row[0] for row in distinct_rows}

    return DQSemanticType(name="enum", properties=EnumProperties(values=distinct_values))


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
        return _detect_numeric_key(ctx, cardinality)

    if _is_text(column_type):
        return _detect_text_key(ctx)

    return None


def _detect_numeric_key(ctx: DQProfileContext, cardinality: int) -> DQSemanticType | None:
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
    return DQSemanticType(name="key")


def _detect_text_key(ctx: DQProfileContext) -> DQSemanticType | None:
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
    return DQSemanticType(name="key")


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

    distribution: Literal["constant", "uniform", "normal", "exponential", "unknown"] = "unknown"
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

    return DQSemanticType(name="measurement", properties=MeasurementProperties(distribution=distribution))


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
    instantiation, *of()*, *default()*, *prepend()*, *append()*, *insert()*,
    *replace()*, *remove()*) route through the model validator, which enforces
    detector-name uniqueness.

    Attributes:
        detectors: Ordered tuple of detectors in first-match-wins order.
            Defaults to an empty tuple; use *SemanticRegistry.default()* to
            obtain the built-in chain or *SemanticRegistry.of(...)* to build
            an arbitrary chain from scratch.
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

    @classmethod
    def of(cls, *detectors: DQSemanticTypeDetector) -> "SemanticRegistry":
        """Return a registry whose chain is exactly *detectors*, in argument order.

        Ergonomic alternative to the ``SemanticRegistry(detectors=(...))`` constructor for
        whole-chain construction from scratch. Reads well when composing with the built-in
        defaults, e.g. ``SemanticRegistry.of(uuid_detector, *default_semantic_detectors())``.

        Args:
            *detectors: Detectors in first-match-wins order.

        Returns:
            A new *SemanticRegistry*. Detector-name uniqueness is enforced by the model
            validator, so duplicate names raise *pydantic.ValidationError*.
        """
        return cls(detectors=tuple(detectors))

    def prepend(self, detector: DQSemanticTypeDetector) -> "SemanticRegistry":
        """Return a new registry with *detector* at position 0 (highest priority).

        The current instance is left unchanged. Re-invokes the constructor
        so the *_validate_unique_names* model-validator runs on the derived
        instance — using *model_copy(update=...)* would skip validation and
        allow duplicate detector names.
        """
        return type(self)(detectors=(detector, *self.detectors))

    def append(self, detector: DQSemanticTypeDetector) -> "SemanticRegistry":
        """Return a new registry with *detector* appended at the end (lowest-priority fallback).

        The current instance is left unchanged. Re-invokes the constructor so the uniqueness
        validator runs on the derived instance.
        """
        return type(self)(detectors=(*self.detectors, detector))

    def insert(self, name: str, detector: DQSemanticTypeDetector) -> "SemanticRegistry":
        """Return a new registry with *detector* inserted immediately after the entry named *name*.

        Semantic: "insert *detector* after the detector identified by *name*". Position of the
        target detector is preserved; every subsequent detector shifts down by one.

        Args:
            name: Name of an existing detector; *detector* is placed at the position
                immediately after it.
            detector: Detector to insert.

        Returns:
            A new *SemanticRegistry* with the additional entry.

        Raises:
            ValueError: If *name* does not match any detector in the current chain.
            pydantic.ValidationError: If *detector*'s name collides with a non-target entry.
        """
        index = self._index_of(name)
        new_detectors = (*self.detectors[: index + 1], detector, *self.detectors[index + 1 :])
        return type(self)(detectors=new_detectors)

    def replace(self, name: str, detector: DQSemanticTypeDetector) -> "SemanticRegistry":
        """Return a new registry with the entry named *name* swapped for *detector*.

        Position is preserved; the rest of the chain is unchanged. To construct a whole
        new chain, use ``SemanticRegistry.of(...)`` or the ``SemanticRegistry(detectors=(...))``
        constructor instead.

        Args:
            name: Name of the detector to swap out.
            detector: Replacement detector. May reuse *name* (position-preserving update) or
                take a different name.

        Returns:
            A new *SemanticRegistry* with the entry replaced.

        Raises:
            ValueError: If *name* does not match any detector in the current chain.
            pydantic.ValidationError: If the replacement's name collides with a non-target
                entry in the chain.
        """
        index = self._index_of(name)
        new_detectors = (*self.detectors[:index], detector, *self.detectors[index + 1 :])
        return type(self)(detectors=new_detectors)

    def remove(self, name: str) -> "SemanticRegistry":
        """Return a new registry with the entry named *name* filtered out.

        Args:
            name: Name of the detector to drop.

        Returns:
            A new *SemanticRegistry* without the entry.

        Raises:
            ValueError: If *name* does not match any detector in the current chain.
        """
        index = self._index_of(name)
        new_detectors = (*self.detectors[:index], *self.detectors[index + 1 :])
        return type(self)(detectors=new_detectors)

    def _index_of(self, name: str) -> int:
        for i, detector in enumerate(self.detectors):
            if detector.name == name:
                return i
        raise ValueError(f"Detector {name!r} is not registered")
