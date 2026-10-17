"""Validation for anomaly detection: training inputs and model record compatibility.

- Training-time: Spark version, fully qualified names, columns, and training params.
- Inference-time: compatibility checks when using an AnomalyModelRecord
  (e.g. sklearn version mismatch). Registry types and persistence live in model_registry.
"""

import collections
import collections.abc
import math
import warnings
from typing import Literal, overload
import sklearn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.model_config import AnomalyModelRecord
from databricks.labs.dqx.anomaly.column_names import names_resolve_same
from databricks.labs.dqx.anomaly.transformers import (
    BASELINE_RELATIVE_SUFFIX,
    BOOLEAN_FEATURE_SUFFIX,
    CALENDAR_FEATURE_SUFFIXES,
    FREQUENCY_FEATURE_SUFFIX,
    NULL_INDICATOR_SUFFIX,
    TEMPORAL_RELATIVE_SUFFIX,
    ColumnTypeInfo,
    ColumnTypeClassifier,
    feature_category_for_type,
)
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError


# =============================================================================
# Training-time validation
# =============================================================================


def validate_spark_version(spark: SparkSession) -> None:
    """Validate Spark version is compatible with anomaly detection."""
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 5):
        raise InvalidParameterError(f"Anomaly detection requires Spark >= 3.5. Found Spark {major}.{minor}.")


def validate_fully_qualified_name(value: str, *, label: str) -> None:
    """Validate that a name is in catalog.schema.table format (exactly three non-empty parts)."""
    parts = [p.strip() for p in value.strip().split(".")]
    if len(parts) != 3 or not all(parts):
        raise InvalidParameterError(f"{label} must be fully qualified as catalog.schema.table, got: {value!r}.")


@overload
def validate_columns(
    df: DataFrame,
    columns: collections.abc.Iterable[str],
    params: AnomalyParams | None = None,
    *,
    baseline_by: list[str] | None = None,
    baseline_over_time: str | None = None,
    include_column_infos: Literal[True],
) -> tuple[list[ColumnTypeInfo], list[str]]: ...


@overload
def validate_columns(
    df: DataFrame,
    columns: collections.abc.Iterable[str],
    params: AnomalyParams | None = None,
    *,
    baseline_by: list[str] | None = None,
    baseline_over_time: str | None = None,
    include_column_infos: Literal[False] = False,
) -> list[str]: ...


def validate_columns(
    df: DataFrame,
    columns: collections.abc.Iterable[str],
    params: AnomalyParams | None = None,
    *,
    baseline_by: list[str] | None = None,
    baseline_over_time: str | None = None,
    include_column_infos: bool = False,
) -> list[str] | tuple[list[ColumnTypeInfo], list[str]]:
    """Validate columns for row anomaly detection with multi-type support.

    *baseline_by* and *baseline_over_time* are passed for the feature-width warning alone: each adds one
    derived feature per numeric column, so omitting them understated the width by up to a factor of three
    and the warning stayed silent on exactly the configurations it exists to flag.
    """
    params = params or AnomalyParams()
    fe_config = params.feature_engineering

    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    column_infos, warnings_list = classifier.analyze_columns(
        df, list(columns), baseline_by=baseline_by, baseline_over_time=baseline_over_time
    )
    if include_column_infos:
        return column_infos, warnings_list
    return warnings_list


#: Group column types whose Spark ``cast("string")`` is guaranteed to match Python's ``str()``.
#: Floating-point and decimal types are excluded deliberately: the two disagree on formatting
#: (Spark renders 1.0 as "1.0" but 1e-7 differently from Python), and a group key that differs
#: between training and scoring misses every baseline lookup silently rather than failing.
_ALLOWED_GROUP_COLUMN_TYPES = (
    T.StringType,
    T.ByteType,
    T.ShortType,
    T.IntegerType,
    T.LongType,
    T.BooleanType,
    T.DateType,
)

#: Time column types elapsed seconds can be read from. A string that happens to hold a date is rejected:
#: parsing it would silently produce nulls for any row whose format differs, and a null time axis makes
#: every expectation for that row wrong rather than absent.
_ALLOWED_TIME_COLUMN_TYPES = (T.TimestampType, T.TimestampNTZType, T.DateType)


def validate_baseline_columns(
    df: DataFrame, baseline_by: list[str] | None, columns: collections.abc.Iterable[str]
) -> None:
    """Validate declared group columns.

    Group columns identify the comparison basis; they are not features. They must therefore
    exist, must not double as feature columns, and must have a type whose string rendering is
    stable between Spark and Python.
    """
    if not baseline_by:
        return

    duplicates = [name for name, count in collections.Counter(baseline_by).items() if count > 1]
    if duplicates:
        raise InvalidParameterError(f"baseline_by contains duplicate columns: {duplicates}.")

    schema_fields = {field.name: field.dataType for field in df.schema.fields}
    missing = [name for name in baseline_by if name not in schema_fields]
    if missing:
        raise InvalidParameterError(f"baseline_by columns not found in DataFrame: {missing}. Available: {df.columns}.")

    overlap = sorted(set(baseline_by) & set(columns))
    if overlap:
        raise InvalidParameterError(
            f"Columns {overlap} are used both as features and as baseline_by columns. A group column "
            "defines the basis a metric is compared against, so it cannot also be one of the "
            "metrics being compared. Remove them from one or the other."
        )

    unsupported = {
        name: schema_fields[name].simpleString()
        for name in baseline_by
        if not isinstance(schema_fields[name], _ALLOWED_GROUP_COLUMN_TYPES)
    }
    if unsupported:
        raise InvalidParameterError(
            f"baseline_by columns have unsupported types: {unsupported}. Group columns must be string, "
            "integral, boolean or date. Floating-point and decimal columns are rejected because "
            "Spark and Python format them differently, which would make a row's group key differ "
            "between training and scoring. Cast to string or bucket the value first."
        )


def validate_baseline_over_time(
    df: DataFrame, baseline_over_time: str | None, columns: collections.abc.Iterable[str]
) -> None:
    """Validate the declared time column.

    The same contract the group columns follow, for the same reason: a time column is the axis a metric is
    measured *along*, not a thing being measured, so it must exist, must not double as a feature, and must
    be a type a timestamp can be read from.

    Rejected loudly rather than coerced. Silently dropping the column from the feature list would leave a
    caller wondering why their explicit *columns* list did not produce the features they asked for.
    """
    if not baseline_over_time:
        return

    schema_fields = {field.name: field.dataType for field in df.schema.fields}
    if baseline_over_time not in schema_fields:
        raise InvalidParameterError(
            f"baseline_over_time column '{baseline_over_time}' not found in DataFrame. Available: {df.columns}."
        )

    if baseline_over_time in set(columns):
        raise InvalidParameterError(
            f"Column '{baseline_over_time}' is used both as a feature and as baseline_over_time. A time "
            "column is the axis a metric is measured along, so it cannot also be one of the metrics being "
            "measured. Remove it from columns."
        )

    if not isinstance(schema_fields[baseline_over_time], _ALLOWED_TIME_COLUMN_TYPES):
        raise InvalidParameterError(
            f"baseline_over_time column '{baseline_over_time}' has type "
            f"{schema_fields[baseline_over_time].simpleString()}, which is not a time type. It must be a "
            "timestamp or a date, because the expected level is fitted against elapsed seconds."
        )


def generated_feature_names(
    schema_fields: dict[str, T.DataType],
    columns: collections.abc.Iterable[str],
    baseline_by: list[str] | None,
    baseline_over_time: str | None,
    *,
    column_infos: collections.abc.Iterable[ColumnTypeInfo] | None = None,
) -> list[tuple[str, str, str]]:
    """Every ``(generated name, source column, transform)`` feature engineering will create.

    A list rather than a name-keyed mapping so that two sources generating the *same* name stay visible;
    a dict would silently keep one, which is the shape of the bug this feeds.

    When *column_infos* is provided, it describes the exact transformed projection from the classifier.
    Null indicators are then claimed only for columns that have nulls, and categorical frequency features
    only when frequency encoding is selected. One-hot aliases are collision-safe UUIDs and continue to be
    checked where they are allocated.
    """
    generated: list[tuple[str, str, str]] = []

    infos = list(column_infos) if column_infos is not None else None
    if infos is None:
        infos = [
            ColumnTypeInfo(name=name, spark_type=col_type, category=feature_category_for_type(col_type))
            for name in columns
            if (col_type := schema_fields.get(name)) is not None
        ]

    for info in infos:
        name = info.name
        if info.category == "numeric":
            generated.append((name, name, "the numeric feature"))
        if info.null_count and info.null_count > 0:
            generated.append((f"{name}{NULL_INDICATOR_SUFFIX}", name, "the null indicator"))
        derived = _derived_names_for_category(
            name,
            info.category,
            baseline_by,
            baseline_over_time,
            frequency_enabled=info.encoding_strategy == "frequency",
        )
        generated.extend((derived_name, name, transform) for derived_name, transform in derived)

    return generated


def _derived_names_for_category(
    name: str,
    category: str,
    baseline_by: list[str] | None,
    baseline_over_time: str | None,
    *,
    frequency_enabled: bool,
) -> list[tuple[str, str]]:
    """``(generated name, transform)`` for one column, from its category alone.

    Split out from the caller so each category's answer is one flat branch. An 'unsupported' column
    generates nothing because feature engineering skips it entirely.
    """
    if category == "numeric":
        derived = []
        if baseline_by:
            derived.append((f"{name}{BASELINE_RELATIVE_SUFFIX}", "the baseline_by comparison"))
        if baseline_over_time:
            derived.append((f"{name}{TEMPORAL_RELATIVE_SUFFIX}", "the baseline_over_time comparison"))
        return derived
    if category == "boolean":
        return [(f"{name}{BOOLEAN_FEATURE_SUFFIX}", "the boolean encoding")]
    if category == "datetime":
        return [(f"{name}{suffix}", "the calendar encoding") for suffix in CALENDAR_FEATURE_SUFFIXES]
    if category == "categorical" and frequency_enabled:
        return [(f"{name}{FREQUENCY_FEATURE_SUFFIX}", "the frequency encoding")]
    return []


def validate_generated_feature_names(
    df: DataFrame,
    columns: collections.abc.Iterable[str],
    baseline_by: list[str] | None,
    baseline_over_time: str | None,
    *,
    column_infos: collections.abc.Iterable[ColumnTypeInfo] | None = None,
) -> None:
    """Refuse a schema where a generated feature would land on the name of a real column.

    Feature engineering builds derived columns by appending a suffix and writing the result with
    ``withColumn``, which *replaces* a column of that name. A table carrying both ``amount`` and
    ``amount_rel_baseline`` therefore loses the second one: the derived value overwrites it, the name is
    appended to the feature list a second time, and the model is fitted on two copies of the derived
    value with the user's real metric gone. Nothing downstream can detect that, and per-feature
    attribution then names the wrong source column.

    Raising is the only honest option. Renaming the derived feature would leave the persisted feature
    list disagreeing with the suffix that redaction and attribution both key on, and dropping either
    column silently discards data the caller asked to be checked.
    """
    feature_columns = list(columns)
    generated = generated_feature_names(
        {field.name: field.dataType for field in df.schema.fields},
        feature_columns,
        baseline_by,
        baseline_over_time,
        column_infos=column_infos,
    )

    # Feature engineering projects only supported requested features. Incidental input columns are neither
    # features nor comparison bases, so their names cannot change the model projection and must not block
    # training. Bases stay here because the transforms still read them after derived columns are added.
    case_sensitive = _uses_case_sensitive_column_resolution(df)
    existing = [*feature_columns, *(baseline_by or [])]
    if baseline_over_time:
        existing.append(baseline_over_time)
    overwrites = sorted(
        entry
        for entry in generated
        if entry[0] != entry[1]
        and any(names_resolve_same(entry[0], name, case_sensitive=case_sensitive) for name in existing)
    )
    if overwrites:
        details = "; ".join(
            f"'{name}' already exists and would be overwritten by {transform} of '{source}'"
            for name, source, transform in overwrites
        )
        raise InvalidParameterError(
            f"Feature engineering would overwrite existing columns: {details}. Rename the existing "
            "column, or exclude it and its source from the checked columns. DQX refuses rather than "
            "picking one, because either choice silently drops data you asked it to check."
        )

    clashes = _generated_name_clashes(generated, case_sensitive=case_sensitive)
    if clashes:
        details = "; ".join(f"'{name}' from {sorted(sources)}" for name, sources in clashes)
        raise InvalidParameterError(
            f"Two checked columns would generate the same feature name: {details}. The feature list is "
            "positional, so a duplicated name makes one column's feature indistinguishable from the "
            "other's. Rename one of the source columns."
        )


def _uses_case_sensitive_column_resolution(df: DataFrame) -> bool:
    """Whether Spark resolves column names by their exact case for this DataFrame's session.

    Spark's resolver decides whether ``withColumn`` replaces a case-only name. Its configuration
    is not readable on every serverless runtime, so an empty isolated projection observes the
    resolver directly without inspecting or collecting the caller's rows.
    """
    probe = df.sparkSession.range(0).select(F.lit(0).alias("dqx_case_probe")).withColumn("DQX_CASE_PROBE", F.lit(0))
    return len(probe.columns) == 2


def _generated_name_clashes(
    generated: list[tuple[str, str, str]], *, case_sensitive: bool
) -> list[tuple[str, list[str]]]:
    """Find generated names that Spark will resolve to the same field, retaining their original spelling."""
    clashes: list[tuple[str, list[str]]] = []
    remaining = list(generated)
    while remaining:
        name, source, _ = remaining.pop(0)
        matching = [(name, source)]
        nonmatching: list[tuple[str, str, str]] = []
        for candidate in remaining:
            if names_resolve_same(name, candidate[0], case_sensitive=case_sensitive):
                matching.append((candidate[0], candidate[1]))
            else:
                nonmatching.append(candidate)
        remaining = nonmatching
        if len(matching) > 1:
            clashes.append((name, [candidate_source for _, candidate_source in matching]))
    return sorted(clashes)


def _validate_float_range(
    value: float,
    *,
    label: str,
    min_exclusive: float | None = None,
    max_inclusive: float | None = None,
) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidParameterError(f"{label} must be a numeric value.")
    numeric_value = float(value)
    if not math.isfinite(numeric_value):
        raise InvalidParameterError(f"{label} must be a finite numeric value.")
    if min_exclusive is not None and numeric_value <= min_exclusive:
        raise InvalidParameterError(f"{label} must be > {min_exclusive}. Got {value}.")
    if max_inclusive is not None and numeric_value > max_inclusive:
        raise InvalidParameterError(f"{label} must be <= {max_inclusive}. Got {value}.")


def _validate_int_min(value: int, *, label: str, min_value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidParameterError(f"{label} must be an integer.")
    if value < min_value:
        raise InvalidParameterError(f"{label} must be >= {min_value}. Got {value}.")


def validate_training_params(params: AnomalyParams) -> None:
    """Validate training parameters with strict fail-fast checks."""
    _validate_float_range(params.sample_fraction, label="params.sample_fraction", min_exclusive=0.0, max_inclusive=1.0)
    _validate_float_range(params.train_ratio, label="params.train_ratio", min_exclusive=0.0, max_inclusive=1.0)

    _validate_int_min(params.max_rows, label="params.max_rows", min_value=1)

    if params.ensemble_size is not None:
        _validate_int_min(params.ensemble_size, label="params.ensemble_size", min_value=1)

    algo_cfg = params.algorithm_config
    if algo_cfg.contamination is not None:
        _validate_float_range(
            algo_cfg.contamination,
            label="params.algorithm_config.contamination",
            min_exclusive=0.0,
            max_inclusive=0.5,
        )

    _validate_int_min(algo_cfg.num_trees, label="params.algorithm_config.num_trees", min_value=1)
    if algo_cfg.subsampling_rate is not None:
        _validate_float_range(
            algo_cfg.subsampling_rate,
            label="params.algorithm_config.subsampling_rate",
            min_exclusive=0.0,
            max_inclusive=1.0,
        )

    fe_cfg = params.feature_engineering
    _validate_int_min(
        fe_cfg.categorical_cardinality_threshold,
        label="params.feature_engineering.categorical_cardinality_threshold",
        min_value=1,
    )
    _validate_int_min(fe_cfg.max_input_columns, label="params.feature_engineering.max_input_columns", min_value=1)
    _validate_int_min(
        fe_cfg.max_engineered_features,
        label="params.feature_engineering.max_engineered_features",
        min_value=1,
    )


# =============================================================================
# Inference-time validation (model record compatibility)
# =============================================================================


def _parse_version_tuple(version_str: str) -> tuple[int, int]:
    """Parse version string into (major, minor) tuple."""
    parts = version_str.split(".")
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    return major, minor


def validate_sklearn_compatibility(model_record: AnomalyModelRecord) -> None:
    """Validate sklearn version compatibility between training and inference.

    Args:
        model_record: Model record containing sklearn_version from training

    Raises:
        Warning if minor version mismatch detected (e.g., 1.2.x vs 1.3.x)

    Example:
        >>> record = AnomalyModelRecord(...)
        >>> validate_sklearn_compatibility(record)
        # Warns if sklearn versions don't match
    """
    if not model_record.grouping.sklearn_version:
        # Old models without version tracking - can't validate
        return

    trained_version = model_record.grouping.sklearn_version
    current_version = sklearn.__version__

    if trained_version == current_version:
        return  # Perfect match

    # Parse versions
    try:
        trained_major, trained_minor = _parse_version_tuple(trained_version)
        current_major, current_minor = _parse_version_tuple(current_version)

        # Check for version mismatches
        if trained_major != current_major or trained_minor != current_minor:
            warnings.warn(
                f"\nSKLEARN VERSION MISMATCH DETECTED\n"
                f"Model Information:\n"
                f"  - Model: {model_record.identity.model_name}\n"
                f"  - Trained with: sklearn {trained_version}\n"
                f"  - Current environment: sklearn {current_version}\n"
                f"\n"
                f"Minor version changes (e.g., 1.2 -> 1.3) can cause pickle incompatibility errors.\n"
                f"\n"
                f"RECOMMENDED ACTION:\n"
                f"Retrain the model with your current sklearn version:\n"
                f"  anomaly_engine.train(\n"
                f"      df=df_train,\n"
                f"      model_name=\"{model_record.identity.model_name}\",\n"
                f"      columns={model_record.training.columns}\n"
                f"  )\n",
                UserWarning,
                stacklevel=3,
            )
    except (ValueError, IndexError):
        # Couldn't parse version - skip validation
        pass
