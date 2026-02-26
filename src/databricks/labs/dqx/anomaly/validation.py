"""Validation for anomaly detection: training inputs and model record compatibility.

- Training-time: Spark version, fully qualified names, columns, and training params.
- Inference-time: compatibility checks when using an AnomalyModelRecord
  (e.g. sklearn version mismatch). Registry types and persistence live in model_registry.
"""

import collections.abc
import warnings
import sklearn
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.model_config import AnomalyModelRecord
from databricks.labs.dqx.anomaly.transformers import ColumnTypeClassifier
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError


# =============================================================================
# Training-time validation
# =============================================================================


def validate_spark_version(spark: SparkSession) -> None:
    """Validate Spark version is compatible with anomaly detection."""
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 4):
        raise InvalidParameterError(
            f"Anomaly detection requires Spark >= 3.4 for SynapseML compatibility. Found Spark {major}.{minor}."
        )


def validate_fully_qualified_name(value: str, *, label: str) -> None:
    """Validate that a name is in catalog.schema.table format (exactly three non-empty parts)."""
    parts = value.split(".")
    if len(parts) != 3 or not all(parts):
        raise InvalidParameterError(f"{label} must be fully qualified as catalog.schema.table, got: {value!r}.")


def validate_columns(
    df: DataFrame, columns: collections.abc.Iterable[str], params: AnomalyParams | None = None
) -> list[str]:
    """Validate columns for row anomaly detection with multi-type support."""
    params = params or AnomalyParams()
    fe_config = params.feature_engineering

    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    _column_infos, warnings_list = classifier.analyze_columns(df, list(columns))
    return warnings_list


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
    if min_exclusive is not None and numeric_value <= min_exclusive:
        raise InvalidParameterError(f"{label} must be > {min_exclusive}. Got {value}.")
    if max_inclusive is not None and numeric_value > max_inclusive:
        raise InvalidParameterError(f"{label} must be <= {max_inclusive}. Got {value}.")


def _validate_int_min(value: int, *, label: str, min_value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidParameterError(f"{label} must be an integer.")
    if value < min_value:
        raise InvalidParameterError(f"{label} must be >= {min_value}. Got {value}.")


def validate_training_params(params: AnomalyParams, expected_anomaly_rate: float) -> None:
    """Validate training parameters with strict fail-fast checks."""
    _validate_float_range(params.sample_fraction, label="params.sample_fraction", min_exclusive=0.0, max_inclusive=1.0)
    _validate_float_range(params.train_ratio, label="params.train_ratio", min_exclusive=0.0, max_inclusive=1.0)

    _validate_int_min(params.max_rows, label="params.max_rows", min_value=1)

    if params.ensemble_size is not None:
        _validate_int_min(params.ensemble_size, label="params.ensemble_size", min_value=1)

    _validate_float_range(
        expected_anomaly_rate,
        label="expected_anomaly_rate",
        min_exclusive=0.0,
        max_inclusive=0.5,
    )

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
    if not model_record.segmentation.sklearn_version:
        # Old models without version tracking - can't validate
        return

    trained_version = model_record.segmentation.sklearn_version
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
