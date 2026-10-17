"""Unit tests for row anomaly detection validation logic and error handling."""

import warnings
from datetime import datetime

import pytest
import sklearn

from databricks.labs.dqx.anomaly import validation
from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    GroupingConfig,
    TrainingMetadata,
)
from databricks.labs.dqx.anomaly.validation import validate_sklearn_compatibility, validate_training_params
from databricks.labs.dqx.config import AnomalyParams, FeatureEngineeringConfig, IsolationForestConfig
from databricks.labs.dqx.errors import InvalidParameterError


# ============================================================================
# Spark Version Validation Tests
# ============================================================================


def test_validate_spark_version_raises_for_old_spark():
    """Test that validate_spark_version raises for Spark < 3.5."""
    mock_spark = type("MockSpark", (), {"version": "3.2.0"})()
    with pytest.raises(InvalidParameterError) as exc_info:
        validation.validate_spark_version(mock_spark)
    assert "Spark >= 3.5" in str(exc_info.value)
    assert "3.2" in str(exc_info.value)


def test_validate_spark_version_raises_for_spark_2():
    """Test that validate_spark_version raises for Spark 2.x."""
    mock_spark = type("MockSpark", (), {"version": "2.4.8"})()
    with pytest.raises(InvalidParameterError) as exc_info:
        validation.validate_spark_version(mock_spark)
    assert "Spark >= 3.5" in str(exc_info.value)
    assert "2.4" in str(exc_info.value)


def test_validate_spark_version_rejects_spark_3_4_and_accepts_3_5() -> None:
    with pytest.raises(InvalidParameterError, match="Spark >= 3.5"):
        validation.validate_spark_version(type("MockSpark", (), {"version": "3.4.9"})())

    validation.validate_spark_version(type("MockSpark", (), {"version": "3.5.0"})())


# ============================================================================
# Model Name Validation Tests
# ============================================================================


def test_validate_fully_qualified_name_accepts_valid():
    """Test fully qualified name validation with valid values."""
    validation.validate_fully_qualified_name("catalog.schema.model", label="model_name")
    validation.validate_fully_qualified_name("catalog.schema.registry", label="registry_table")


def test_validate_fully_qualified_name_rejects_invalid():
    """Test fully qualified name validation with invalid values."""
    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("model", label="model_name")

    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("schema.model", label="model_name")

    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("catalog..table", label="model_name")

    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("catalog.schema.table.extra", label="model_name")


# Exercise the real validators: literal range comparisons cannot detect a missing validation.
_FLOAT_PARAMS = [
    ("sample_fraction", 1.0),
    ("train_ratio", 1.0),
]
_ALGORITHM_FLOAT_PARAMS = [
    ("contamination", 0.5),
    ("subsampling_rate", 1.0),
]


@pytest.mark.parametrize("field,upper", _FLOAT_PARAMS)
@pytest.mark.parametrize("boundary", ["minimum_positive", "maximum"])
def test_training_float_parameters_accept_valid_boundaries(field: str, upper: float, boundary: str) -> None:
    value = 0.01 if boundary == "minimum_positive" else upper
    params = AnomalyParams()
    setattr(params, field, value)
    validate_training_params(params)


@pytest.mark.parametrize("field,upper", _ALGORITHM_FLOAT_PARAMS)
@pytest.mark.parametrize("boundary", ["minimum_positive", "maximum"])
def test_algorithm_float_parameters_accept_valid_boundaries(field: str, upper: float, boundary: str) -> None:
    value = 0.01 if boundary == "minimum_positive" else upper
    config = IsolationForestConfig()
    setattr(config, field, value)
    validate_training_params(AnomalyParams(algorithm_config=config))


@pytest.mark.parametrize("field,upper", _FLOAT_PARAMS)
@pytest.mark.parametrize("value", [0.0, -0.1, 2.0, float("nan"), float("inf"), -float("inf"), True, "0.5"])
def test_training_float_parameters_reject_invalid_values(field: str, upper: float, value: object) -> None:
    params = AnomalyParams()
    setattr(params, field, value)
    with pytest.raises(InvalidParameterError, match=field):
        validate_training_params(params)
    setattr(params, field, upper + 0.01)
    with pytest.raises(InvalidParameterError, match=field):
        validate_training_params(params)


@pytest.mark.parametrize("field,upper", _ALGORITHM_FLOAT_PARAMS)
@pytest.mark.parametrize("value", [0.0, -0.1, 2.0, float("nan"), float("inf"), -float("inf"), True, "0.5"])
def test_algorithm_float_parameters_reject_invalid_values(field: str, upper: float, value: object) -> None:
    config = IsolationForestConfig()
    setattr(config, field, value)
    with pytest.raises(InvalidParameterError, match=field):
        validate_training_params(AnomalyParams(algorithm_config=config))
    setattr(config, field, upper + 0.01)
    with pytest.raises(InvalidParameterError, match=field):
        validate_training_params(AnomalyParams(algorithm_config=config))


@pytest.mark.parametrize("field", ["max_rows", "ensemble_size"])
@pytest.mark.parametrize("value", [0, -1, 1.5, True, "2"])
def test_training_integer_parameters_reject_invalid_values(field: str, value: object) -> None:
    params = AnomalyParams()
    setattr(params, field, value)
    with pytest.raises(InvalidParameterError, match=field):
        validate_training_params(params)


@pytest.mark.parametrize("value", [0, -1, 1.5, True, "2"])
def test_tree_count_rejects_invalid_values(value: object) -> None:
    config = IsolationForestConfig()
    setattr(config, "num_trees", value)
    with pytest.raises(InvalidParameterError, match="num_trees"):
        validate_training_params(AnomalyParams(algorithm_config=config))


@pytest.mark.parametrize("field", ["categorical_cardinality_threshold", "max_input_columns", "max_engineered_features"])
@pytest.mark.parametrize("value", [0, -1, 1.5, True, "2"])
def test_feature_limits_reject_invalid_values(field: str, value: object) -> None:
    config = FeatureEngineeringConfig()
    setattr(config, field, value)
    with pytest.raises(InvalidParameterError, match=field):
        validate_training_params(AnomalyParams(feature_engineering=config))


def test_integer_parameter_minimums_are_valid() -> None:
    validate_training_params(
        AnomalyParams(
            max_rows=1,
            ensemble_size=1,
            algorithm_config=IsolationForestConfig(num_trees=1),
            feature_engineering=FeatureEngineeringConfig(
                categorical_cardinality_threshold=1, max_input_columns=1, max_engineered_features=1
            ),
        )
    )


def test_optional_training_parameters_can_be_unset() -> None:
    validate_training_params(
        AnomalyParams(
            ensemble_size=None, algorithm_config=IsolationForestConfig(contamination=None, subsampling_rate=None)
        )
    )


@pytest.mark.parametrize("label", ["model_name", "registry_table"])
@pytest.mark.parametrize(
    "value", ["", "model", "schema.model", "catalog..model", "catalog.schema.model.extra", " . . "]
)
def test_qualified_names_reject_empty_or_incomplete_components(label: str, value: str) -> None:
    with pytest.raises(InvalidParameterError, match=f"{label} must be fully qualified"):
        validation.validate_fully_qualified_name(value, label=label)


# ============================================================================
# validate_training_params — direct calls with real AnomalyParams
# ============================================================================


def test_validate_training_params_accepts_defaults():
    """Default AnomalyParams should not raise."""
    validate_training_params(AnomalyParams())


def test_validate_training_params_rejects_non_numeric_sample_fraction():
    """Test that non-numeric sample_fraction raises (validation.py 64-65)."""
    params = AnomalyParams(sample_fraction="0.5")  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be a numeric value"):
        validate_training_params(params)


def test_validate_training_params_rejects_bool_sample_fraction():
    """Test that bool sample_fraction raises."""
    params = AnomalyParams(sample_fraction=True)  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be a numeric value"):
        validate_training_params(params)


def test_validate_training_params_rejects_zero_sample_fraction():
    with pytest.raises(InvalidParameterError, match="params.sample_fraction"):
        validate_training_params(AnomalyParams(sample_fraction=0.0))


def test_validate_training_params_rejects_sample_fraction_above_one():
    with pytest.raises(InvalidParameterError, match="params.sample_fraction"):
        validate_training_params(AnomalyParams(sample_fraction=1.1))


def test_validate_training_params_still_range_checks_contamination():
    """The removed `expected_anomaly_rate` used to fill contamination. Setting it directly is now the
    only route, so its bounds matter more than before, not less."""
    with pytest.raises(InvalidParameterError, match="contamination"):
        validate_training_params(AnomalyParams(algorithm_config=IsolationForestConfig(contamination=0.9)))


def test_validate_training_params_rejects_non_integer_max_rows():
    """Test that non-integer max_rows raises."""
    params = AnomalyParams(max_rows=1000.5)  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be an integer"):
        validate_training_params(params)


def test_validate_training_params_rejects_bool_max_rows():
    """Test that bool max_rows raises."""
    params = AnomalyParams(max_rows=True)  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be an integer"):
        validate_training_params(params)


def test_validate_training_params_rejects_zero_max_rows():
    with pytest.raises(InvalidParameterError, match="params.max_rows"):
        validate_training_params(AnomalyParams(max_rows=0))


def test_validate_training_params_rejects_zero_ensemble_size():
    with pytest.raises(InvalidParameterError, match="params.ensemble_size"):
        validate_training_params(AnomalyParams(ensemble_size=0))


# ============================================================================
# validate_sklearn_compatibility — direct calls with real AnomalyModelRecord
# ============================================================================


def _make_record(sklearn_version: str | None) -> AnomalyModelRecord:
    return AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="catalog.schema.model",
            model_uri="models:/catalog.schema.model/1",
            algorithm="IsolationForestV1",
            mlflow_run_id="run123",
        ),
        training=TrainingMetadata(
            columns=["amount", "quantity"],
            hyperparameters={},
            training_rows=1000,
            training_time=datetime(2024, 1, 1),
        ),
        features=FeatureEngineering(),
        grouping=GroupingConfig(sklearn_version=sklearn_version),
    )


def test_validate_sklearn_compatibility_no_warning_on_exact_match():
    """No warning when the stored version matches the installed version."""
    record = _make_record(sklearn_version=sklearn.__version__)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_sklearn_compatibility(record)
    assert not any("SKLEARN" in str(w.message) for w in caught)


def test_validate_sklearn_compatibility_warns_on_version_mismatch():
    """UserWarning is emitted when the stored sklearn version differs from the current one."""
    record = _make_record(sklearn_version="0.0.0")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_sklearn_compatibility(record)
    assert any("SKLEARN VERSION MISMATCH" in str(w.message) for w in caught)


def test_validate_sklearn_compatibility_skips_when_no_version():
    """No warning and no error when model was saved without a sklearn_version."""
    record = _make_record(sklearn_version=None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_sklearn_compatibility(record)
    assert not any("SKLEARN" in str(w.message) for w in caught)
