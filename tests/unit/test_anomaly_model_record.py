"""Unit tests for AnomalyModelRecord data structure and registry utilities."""

from datetime import datetime
from decimal import Decimal


from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry


def test_anomaly_model_record_creation_with_defaults():
    """Test AnomalyModelRecord creation with default values."""
    record = AnomalyModelRecord(
        model_name="test_model",
        model_uri="models:/test_model/1",
        columns=["col1", "col2"],
        algorithm="IsolationForest",
        hyperparameters={"contamination": "0.1", "num_trees": "100"},
        training_rows=1000,
        training_time=datetime(2024, 1, 1, 12, 0, 0),
        mlflow_run_id="run123",
    )

    assert record.model_name == "test_model"
    assert record.model_uri == "models:/test_model/1"
    assert record.status == "active"  # Default
    assert record.mode == "spark"  # Default
    assert record.is_global_model is True  # Default
    assert record.metrics is None  # Default
    assert record.segment_by is None  # Default
    assert record.segment_values is None  # Default


def test_anomaly_model_record_with_all_fields():
    """Test AnomalyModelRecord with all fields populated."""
    record = AnomalyModelRecord(
        model_name="segmented_model",
        model_uri="models:/segmented_model/2",
        columns=["amount", "quantity", "discount"],
        algorithm="IsolationForest",
        hyperparameters={"contamination": "0.05", "num_trees": "200", "max_depth": "10"},
        training_rows=50000,
        training_time=datetime(2024, 6, 15, 14, 30, 0),
        mlflow_run_id="run456",
        status="archived",
        metrics={"precision": 0.85, "recall": 0.78},
        mode="sklearn",
        baseline_stats={
            "amount": {"mean": 150.0, "std": 25.0, "min": 50.0, "max": 300.0},
            "quantity": {"mean": 10.0, "std": 2.5, "min": 1.0, "max": 20.0},
        },
        feature_importance={"amount": 0.45, "quantity": 0.35, "discount": 0.20},
        temporal_config={"timestamp_column": "order_date", "features": "hour,day_of_week"},
        segment_by=["region"],
        segment_values={"region": "US"},
        is_global_model=False,
        column_types={"amount": "numeric", "quantity": "numeric", "discount": "numeric"},
        feature_metadata='{"engineered_features": ["amount_scaled", "quantity_scaled"]}',
    )

    assert record.model_name == "segmented_model"
    assert record.status == "archived"
    assert record.mode == "sklearn"
    assert record.is_global_model is False
    assert len(record.metrics) == 2
    assert record.metrics["precision"] == 0.85
    assert len(record.baseline_stats) == 2
    assert record.baseline_stats["amount"]["mean"] == 150.0
    assert len(record.feature_importance) == 3
    assert record.segment_by == ["region"]
    assert record.segment_values == {"region": "US"}
    assert record.column_types["amount"] == "numeric"
    assert "engineered_features" in record.feature_metadata


def test_convert_decimals_simple_decimal():
    """Test decimal conversion with a simple Decimal value."""
    result = AnomalyModelRegistry.convert_decimals(Decimal("123.45"))
    assert isinstance(result, float)
    assert result == 123.45


def test_convert_decimals_in_dict():
    """Test decimal conversion within a dictionary."""
    input_dict = {
        "metric1": Decimal("0.85"),
        "metric2": 0.92,
        "metric3": Decimal("123.456789"),
    }

    result = AnomalyModelRegistry.convert_decimals(input_dict)

    assert isinstance(result, dict)
    assert isinstance(result["metric1"], float)
    assert isinstance(result["metric2"], float)
    assert isinstance(result["metric3"], float)
    assert result["metric1"] == 0.85
    assert result["metric2"] == 0.92
    assert abs(result["metric3"] - 123.456789) < 1e-6


def test_convert_decimals_in_nested_dict():
    """Test decimal conversion in nested dictionary structures."""
    input_dict = {
        "baseline_stats": {
            "amount": {
                "mean": Decimal("150.50"),
                "std": Decimal("25.25"),
            },
            "quantity": {
                "mean": Decimal("10.0"),
                "std": Decimal("2.5"),
            },
        },
        "metrics": {
            "precision": Decimal("0.85"),
        },
    }

    result = AnomalyModelRegistry.convert_decimals(input_dict)

    assert isinstance(result["baseline_stats"]["amount"]["mean"], float)
    assert isinstance(result["baseline_stats"]["amount"]["std"], float)
    assert result["baseline_stats"]["amount"]["mean"] == 150.50
    assert result["baseline_stats"]["quantity"]["std"] == 2.5
    assert result["metrics"]["precision"] == 0.85


def test_convert_decimals_in_list():
    """Test decimal conversion in list structures."""
    input_list = [
        Decimal("1.5"),
        2.5,
        Decimal("3.5"),
        {"value": Decimal("4.5")},
    ]

    result = AnomalyModelRegistry.convert_decimals(input_list)

    assert isinstance(result, list)
    assert len(result) == 4
    assert isinstance(result[0], float)
    assert isinstance(result[1], float)
    assert isinstance(result[2], float)
    assert result[0] == 1.5
    assert result[1] == 2.5
    assert result[2] == 3.5
    assert isinstance(result[3]["value"], float)
    assert result[3]["value"] == 4.5


def test_convert_decimals_preserves_non_decimal_types():
    """Test that non-Decimal types are preserved during conversion."""
    input_dict = {
        "string_value": "test",
        "int_value": 42,
        "float_value": 3.14,
        "bool_value": True,
        "none_value": None,
        "list_value": [1, 2, 3],
    }

    result = AnomalyModelRegistry.convert_decimals(input_dict)

    assert result["string_value"] == "test"
    assert result["int_value"] == 42
    assert result["float_value"] == 3.14
    assert result["bool_value"] is True
    assert result["none_value"] is None
    assert result["list_value"] == [1, 2, 3]


def test_anomaly_model_record_to_dict():
    """Test converting AnomalyModelRecord to dictionary."""
    record = AnomalyModelRecord(
        model_name="test_model",
        model_uri="models:/test_model/1",
        columns=["col1", "col2"],
        algorithm="IsolationForest",
        hyperparameters={"contamination": "0.1"},
        training_rows=1000,
        training_time=datetime(2024, 1, 1, 12, 0, 0),
        mlflow_run_id="run123",
        metrics={"precision": 0.85},
    )

    record_dict = record.__dict__

    assert isinstance(record_dict, dict)
    assert record_dict["model_name"] == "test_model"
    assert record_dict["model_uri"] == "models:/test_model/1"
    assert record_dict["columns"] == ["col1", "col2"]
    assert record_dict["algorithm"] == "IsolationForest"
    assert record_dict["training_rows"] == 1000
    assert "hyperparameters" in record_dict
    assert "metrics" in record_dict


def test_anomaly_model_record_defaults_for_optional_fields():
    """Test that optional fields have correct None defaults."""
    record = AnomalyModelRecord(
        model_name="minimal_model",
        model_uri="models:/minimal/1",
        columns=["col1"],
        algorithm="IF",
        hyperparameters={},
        training_rows=100,
        training_time=datetime.now(),
        mlflow_run_id="run1",
    )

    # These should all be None by default
    assert record.metrics is None
    assert record.baseline_stats is None
    assert record.feature_importance is None
    assert record.temporal_config is None
    assert record.segment_by is None
    assert record.segment_values is None
    assert record.column_types is None
    assert record.feature_metadata is None


def test_anomaly_model_record_with_empty_collections():
    """Test AnomalyModelRecord with empty collections instead of None."""
    record = AnomalyModelRecord(
        model_name="empty_model",
        model_uri="models:/empty/1",
        columns=[],  # Empty list
        algorithm="IF",
        hyperparameters={},  # Empty dict
        training_rows=0,
        training_time=datetime.now(),
        mlflow_run_id="run1",
        metrics={},  # Empty dict
        baseline_stats={},  # Empty dict
    )

    assert not record.columns
    assert not record.hyperparameters
    assert record.metrics == {}
    assert record.baseline_stats == {}
