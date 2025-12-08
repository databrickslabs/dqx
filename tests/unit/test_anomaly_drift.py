"""Unit tests for drift detection."""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score


def test_no_drift_when_distributions_match(spark: SparkSession):
    """Test that drift is not detected when distributions are identical."""
    df = spark.createDataFrame(
        [(100.0,), (101.0,), (102.0,), (99.0,), (100.5,)],
        "value double",
    )
    
    baseline_stats = {
        "value": {
            "mean": 100.5,
            "std": 1.12,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.5,
            "p75": 101.5,
        }
    }
    
    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)
    
    assert not result.drift_detected
    assert result.drift_score < 3.0
    assert len(result.drifted_columns) == 0


def test_drift_detected_when_mean_shifts(spark: SparkSession):
    """Test that drift is detected when mean shifts significantly."""
    df = spark.createDataFrame(
        [(500.0,), (501.0,), (502.0,), (499.0,), (500.5,)],
        "value double",
    )
    
    baseline_stats = {
        "value": {
            "mean": 100.0,
            "std": 1.0,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.0,
            "p75": 101.0,
        }
    }
    
    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)
    
    assert result.drift_detected
    assert result.drift_score > 3.0
    assert "value" in result.drifted_columns
    assert result.recommendation == "retrain"


def test_drift_with_multiple_columns(spark: SparkSession):
    """Test drift detection with multiple columns."""
    df = spark.createDataFrame(
        [(100.0, 500.0), (101.0, 501.0), (102.0, 502.0)],
        "col1 double, col2 double",
    )
    
    baseline_stats = {
        "col1": {"mean": 100.0, "std": 1.0, "min": 99.0, "max": 102.0, "p25": 99.5, "p50": 100.0, "p75": 101.0},
        "col2": {"mean": 100.0, "std": 1.0, "min": 99.0, "max": 102.0, "p25": 99.5, "p50": 100.0, "p75": 101.0},
    }
    
    result = compute_drift_score(df, ["col1", "col2"], baseline_stats, threshold=3.0)
    
    assert result.drift_detected
    assert "col2" in result.drifted_columns
    assert "col1" not in result.drifted_columns

