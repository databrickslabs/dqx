"""Pure test helpers for anomaly integration tests (no pytest fixtures)."""

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.rule import DQDatasetRule
from tests.integration_anomaly.constants import DEFAULT_SCORE_THRESHOLD, OUTLIER_AMOUNT, OUTLIER_QUANTITY
from tests.integration_anomaly.test_helpers_data import (
    get_standard_2d_training_data,
    get_standard_3d_training_data,
    get_standard_4d_training_data,
    get_standard_test_points_2d,
    get_standard_test_points_4d,
    get_standard_training_ranges,
    qualify_model_name,
)


def _normalize_anomaly_apply_fn(apply_fn, info_col: str):
    """Wrap apply_fn so the result DataFrame exposes _dq_info for direct-access tests.

    When used through DQEngine the engine merges info columns into an array; this wrapper
    ensures _dq_info is always array<struct<...>> (one element when a single check runs)
    so tests can use row["_dq_info"][0]["anomaly"]["contributions"] etc.
    """

    def normalized(df: DataFrame) -> DataFrame:
        result = apply_fn(df)
        if info_col not in result.columns:
            return result
        # Single check produces one struct; wrap in array to match engine/merge contract
        return result.withColumn("_dq_info", F.array(F.col(info_col))).drop(info_col)

    return normalized


def create_anomaly_apply_fn(
    model_name: str,
    registry_table: str,
    *,
    driver_only: bool = True,
    **check_kwargs,
):
    """Create apply function from has_no_row_anomalies check. Default driver_only=True for tests."""
    _, apply_fn, info_col = has_no_row_anomalies(
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        driver_only=driver_only,
        **check_kwargs,
    )
    return _normalize_anomaly_apply_fn(apply_fn, info_col)


def train_model_with_params(
    engine: AnomalyEngine,
    df: DataFrame,
    model_name: str,
    registry_table: str,
    columns: list[str],
    params: AnomalyParams,
    segment_by: list[str] | None = None,
    expected_anomaly_rate: float = 0.02,
) -> str:
    """Train a model with internal params (test-only)."""
    return engine.train(
        df=df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        segment_by=segment_by,
        params=params,
        expected_anomaly_rate=expected_anomaly_rate,
    )


def get_percentile_threshold_from_data(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    percentile: float = 0.95,
) -> float:
    """Return a fixed severity percentile threshold (0–100) for test stability.

    Does not derive from data; accepts df/model_name/registry_table for API
    compatibility with call sites that pass them.
    """
    _ = (df, model_name, registry_table)
    return float(percentile * 100.0)


def create_anomaly_check_rule(
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
    criticality: str = "error",
    *,
    driver_only: bool = True,
    **kwargs: Any,
) -> DQDatasetRule:
    """Create a standard DQDatasetRule for anomaly detection. Default driver_only=True for tests."""
    check_kwargs = {
        "model_name": qualify_model_name(model_name, registry_table),
        "registry_table": registry_table,
        "threshold": threshold,
        "driver_only": driver_only,
    }
    check_kwargs.update(kwargs)
    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_row_anomalies,
        check_func_kwargs=check_kwargs,
    )


def apply_anomaly_check_direct(
    test_df: Any,
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
    *,
    driver_only: bool = True,
    **kwargs: Any,
) -> Any:
    """Apply anomaly detection directly (without DQEngine) to get anomaly_score column."""
    _, apply_fn, info_col = has_no_row_anomalies(
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=threshold,
        driver_only=driver_only,
        **kwargs,
    )
    result_df = apply_fn(test_df)
    return result_df.withColumn("anomaly_score", F.col(f"{info_col}.anomaly.score")).withColumn(
        "severity_percentile", F.col(f"{info_col}.anomaly.severity_percentile")
    )


def train_simple_2d_model(
    spark: SparkSession,
    engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    train_size: int = 50,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
) -> None:
    """Train a simple 2D model with standard amount/quantity columns."""
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0) for i in range(train_size)]
    train_df = spark.createDataFrame(train_data, "amount double, quantity double")
    full_model_name = qualify_model_name(model_name, registry_table)
    if params is None:
        engine.train(
            df=train_df,
            columns=["amount", "quantity"],
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return
    train_model_with_params(
        engine=engine,
        df=train_df,
        columns=["amount", "quantity"],
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def train_simple_3d_model(
    spark: SparkSession,
    engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    train_size: int = 50,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
) -> None:
    """Train a simple 3D model with amount, quantity, discount columns."""
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0, 0.1) for i in range(train_size)]
    train_df = spark.createDataFrame(train_data, "amount double, quantity double, discount double")
    full_model_name = qualify_model_name(model_name, registry_table)
    if params is None:
        engine.train(
            df=train_df,
            columns=["amount", "quantity", "discount"],
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return
    train_model_with_params(
        engine=engine,
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def train_large_dataset_model(
    spark: SparkSession,
    engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    num_rows: int,
    columns: list[str] | None = None,
    params: AnomalyParams | None = None,
) -> None:
    """Train a model on large synthetic dataset using spark.range()."""
    if columns is None:
        columns = ["amount", "quantity"]
    train_df = spark.range(num_rows).selectExpr("cast(id as double) as amount", "2.0 as quantity")
    full_model_name = qualify_model_name(model_name, registry_table)
    if params is None:
        engine.train(
            df=train_df,
            columns=columns,
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return
    train_model_with_params(
        engine=engine,
        df=train_df,
        columns=columns,
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def score_with_anomaly_check(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
) -> DataFrame:
    """Score a DataFrame using has_no_row_anomalies and collect results."""
    apply_fn = create_anomaly_apply_fn(
        model_name=model_name,
        registry_table=registry_table,
        threshold=threshold,
    )
    result_df = apply_fn(df)
    result_df.collect()
    return result_df


def score_3d_with_contributions(
    spark: SparkSession,
    df_factory,
    scorer,
    model_name: str,
    registry_table: str,
    normal_rows: list[tuple[float, float, float]] | None = None,
    anomaly_rows: list[tuple[float, float, float]] | None = None,
    enable_confidence_std: bool = False,
) -> DataFrame:
    """Score a 3D dataset with contributions enabled (optionally confidence)."""
    test_df = df_factory(
        spark,
        normal_rows=normal_rows or [],
        anomaly_rows=anomaly_rows or [(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )
    return scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_confidence_std=enable_confidence_std,
        extract_score=False,
    )


def create_anomaly_dataset_rule(
    model_name: str,
    registry_table: str,
    criticality: str = "error",
    threshold: float = 60.0,
    *,
    driver_only: bool = True,
    **kwargs: Any,
) -> DQDatasetRule:
    """Create DQDatasetRule for anomaly detection. Default driver_only=True for tests."""
    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_row_anomalies,
        check_func_kwargs={
            "model_name": qualify_model_name(model_name, registry_table),
            "registry_table": registry_table,
            "threshold": threshold,
            "driver_only": driver_only,
            **kwargs,
        },
    )
