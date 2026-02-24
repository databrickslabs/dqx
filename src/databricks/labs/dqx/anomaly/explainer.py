"""
Explainability utilities for anomaly detection.

Provides TreeSHAP-based feature contribution analysis to understand which columns
contribute most to anomaly scores for individual records.

Requires the 'anomaly' extras: pip install databricks-labs-dqx[anomaly]
"""

import logging
from typing import Any

import mlflow.sklearn as mlflow_sklearn
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import shap
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, MapType, StringType, StructField, StructType
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)


def format_contributions_map(contributions_map: dict[str, float | None] | None, top_n: int) -> str:
    """Format contributions map as string for top N contributors.

    Args:
        contributions_map: Dictionary mapping feature names to contribution values (0-100 range)
        top_n: Number of top contributors to include

    Returns:
        Formatted string like "amount (85%), quantity (10%), discount (5%)"
        Empty string if contributions_map is None or empty

    Example:
        >>> format_contributions_map(dict(amount=85.0, quantity=10.0), 2)
        'amount (85%), quantity (10%)'
    """
    if not contributions_map:
        return ""

    # Sort by absolute contribution value (descending) to rank by impact magnitude
    sorted_contribs = sorted(
        contributions_map.items(), key=lambda x: abs(x[1]) if x[1] is not None else 0.0, reverse=True
    )

    # Take top N
    top_contribs = sorted_contribs[:top_n]

    # Format as string: "amount (85%), quantity (10%), discount (5%)"
    parts = [f"{col} ({val:.0f}%)" for col, val in top_contribs if val is not None]
    return ", ".join(parts)


def create_optimal_tree_explainer(tree_model: Any) -> shap.TreeExplainer:
    """Create TreeSHAP explainer for the given tree model.

    Uses SHAP's TreeExplainer, which provides efficient SHAP value computation
    for tree-based models via optimized C++ implementations.

    Args:
        tree_model: Trained tree-based model (e.g., IsolationForest)

    Returns:
        Configured SHAP TreeExplainer
    """
    return shap.TreeExplainer(tree_model)


def compute_contributions_for_matrix(
    model_local: Any, feature_matrix: np.ndarray, columns: list[str]
) -> list[dict[str, float | None]]:
    """Compute normalized SHAP contributions for a feature matrix."""
    # If model is a Pipeline (due to feature scaling), extract components
    # SHAP's TreeExplainer only supports tree models, not pipelines
    if isinstance(model_local, Pipeline):
        scaler = model_local.named_steps["scaler"]
        tree_model = model_local.named_steps["model"]
        needs_scaling = True
    else:
        scaler = None
        tree_model = model_local
        needs_scaling = False

    explainer = shap.TreeExplainer(tree_model)

    # Scale the data if the model uses a scaler
    if needs_scaling:
        feature_matrix = scaler.transform(feature_matrix)

    # Handle NaN values (SHAP can't process them)
    has_nan = pd.isna(feature_matrix).any(axis=1)

    contributions_list: list[dict[str, float | None]] = []
    for i in range(len(feature_matrix)):
        if has_nan[i]:
            contributions_list.append({col: None for col in columns})
            continue

        shap_values = explainer.shap_values(feature_matrix[i : i + 1])[0]
        abs_shap = np.abs(shap_values)
        total = abs_shap.sum()

        if total > 0:
            normalized = abs_shap / total
            contributions: dict[str, float | None] = {col: float(normalized[j]) for j, col in enumerate(columns)}
        else:
            contributions = {col: 1.0 / len(columns) for col in columns}

        contributions_list.append(contributions)

    return contributions_list


def compute_feature_contributions(
    model_uri: str,
    df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """
    Compute per-row feature contributions using TreeSHAP.

    TreeSHAP provides exact feature attributions from the IsolationForest model,
    showing which features contributed most to each anomaly score.

    Args:
        model_uri: MLflow model URI to load sklearn IsolationForest.
        df: DataFrame with data to explain.
        columns: Feature columns used for training.

    Returns:
        DataFrame with additional 'anomaly_contributions' map column containing
        normalized SHAP values (absolute contributions summing to 1.0 per row).
    """
    return_schema = StructType([StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)])

    @pandas_udf(return_schema)  # type: ignore[call-overload]
    def compute_shap_udf(feature_struct: pd.Series) -> pd.DataFrame:
        """Compute SHAP values for each row using TreeExplainer."""
        model_local = mlflow_sklearn.load_model(model_uri)

        # feature_struct is already a DataFrame with struct fields as columns
        feature_matrix = feature_struct.values
        contributions_list = compute_contributions_for_matrix(model_local, feature_matrix, columns)

        # Return as a DataFrame so the StructType schema is satisfied
        return pd.DataFrame({"anomaly_contributions": contributions_list})

    # Combine feature columns into struct, then apply UDF
    result = df.withColumn("anomaly_contributions", compute_shap_udf(F.struct(*[F.col(c) for c in columns])))

    return result


def add_top_contributors_to_message(df: DataFrame, threshold: float, top_n: int = 3) -> DataFrame:
    """
    Enhance error messages with top feature contributors from SHAP values.

    Args:
        df: DataFrame with anomaly_score and anomaly_contributions.
        threshold: Score threshold for anomalies.
        top_n: Number of top contributors to include in message.

    Returns:
        DataFrame with enhanced messages including top contributing features.
    """
    format_udf = F.udf(lambda m: format_contributions_map(m, top_n), StringType())

    if "severity_percentile" in df.columns:
        severity_col = F.col("severity_percentile")
    elif "_dq_info" in df.columns:
        severity_col = F.col("_dq_info").anomaly.severity_percentile
    else:
        raise InvalidParameterError(
            "severity_percentile is required to determine top contributors. "
            "Ensure scoring adds severity_percentile before calling this helper."
        )

    return df.withColumn(
        "_top_contributors",
        F.when(severity_col >= threshold, format_udf(F.col("anomaly_contributions"))).otherwise(F.lit("")),
    )
