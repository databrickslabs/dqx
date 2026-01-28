"""
Explainability utilities for anomaly detection.

Provides TreeSHAP-based feature contribution analysis to understand which columns
contribute most to anomaly scores for individual records.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import pandas_udf

logger = logging.getLogger(__name__)

# Optional dependencies for anomaly detection explainability
try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None  # type: ignore
    np = None  # type: ignore

try:
    import shap
except ImportError:
    shap = None  # type: ignore

try:
    import mlflow.sklearn as mlflow_sklearn
except ImportError:
    mlflow_sklearn = None  # type: ignore

try:
    from sklearn.pipeline import Pipeline
except ImportError:
    Pipeline = None  # type: ignore


def create_optimal_tree_explainer(tree_model: Any) -> Any:
    """Create TreeSHAP explainer for the given tree model.

    Uses SHAP's TreeExplainer, which provides efficient SHAP value computation
    for tree-based models via optimized C++ implementations.

    Args:
        tree_model (Any): Trained tree-based model (e.g., IsolationForest)

    Returns:
        Any: Configured SHAP TreeExplainer

    Raises:
        ImportError: If SHAP library is not installed
    """
    if shap is None:
        raise ImportError("SHAP library required for explainability")

    return shap.TreeExplainer(tree_model)


def compute_contributions_for_matrix(
    model_local: Any, feature_matrix: "np.ndarray", columns: list[str]
) -> list[dict[str, float | None]]:
    """Compute normalized SHAP contributions for a feature matrix."""
    if pd is None or np is None or shap is None or Pipeline is None:
        raise ImportError("Explainability dependencies are not available.")

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
    # Validate that required dependencies are available
    if pd is None or np is None:
        raise ImportError(
            "pandas and numpy are required for feature contributions. "
            "Install with: pip install 'databricks-labs-dqx[anomaly]'"
        )

    if shap is None or mlflow_sklearn is None or Pipeline is None:
        raise ImportError(
            "To use feature contributions (include_contributions=True), install 'shap', 'mlflow', and 'scikit-learn'.\n"
            "Install with: pip install 'databricks-labs-dqx[anomaly]' or manually install shap>=0.42.0,<0.46"
        )

    # Define pandas UDF for distributed SHAP computation
    return_schema = StructType([StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)])

    @pandas_udf(return_schema)  # type: ignore[call-overload]  # StructType is valid but mypy has incomplete stubs
    def compute_shap_udf(feature_struct: pd.Series) -> pd.DataFrame:
        """Compute SHAP values for each row using TreeExplainer.

        Args:
            feature_struct: Pandas Series containing struct with all feature columns

        Returns:
            DataFrame with anomaly_contributions column
        """
        import pandas as pd
        import mlflow.sklearn as mlflow_sklearn

        # Load model once per executor
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

    def format_contributions(contributions_map):
        """Format contributions map as string for top N contributors."""
        if not contributions_map:
            return ""

        # Sort by absolute contribution value (descending) to rank by impact magnitude
        sorted_contribs = sorted(
            contributions_map.items(), key=lambda x: abs(x[1]) if x[1] is not None else 0.0, reverse=True
        )

        # Take top N
        top_contribs = sorted_contribs[:top_n]

        # Format as string: "amount (85%), quantity (10%), discount (5%)"
        parts = [f"{col} ({val*100:.0f}%)" for col, val in top_contribs if val is not None]
        return ", ".join(parts)

    format_udf = F.udf(format_contributions, StringType())

    return df.withColumn(
        "_top_contributors",
        F.when(F.col("anomaly_score") >= threshold, format_udf(F.col("anomaly_contributions"))).otherwise(F.lit("")),
    )
