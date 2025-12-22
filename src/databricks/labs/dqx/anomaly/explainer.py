"""
Explainability utilities for anomaly detection.

Provides TreeSHAP-based feature contribution analysis to understand which columns
contribute most to anomaly scores for individual records.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import pandas_udf, PandasUDFType

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

    @pandas_udf(return_schema, PandasUDFType.SCALAR)
    def compute_shap_udf(feature_struct):
        """Compute SHAP values for each row using TreeExplainer.

        Args:
            feature_struct (pd.Series): Pandas Series containing struct with all feature columns

        Returns:
            pd.Series: Series containing map of anomaly_contributions
        """
        # Load model once per executor
        model_local = mlflow_sklearn.load_model(model_uri)

        # If model is a Pipeline (due to feature scaling), extract components
        # SHAP's TreeExplainer only supports tree models, not pipelines
        if isinstance(model_local, Pipeline):
            # Extract the scaler and the actual tree model
            scaler = model_local.named_steps['scaler']
            tree_model = model_local.named_steps['model']
            needs_scaling = True
        else:
            scaler = None
            tree_model = model_local
            needs_scaling = False

        # Create TreeExplainer (fast for tree-based models)
        explainer = shap.TreeExplainer(tree_model)

        # feature_struct is already a DataFrame with struct fields as columns
        feature_matrix = feature_struct.values

        # Scale the data if the model uses a scaler
        # SHAP values are computed in the scaled space to match how the model was trained
        if needs_scaling:
            feature_matrix = scaler.transform(feature_matrix)

        # Handle NaN values (SHAP can't process them)
        has_nan = pd.isna(feature_matrix).any(axis=1)

        # Initialize output
        contributions_list = []

        for i in range(len(feature_matrix)):
            if has_nan[i]:
                # Null contributions for rows with NaN
                contributions_list.append({col: None for col in columns})
            else:
                # Compute SHAP values for this row
                shap_values = explainer.shap_values(feature_matrix[i : i + 1])[0]

                # Convert to absolute contributions normalized to sum to 1.0
                abs_shap = np.abs(shap_values)
                total = abs_shap.sum()

                if total > 0:
                    normalized = abs_shap / total
                    contributions = {col: float(normalized[j]) for j, col in enumerate(columns)}
                else:
                    # Equal contributions if all SHAP values are 0
                    contributions = {col: 1.0 / len(columns) for col in columns}

                contributions_list.append(contributions)

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

        # Sort by contribution value (descending)
        sorted_contribs = sorted(
            contributions_map.items(), key=lambda x: x[1] if x[1] is not None else 0.0, reverse=True
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
