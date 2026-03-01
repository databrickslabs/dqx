"""SHAP contribution formatting and computation for anomaly explainability."""

from typing import Any

import numpy as np
import pandas as pd

try:
    import shap as _shap  # type: ignore

    SHAP = _shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP = None
    SHAP_AVAILABLE = False


def format_shap_contributions(
    shap_values: np.ndarray,
    valid_indices: np.ndarray,
    num_rows: int,
    engineered_feature_cols: list[str],
) -> list[dict[str, float | None]]:
    """Format SHAP values into contribution dictionaries."""
    num_features = len(engineered_feature_cols)
    contributions: list[dict[str, float | None]] = [{c: None for c in engineered_feature_cols} for _ in range(num_rows)]

    if shap_values.size == 0:
        return contributions

    abs_shap = np.abs(shap_values)
    totals = abs_shap.sum(axis=1, keepdims=True)
    normalized = np.divide(abs_shap, totals, out=np.zeros_like(abs_shap), where=totals > 0)
    if num_features > 0:
        normalized[totals.squeeze(axis=1) == 0] = 1.0 / num_features

    valid_row_idx = 0
    for i in range(num_rows):
        if valid_indices[i]:
            contributions[i] = {
                engineered_feature_cols[j]: round(float(normalized[valid_row_idx, j] * 100.0), 1)
                for j in range(num_features)
            }
            valid_row_idx += 1

    return contributions


def compute_shap_values(
    model_local: Any,
    feature_matrix: pd.DataFrame,
    engineered_feature_cols: list[str],
) -> tuple[np.ndarray, np.ndarray]:
    """Compute SHAP values for a model and feature matrix."""
    scaler = getattr(model_local, "named_steps", {}).get("scaler")
    tree_model = getattr(model_local, "named_steps", {}).get("model", model_local)

    shap_data = scaler.transform(feature_matrix) if scaler else feature_matrix.values
    valid_indices = ~pd.isna(shap_data).any(axis=1)

    shap_values = np.array([])
    if valid_indices.any():
        if len(engineered_feature_cols) == 1:
            shap_values = np.ones((len(shap_data[valid_indices]), 1))
        else:
            explainer = SHAP.TreeExplainer(tree_model)
            shap_values = explainer.shap_values(shap_data[valid_indices])

    return shap_values, valid_indices
