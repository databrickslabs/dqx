# databricks.labs.dqx.anomaly.explainability

SHAP-based explainability for row anomaly detection.

Provides contribution formatting and computation for scoring pipelines, plus TreeSHAP-based feature contribution analysis for reporting and messages. Requires the 'anomaly' extras: pip install databricks-labs-dqx\[anomaly]

### format\_shap\_contributions[​](#format_shap_contributions "Direct link to format_shap_contributions")

```python
def format_shap_contributions(
        shap_values: np.ndarray, valid_indices: np.ndarray, num_rows: int,
        engineered_feature_cols: list[str]) -> list[dict[str, float | None]]

```

Format SHAP values into contribution dictionaries.

### compute\_shap\_values[​](#compute_shap_values "Direct link to compute_shap_values")

```python
def compute_shap_values(
        model_local: Any, feature_matrix: pd.DataFrame,
        engineered_feature_cols: list[str]) -> tuple[np.ndarray, np.ndarray]

```

Compute SHAP values for a model and feature matrix.

### format\_contributions\_map[​](#format_contributions_map "Direct link to format_contributions_map")

```python
def format_contributions_map(contributions_map: dict[str, float | None] | None,
                             top_n: int) -> str

```

Format contributions map as string for top N contributors.

**Arguments**:

* `contributions_map` - Dictionary mapping feature names to contribution values (0-100 range)
* `top_n` - Number of top contributors to include

**Returns**:

Formatted string like "amount (85%), quantity (10%), discount (5%)" Empty string if contributions\_map is None or empty

**Example**:

\>>> format\_contributions\_map(dict(amount=85.0, quantity=10.0), 2) 'amount (85%), quantity (10%)'

### create\_optimal\_tree\_explainer[​](#create_optimal_tree_explainer "Direct link to create_optimal_tree_explainer")

```python
def create_optimal_tree_explainer(tree_model: Any) -> Any

```

Create TreeSHAP explainer for the given tree model.

Uses SHAP's TreeExplainer, which provides efficient SHAP value computation for tree-based models via optimized C++ implementations.

**Arguments**:

* `tree_model` - Trained tree-based model (e.g., IsolationForest)

**Returns**:

Configured SHAP TreeExplainer

### compute\_contributions\_for\_matrix[​](#compute_contributions_for_matrix "Direct link to compute_contributions_for_matrix")

```python
def compute_contributions_for_matrix(
        model_local: Any, feature_matrix: np.ndarray,
        columns: list[str]) -> list[dict[str, float | None]]

```

Compute normalized SHAP contributions for a feature matrix.

### compute\_feature\_contributions[​](#compute_feature_contributions "Direct link to compute_feature_contributions")

```python
def compute_feature_contributions(model_uri: str, df: DataFrame,
                                  columns: list[str]) -> DataFrame

```

Compute per-row feature contributions using TreeSHAP.

TreeSHAP provides exact feature attributions from the IsolationForest model, showing which features contributed most to each anomaly score.

**Arguments**:

* `model_uri` - MLflow model URI to load sklearn IsolationForest.
* `df` - DataFrame with data to explain.
* `columns` - Feature columns used for training.

**Returns**:

DataFrame with additional 'anomaly\_contributions' map column containing normalized SHAP values (absolute contributions summing to 1.0 per row).

### add\_top\_contributors\_to\_message[​](#add_top_contributors_to_message "Direct link to add_top_contributors_to_message")

```python
def add_top_contributors_to_message(df: DataFrame,
                                    threshold: float,
                                    top_n: int = 3) -> DataFrame

```

Enhance error messages with top feature contributors from SHAP values.

**Arguments**:

* `df` - DataFrame with anomaly\_score and anomaly\_contributions.
* `threshold` - Score threshold for anomalies.
* `top_n` - Number of top contributors to include in message.

**Returns**:

DataFrame with enhanced messages including top contributing features.
