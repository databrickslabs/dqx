---
sidebar_label: core
title: databricks.labs.dqx.anomaly.core
---

Core ML operations for row anomaly detection.

Contains the fundamental building blocks for training and scoring:
- Feature engineering (prepare_training_features, prepare_engineered_pandas)
- Model training (fit_sklearn_model, fit_isolation_forest)
- Scoring (score_with_model, score_with_ensemble_models)
- Metrics computation (compute_validation_metrics, compute_score_quantiles)
- Baseline statistics (compute_baseline_statistics)

All functions work with distributed Spark DataFrames and sklearn models.
MLflow model registration is handled by mlflow_registry.py.

#### sample\_df

```python
def sample_df(df: DataFrame, columns: list[str],
              params: AnomalyParams) -> tuple[DataFrame, int, bool]
```

Sample DataFrame for training.

**Arguments**:

- `df` - Input DataFrame
- `columns` - Columns to include in sample
- `params` - Training parameters with sample_fraction and max_rows
  

**Returns**:

  Tuple of (sampled DataFrame, row count, truncated flag)

#### train\_validation\_split

```python
def train_validation_split(
        df: DataFrame, params: AnomalyParams) -> tuple[DataFrame, DataFrame]
```

Split DataFrame into training and validation sets.

#### prepare\_training\_features

```python
def prepare_training_features(
        train_df: DataFrame, feature_columns: list[str],
        params: AnomalyParams) -> tuple[pd.DataFrame, SparkFeatureMetadata]
```

Prepare training features using Spark-based feature engineering.

Analyzes column types, applies transformations (one-hot encoding, frequency maps, etc.),
and collects the result to the driver as a pandas DataFrame.

**Returns**:

  - train_pandas: pandas DataFrame with engineered numeric features
  - feature_metadata: Transformation metadata for distributed scoring

#### fit\_sklearn\_model

```python
def fit_sklearn_model(
        train_pandas: pd.DataFrame,
        params: AnomalyParams) -> tuple[Pipeline, dict[str, Any]]
```

Train sklearn IsolationForest pipeline on pre-engineered pandas DataFrame.

**Returns**:

  - pipeline: sklearn Pipeline (RobustScaler + IsolationForest)
  - hyperparams: Model configuration for MLflow tracking

#### fit\_isolation\_forest

```python
def fit_isolation_forest(
    train_df: DataFrame, feature_columns: list[str], params: AnomalyParams
) -> tuple[Pipeline, dict[str, Any], SparkFeatureMetadata]
```

Train IsolationForest model with distributed feature engineering.

Feature engineering runs on Spark, then the model trains on the driver.

**Returns**:

  - pipeline: sklearn Pipeline (RobustScaler + IsolationForest)
  - hyperparams: Model configuration for MLflow tracking
  - feature_metadata: Transformation metadata for distributed scoring

#### score\_with\_model

```python
def score_with_model(model: Pipeline, df: DataFrame, feature_cols: list[str],
                     feature_metadata: SparkFeatureMetadata) -> DataFrame
```

Score DataFrame using scikit-learn model with distributed pandas UDF.

Feature engineering is applied in Spark before the pandas UDF.
This enables distributed inference across the Spark cluster.

#### score\_with\_ensemble\_models

```python
def score_with_ensemble_models(
        models: list[Pipeline], df: DataFrame, feature_cols: list[str],
        feature_metadata: SparkFeatureMetadata) -> DataFrame
```

Score DataFrame using an ensemble of models and return mean scores.

#### compute\_validation\_metrics

```python
def compute_validation_metrics(
        model: Pipeline, val_df: DataFrame, feature_cols: list[str],
        feature_metadata: SparkFeatureMetadata) -> dict[str, float]
```

Compute validation metrics and distribution statistics.

#### compute\_score\_quantiles

```python
def compute_score_quantiles(
        model: Pipeline, df: DataFrame, feature_cols: list[str],
        feature_metadata: SparkFeatureMetadata) -> dict[str, float]
```

Compute score quantiles from the training score distribution.

#### compute\_score\_quantiles\_ensemble

```python
def compute_score_quantiles_ensemble(
        models: list[Pipeline], df: DataFrame, feature_cols: list[str],
        feature_metadata: SparkFeatureMetadata) -> dict[str, float]
```

Compute score quantiles using ensemble mean scores.

#### compute\_baseline\_statistics

```python
def compute_baseline_statistics(
        train_df: DataFrame,
        columns: list[str]) -> dict[str, dict[str, float]]
```

Compute baseline distribution statistics for drift detection.

**Arguments**:

- `train_df` - Training DataFrame
- `columns` - Feature columns to compute statistics for
  

**Returns**:

  Dictionary mapping column names to their baseline statistics

#### aggregate\_ensemble\_metrics

```python
def aggregate_ensemble_metrics(
        all_metrics: list[dict[str, float]]) -> dict[str, float]
```

Aggregate metrics across ensemble members (mean and std).

**Arguments**:

- `all_metrics` - List of metric dictionaries, one per ensemble member
  

**Returns**:

  Dictionary with aggregated metrics (mean and std for each metric)

#### prepare\_engineered\_pandas

```python
def prepare_engineered_pandas(
        train_df: DataFrame,
        feature_metadata: SparkFeatureMetadata) -> pd.DataFrame
```

Prepare engineered pandas DataFrame from Spark DataFrame.

Applies feature engineering transformations and collects to pandas.
Used for MLflow signature inference.

**Arguments**:

- `train_df` - Training Spark DataFrame
- `feature_metadata` - Feature engineering metadata from training
  

**Returns**:

  Pandas DataFrame with engineered features

