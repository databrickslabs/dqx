---
sidebar_label: single_model_scorer
title: databricks.labs.dqx.anomaly.single_model_scorer
---

Single-model anomaly scoring (distributed UDF and driver-local).

#### create\_scoring\_udf

```python
def create_scoring_udf(model_bytes: bytes, engineered_feature_cols: list[str],
                       schema: StructType)
```

Create pandas UDF for distributed scoring.

#### create\_scoring\_udf\_with\_contributions

```python
def create_scoring_udf_with_contributions(
        model_bytes: bytes,
        engineered_feature_cols: list[str],
        schema: StructType,
        quantile_points: list[tuple[float, float]] | None = None,
        threshold: float | None = None)
```

Create pandas UDF for distributed scoring with SHAP contributions.

When *quantile_points* and *threshold* are provided, SHAP runs only for rows whose
severity reaches the threshold (contributions are only surfaced for anomalous rows);
other rows get a null contributions map.

#### score\_with\_sklearn\_model

```python
def score_with_sklearn_model(model_uri: str,
                             df: DataFrame,
                             feature_cols: list[str],
                             feature_metadata_json: str,
                             merge_columns: list[str],
                             enable_contributions: bool = False,
                             *,
                             model_record: AnomalyModelRecord,
                             quantile_points: list[tuple[float, float]]
                             | None = None,
                             threshold: float | None = None) -> DataFrame
```

Score DataFrame using scikit-learn model with distributed pandas UDF.

The original row rides through feature engineering inside a struct column and is
restored after scoring, so scores are attached in the same pass — no join back onto
the caller&#x27;s DataFrame (which would recompute the source and shuffle on the row id).

#### score\_with\_sklearn\_model\_local

```python
def score_with_sklearn_model_local(
        model_uri: str,
        df: DataFrame,
        feature_cols: list[str],
        feature_metadata_json: str,
        merge_columns: list[str],
        enable_contributions: bool = False,
        *,
        model_record: AnomalyModelRecord,
        quantile_points: list[tuple[float, float]] | None = None,
        threshold: float | None = None) -> DataFrame
```

Score DataFrame using scikit-learn model locally on the driver.

