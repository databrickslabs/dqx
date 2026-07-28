---
sidebar_label: ensemble_scorer
title: databricks.labs.dqx.anomaly.ensemble_scorer
---

Ensemble anomaly scoring (distributed UDF and driver-local).

#### serialize\_ensemble\_models

```python
def serialize_ensemble_models(model_uris: list[str],
                              model_record: AnomalyModelRecord) -> list[bytes]
```

Load and serialize ensemble models for UDF.

#### prepare\_ensemble\_scoring\_schema

```python
def prepare_ensemble_scoring_schema(enable_contributions: bool) -> StructType
```

Prepare schema for ensemble scoring UDF.

#### create\_ensemble\_scoring\_udf

```python
def create_ensemble_scoring_udf(models_bytes: list[bytes],
                                engineered_feature_cols: list[str],
                                schema: StructType)
```

Create ensemble scoring UDF.

#### create\_ensemble\_scoring\_udf\_with\_contributions

```python
def create_ensemble_scoring_udf_with_contributions(
        models_bytes: list[bytes],
        engineered_feature_cols: list[str],
        schema: StructType,
        quantile_points: list[tuple[float, float]] | None = None,
        threshold: float | None = None)
```

Create ensemble scoring UDF with SHAP contributions.

When *quantile_points* and *threshold* are provided, SHAP runs only for rows whose
mean-score severity reaches the threshold; other rows get a null contributions map.

#### score\_ensemble\_models

```python
def score_ensemble_models(model_uris: list[str],
                          df_filtered: DataFrame,
                          columns: list[str],
                          feature_metadata_json: str,
                          merge_columns: list[str],
                          enable_contributions: bool,
                          *,
                          model_record: AnomalyModelRecord,
                          quantile_points: list[tuple[float, float]]
                          | None = None,
                          threshold: float | None = None) -> DataFrame
```

Score DataFrame with multiple ensemble models and compute statistics.

The original row rides through feature engineering inside a struct column and is
restored after scoring, so scores are attached in the same pass — no join back onto
the caller&#x27;s DataFrame.

#### score\_ensemble\_models\_local

```python
def score_ensemble_models_local(model_uris: list[str],
                                df_filtered: DataFrame,
                                columns: list[str],
                                feature_metadata_json: str,
                                merge_columns: list[str],
                                enable_contributions: bool,
                                *,
                                model_record: AnomalyModelRecord,
                                quantile_points: list[tuple[float, float]]
                                | None = None,
                                threshold: float | None = None) -> DataFrame
```

Score ensemble models locally on the driver.

