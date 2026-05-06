# databricks.labs.dqx.anomaly.ensemble\_scorer

Ensemble anomaly scoring (distributed UDF and driver-local).

### serialize\_ensemble\_models[​](#serialize_ensemble_models "Direct link to serialize_ensemble_models")

```python
def serialize_ensemble_models(model_uris: list[str],
                              model_record: AnomalyModelRecord) -> list[bytes]

```

Load and serialize ensemble models for UDF.

### prepare\_ensemble\_scoring\_schema[​](#prepare_ensemble_scoring_schema "Direct link to prepare_ensemble_scoring_schema")

```python
def prepare_ensemble_scoring_schema(enable_contributions: bool) -> StructType

```

Prepare schema for ensemble scoring UDF.

### join\_ensemble\_scores[​](#join_ensemble_scores "Direct link to join_ensemble_scores")

```python
def join_ensemble_scores(df_filtered: DataFrame, scored_df: DataFrame,
                         merge_columns: list[str],
                         enable_contributions: bool) -> DataFrame

```

Join scores back to original DataFrame.

### create\_ensemble\_scoring\_udf[​](#create_ensemble_scoring_udf "Direct link to create_ensemble_scoring_udf")

```python
def create_ensemble_scoring_udf(models_bytes: list[bytes],
                                engineered_feature_cols: list[str],
                                schema: StructType)

```

Create ensemble scoring UDF.

### create\_ensemble\_scoring\_udf\_with\_contributions[​](#create_ensemble_scoring_udf_with_contributions "Direct link to create_ensemble_scoring_udf_with_contributions")

```python
def create_ensemble_scoring_udf_with_contributions(
        models_bytes: list[bytes], engineered_feature_cols: list[str],
        schema: StructType)

```

Create ensemble scoring UDF with SHAP contributions.

### score\_ensemble\_models[​](#score_ensemble_models "Direct link to score_ensemble_models")

```python
def score_ensemble_models(model_uris: list[str], df_filtered: DataFrame,
                          columns: list[str], feature_metadata_json: str,
                          merge_columns: list[str], enable_contributions: bool,
                          *, model_record: AnomalyModelRecord) -> DataFrame

```

Score DataFrame with multiple ensemble models and compute statistics.

### score\_ensemble\_models\_local[​](#score_ensemble_models_local "Direct link to score_ensemble_models_local")

```python
def score_ensemble_models_local(model_uris: list[str], df_filtered: DataFrame,
                                columns: list[str], feature_metadata_json: str,
                                merge_columns: list[str],
                                enable_contributions: bool, *,
                                model_record: AnomalyModelRecord) -> DataFrame

```

Score ensemble models locally on the driver.
