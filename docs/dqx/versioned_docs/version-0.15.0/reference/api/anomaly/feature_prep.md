---
sidebar_label: feature_prep
title: databricks.labs.dqx.anomaly.feature_prep
---

Prepare feature metadata and apply feature engineering for anomaly scoring.

#### prepare\_feature\_metadata

```python
def prepare_feature_metadata(
    feature_metadata_json: str
) -> tuple[list[ColumnTypeInfo], SparkFeatureMetadata]
```

Load and prepare feature metadata from JSON.

#### apply\_feature\_engineering\_for\_scoring

```python
def apply_feature_engineering_for_scoring(
        df: DataFrame,
        feature_cols: list[str],
        merge_columns: list[str],
        column_infos: list[ColumnTypeInfo],
        feature_metadata: SparkFeatureMetadata,
        passthrough_columns: list[str] | None = None) -> DataFrame
```

Apply feature engineering to DataFrame for scoring.

Note: the internal row identifier must exist in the DataFrame as it is required for
joining results back in row_filter cases. *passthrough_columns* are carried through
the transformation untouched (feature engineering preserves columns it does not know
about).

#### apply\_feature\_engineering\_with\_row\_passthrough

```python
def apply_feature_engineering_with_row_passthrough(
        df: DataFrame, feature_cols: list[str], merge_columns: list[str],
        column_infos: list[ColumnTypeInfo],
        feature_metadata: SparkFeatureMetadata) -> tuple[DataFrame, str]
```

Apply feature engineering while carrying every original column through unchanged.

Feature engineering mutates feature columns in place (imputation, encodings) and drops
some of them (e.g. datetime), so scorers used to re-join scores onto the caller&#x27;s
DataFrame to restore the original rows — recomputing the source a second time and
shuffling on a non-deterministic row id. Instead, pack the pristine original row into a
collision-proof struct column that rides through the transformation untouched; after
scoring, selecting ``&lt;struct&gt;.*`` restores the exact original columns without a join.

**Returns**:

  The engineered DataFrame and the name of the struct column holding the original row.

