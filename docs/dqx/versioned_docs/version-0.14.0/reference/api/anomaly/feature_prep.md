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
        df: DataFrame, feature_cols: list[str], merge_columns: list[str],
        column_infos: list[ColumnTypeInfo],
        feature_metadata: SparkFeatureMetadata) -> DataFrame
```

Apply feature engineering to DataFrame for scoring.

Note: the internal row identifier must exist in the DataFrame as it is required for
joining results back in row_filter cases.

