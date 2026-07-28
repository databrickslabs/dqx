---
sidebar_label: scoring_utils
title: databricks.labs.dqx.anomaly.scoring_utils
---

Anomaly scoring helpers: DataFrame/schema builders, row filter, join, reserved column checks.

#### create\_null\_scored\_dataframe

```python
def create_null_scored_dataframe(
        df: DataFrame,
        enable_contributions: bool,
        enable_confidence_std: bool = False,
        score_col: str = "anomaly_score",
        score_std_col: str = "anomaly_score_std",
        contributions_col: str = "anomaly_contributions",
        severity_col: str = "severity_percentile",
        info_col_name: str = "_dq_info") -> DataFrame
```

Create a DataFrame with null anomaly scores (for empty segments or filtered rows).

**Arguments**:

- `df` - Input DataFrame
- `enable_contributions` - Whether to include null contributions column
- `enable_confidence_std` - Whether to include null confidence/std column
- `score_col` - Name for the score column
- `score_std_col` - Name for the standard deviation column
- `contributions_col` - Name for the contributions column
- `severity_col` - Name for the severity percentile column
- `info_col_name` - Name for the info struct column (collision-safe UUID name expected).
  

**Returns**:

  DataFrame with null anomaly scores and properly structured info column

#### add\_info\_column

```python
def add_info_column(df: DataFrame,
                    model_name: str,
                    threshold: float,
                    info_col_name: str,
                    segment_values: dict[str, str] | None = None,
                    enable_contributions: bool = False,
                    enable_confidence_std: bool = False,
                    score_col: str = "anomaly_score",
                    score_std_col: str = "anomaly_score_std",
                    contributions_col: str = "anomaly_contributions",
                    severity_col: str = "severity_percentile") -> DataFrame
```

Add info struct column with anomaly metadata.

**Arguments**:

- `df` - Scored DataFrame with anomaly_score, prediction, etc.
- `model_name` - Name of the model used for scoring.
- `threshold` - Threshold used for row anomaly detection.
- `info_col_name` - Name for the info struct column (collision-safe UUID name expected).
- `segment_values` - Segment values if model is segmented (None for global models).
- `enable_contributions` - Whether anomaly_contributions are available (0–100 percent).
- `enable_confidence_std` - Whether anomaly_score_std is available.
- `score_col` - Column name for anomaly scores (internal, collision-safe).
- `score_std_col` - Column name for ensemble std scores (internal, collision-safe).
- `contributions_col` - Column name for SHAP contributions (internal, collision-safe, 0–100 percent).
- `model_name`0 - Column name for severity percentile (internal, collision-safe).
  

**Returns**:

  DataFrame with info column added.

#### add\_severity\_percentile\_column

```python
def add_severity_percentile_column(
        df: DataFrame, *, score_col: str, severity_col: str,
        quantile_points: list[tuple[float, float]]) -> DataFrame
```

Add a severity percentile column using piecewise linear interpolation.

**Arguments**:

- `df` - DataFrame with anomaly score column.
- `score_col` - Column name containing anomaly scores.
- `severity_col` - Output column name for severity percentile (0–100).
- `quantile_points` - Ordered list of (percentile, score) points.
  

**Returns**:

  DataFrame with severity percentile column added.

#### create\_udf\_schema

```python
def create_udf_schema(enable_contributions: bool) -> StructType
```

Create schema for scoring UDF output.

The anomaly_score is used internally for populating _dq_info (array of structs).
After merge, first check&#x27;s anomaly info is at _dq_info[0].anomaly; check _dq_info[0].anomaly.is_anomaly for status.

**Arguments**:

- `enable_contributions` - Whether to include contributions field
  

**Returns**:

  StructType schema for the UDF output

#### check\_reserved\_row\_id\_columns

```python
def check_reserved_row_id_columns(df: DataFrame) -> None
```

Raise if DataFrame has reserved _dqx_row_id / __dqx_row_id columns.

#### join\_filtered\_results\_back

```python
def join_filtered_results_back(df: DataFrame, result: DataFrame,
                               merge_columns: list[str], score_col: str,
                               info_col: str) -> DataFrame
```

Left-join scored result onto df so every input row is preserved.

Rows that were scored get score/info; rows that were not (e.g. filtered out by
row_filter) get null. merge_columns (e.g. row_id) must exist on both df and result.

#### apply\_row\_filter

```python
def apply_row_filter(df: DataFrame, row_filter: str | None) -> DataFrame
```

Return only rows that match row_filter for scoring; if no filter, return df unchanged.

row_filter is a SQL expression (e.g. &quot;region = &#x27;US&#x27;&quot;). Only these rows are run
through anomaly detection; elsewhere we join results back so output has same row count.

