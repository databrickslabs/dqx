# databricks.labs.dqx.anomaly.scoring\_utils

Anomaly scoring helpers: DataFrame/schema builders, row filter, join, reserved column checks.

### create\_null\_scored\_dataframe[​](#create_null_scored_dataframe "Direct link to create_null_scored_dataframe")

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

* `df` - Input DataFrame
* `enable_contributions` - Whether to include null contributions column
* `enable_confidence_std` - Whether to include null confidence/std column
* `score_col` - Name for the score column
* `score_std_col` - Name for the standard deviation column
* `contributions_col` - Name for the contributions column
* `severity_col` - Name for the severity percentile column
* `info_col_name` - Name for the info struct column (collision-safe UUID name expected).

**Returns**:

DataFrame with null anomaly scores and properly structured info column

### add\_info\_column[​](#add_info_column "Direct link to add_info_column")

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

* `df` - Scored DataFrame with anomaly\_score, prediction, etc.
* `model_name` - Name of the model used for scoring.
* `threshold` - Threshold used for row anomaly detection.
* `info_col_name` - Name for the info struct column (collision-safe UUID name expected).
* `segment_values` - Segment values if model is segmented (None for global models).
* `enable_contributions` - Whether anomaly\_contributions are available (0–100 percent).
* `enable_confidence_std` - Whether anomaly\_score\_std is available.
* `score_col` - Column name for anomaly scores (internal, collision-safe).
* `score_std_col` - Column name for ensemble std scores (internal, collision-safe).
* `contributions_col` - Column name for SHAP contributions (internal, collision-safe, 0–100 percent).
* `model_name`0 - Column name for severity percentile (internal, collision-safe).

**Returns**:

DataFrame with info column added.

### add\_severity\_percentile\_column[​](#add_severity_percentile_column "Direct link to add_severity_percentile_column")

```python
def add_severity_percentile_column(
        df: DataFrame, *, score_col: str, severity_col: str,
        quantile_points: list[tuple[float, float]]) -> DataFrame

```

Add a severity percentile column using piecewise linear interpolation.

**Arguments**:

* `df` - DataFrame with anomaly score column.
* `score_col` - Column name containing anomaly scores.
* `severity_col` - Output column name for severity percentile (0–100).
* `quantile_points` - Ordered list of (percentile, score) points.

**Returns**:

DataFrame with severity percentile column added.

### create\_udf\_schema[​](#create_udf_schema "Direct link to create_udf_schema")

```python
def create_udf_schema(enable_contributions: bool) -> StructType

```

Create schema for scoring UDF output.

The anomaly\_score is used internally for populating \_dq\_info (array of structs). After merge, first check's anomaly info is at \_dq\_info\[0].anomaly; check \_dq\_info\[0].anomaly.is\_anomaly for status.

**Arguments**:

* `enable_contributions` - Whether to include contributions field

**Returns**:

StructType schema for the UDF output

### check\_reserved\_row\_id\_columns[​](#check_reserved_row_id_columns "Direct link to check_reserved_row_id_columns")

```python
def check_reserved_row_id_columns(df: DataFrame) -> None

```

Raise if DataFrame has reserved \_dqx\_row\_id / \_\_dqx\_row\_id columns.

### join\_filtered\_results\_back[​](#join_filtered_results_back "Direct link to join_filtered_results_back")

```python
def join_filtered_results_back(df: DataFrame, result: DataFrame,
                               merge_columns: list[str], score_col: str,
                               info_col: str) -> DataFrame

```

Left-join scored result onto df so every input row is preserved.

Rows that were scored get score/info; rows that were not (e.g. filtered out by row\_filter) get null. merge\_columns (e.g. row\_id) must exist on both df and result.

### apply\_row\_filter[​](#apply_row_filter "Direct link to apply_row_filter")

```python
def apply_row_filter(df: DataFrame, row_filter: str | None) -> DataFrame

```

Return only rows that match row\_filter for scoring; if no filter, return df unchanged.

row\_filter is a SQL expression (e.g. "region = 'US'"). Only these rows are run through anomaly detection; elsewhere we join results back so output has same row count.
