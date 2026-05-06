# databricks.labs.dqx.anomaly.profiler

Auto-discovery logic for row anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for anomaly detection using on-the-fly heuristics.

## AnomalyProfile Objects[​](#anomalyprofile-objects "Direct link to AnomalyProfile Objects")

```python
@dataclass
class AnomalyProfile()

```

Auto-discovery results for row anomaly detection.

#### column\_types[​](#column_types "Direct link to column_types")

NEW: maps column -> type category

#### unsupported\_columns[​](#unsupported_columns "Direct link to unsupported_columns")

NEW: columns that cannot be used

### auto\_discover\_columns[​](#auto_discover_columns "Direct link to auto_discover_columns")

```python
def auto_discover_columns(df: DataFrame) -> AnomalyProfile

```

Auto-discover columns and segments for row anomaly detection.

Analyzes the DataFrame using on-the-fly heuristics to recommend suitable columns and segmentation strategy.

Column selection criteria:

* Numeric types (int, long, float, double, decimal)
* stddev > 0 (has variance)
* null\_rate < 50%
* Exclude: timestamps, IDs (detected by name patterns)

Segment selection criteria:

* Categorical types (string, int with low cardinality)
* Distinct values: 2-50 (inclusive)
* null\_rate < 10%
* At least 1000 rows per segment (warn if violated)

**Arguments**:

* `df` - DataFrame to analyze.

**Returns**:

AnomalyProfile with recommendations and warnings.
