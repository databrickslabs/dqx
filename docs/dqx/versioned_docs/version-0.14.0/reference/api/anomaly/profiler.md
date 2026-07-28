---
sidebar_label: profiler
title: databricks.labs.dqx.anomaly.profiler
---

Auto-discovery logic for row anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for
anomaly detection using on-the-fly heuristics.

## AnomalyProfile Objects

```python
@dataclass
class AnomalyProfile()
```

Auto-discovery results for row anomaly detection.

#### column\_types

NEW: maps column -&gt; type category

#### unsupported\_columns

NEW: columns that cannot be used

#### auto\_discover\_columns

```python
def auto_discover_columns(df: DataFrame) -> AnomalyProfile
```

Auto-discover columns and segments for row anomaly detection.

Analyzes the DataFrame using on-the-fly heuristics to recommend
suitable columns and segmentation strategy.

Column selection criteria:
- Numeric types (int, long, float, double, decimal)
- stddev &gt; 0 (has variance)
- null_rate &lt; 50%
- Exclude: timestamps, IDs (detected by name patterns)

Segment selection criteria:
- Categorical types (string, int with low cardinality)
- Distinct values: 2-50 (inclusive)
- null_rate &lt; 10%
- At least 1000 rows per segment (warn if violated)

**Arguments**:

- `df` - DataFrame to analyze.
  

**Returns**:

  AnomalyProfile with recommendations and warnings.

