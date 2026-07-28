---
sidebar_label: segment_utils
title: databricks.labs.dqx.anomaly.segment_utils
---

Segment naming and filtering for row anomaly detection.

#### canonicalize\_segment\_values

```python
def canonicalize_segment_values(
        segment_values: Mapping[str, Any] | None) -> dict[str, str]
```

Canonicalize segment values for deterministic naming and filtering.

#### build\_segment\_name

```python
def build_segment_name(segment_values: Mapping[str, Any] | None) -> str
```

Build deterministic segment name from segment values.

#### build\_segment\_filter

```python
def build_segment_filter(
        segment_values: dict[str, str] | None) -> Column | None
```

Build Spark filter expression for a segment&#x27;s values.

**Arguments**:

- `segment_values` - Dictionary mapping segment column names to values
  

**Returns**:

  Spark Column expression combining all segment filters with AND
  None if segment_values is None or empty
  

**Example**:

  &gt;&gt;&gt; build_segment_filter(dict(region=&quot;US&quot;, product=&quot;A&quot;))
  Column&lt;&#x27;((region = US) AND (product = A))&#x27;&gt;
  &gt;&gt;&gt; build_segment_filter(None)
  None

