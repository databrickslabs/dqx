# databricks.labs.dqx.anomaly.segment_utils

Segment naming and filtering for row anomaly detection.

### canonicalize_segment_values[​](#canonicalize_segment_values "Direct link to canonicalize_segment_values")

```python
def canonicalize_segment_values(
        segment_values: Mapping[str, Any] | None) -> dict[str, str]

```

Canonicalize segment values for deterministic naming and filtering.

### build_segment_name[​](#build_segment_name "Direct link to build_segment_name")

```python
def build_segment_name(segment_values: Mapping[str, Any] | None) -> str

```

Build deterministic segment name from segment values.

### build_segment_filter[​](#build_segment_filter "Direct link to build_segment_filter")

```python
def build_segment_filter(
        segment_values: dict[str, str] | None) -> Column | None

```

Build Spark filter expression for a segment's values.

**Arguments**:

* `segment_values` - Dictionary mapping segment column names to values

**Returns**:

Spark Column expression combining all segment filters with AND None if segment_values is None or empty

**Example**:

\>>> build_segment_filter(dict(region="US", product="A")) Column<'((region = US) AND (product = A))'> >>> build_segment_filter(None) None
