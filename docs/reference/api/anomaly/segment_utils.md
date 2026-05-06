# databricks.labs.dqx.anomaly.segment\_utils

Segment naming and filtering for row anomaly detection.

### canonicalize\_segment\_values[​](#canonicalize_segment_values "Direct link to canonicalize_segment_values")

```python
def canonicalize_segment_values(
        segment_values: Mapping[str, Any] | None) -> dict[str, str]

```

Canonicalize segment values for deterministic naming and filtering.

### build\_segment\_name[​](#build_segment_name "Direct link to build_segment_name")

```python
def build_segment_name(segment_values: Mapping[str, Any] | None) -> str

```

Build deterministic segment name from segment values.

### build\_segment\_filter[​](#build_segment_filter "Direct link to build_segment_filter")

```python
def build_segment_filter(
        segment_values: dict[str, str] | None) -> Column | None

```

Build Spark filter expression for a segment's values.

**Arguments**:

* `segment_values` - Dictionary mapping segment column names to values

**Returns**:

Spark Column expression combining all segment filters with AND None if segment\_values is None or empty

**Example**:

\>>> build\_segment\_filter(dict(region="US", product="A")) Column<'((region = US) AND (product = A))'> >>> build\_segment\_filter(None) None
