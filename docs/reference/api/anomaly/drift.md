# databricks.labs.dqx.anomaly.drift

Drift detection and warnings for row anomaly models.

Compares current data distribution against baseline statistics to detect significant changes; provides check/warn helpers for scoring pipelines.

## DriftResult Objects[​](#driftresult-objects "Direct link to DriftResult Objects")

```python
@dataclass
class DriftResult()

```

Results from drift detection.

### compute\_drift\_score[​](#compute_drift_score "Direct link to compute_drift_score")

```python
def compute_drift_score(df: DataFrame,
                        columns: list[str],
                        baseline_stats: dict[str, dict[str, float]],
                        threshold: float = 3.0) -> DriftResult

```

Compute drift score comparing current data to baseline statistics.

**Arguments**:

* `df` - Current DataFrame to check for drift.
* `columns` - Columns to check for drift.
* `baseline_stats` - Baseline statistics from training data.
* `threshold` - Drift score threshold (default 3.0 = 3 std deviations).

**Returns**:

DriftResult with detection status and details.

### prepare\_drift\_df[​](#prepare_drift_df "Direct link to prepare_drift_df")

```python
def prepare_drift_df(
        df: DataFrame, columns: list[str],
        record: AnomalyModelRecord) -> tuple[DataFrame, list[str]]

```

Prepare drift DataFrame and columns aligned to training baseline stats.

### check\_segment\_drift[​](#check_segment_drift "Direct link to check_segment_drift")

```python
def check_segment_drift(segment_df: DataFrame, columns: list[str],
                        segment_model: AnomalyModelRecord,
                        drift_threshold: float | None,
                        drift_threshold_value: float) -> DriftResult | None

```

Check and warn about data drift in a segment. Returns the DriftResult when computed.

### check\_and\_warn\_drift[​](#check_and_warn_drift "Direct link to check_and_warn_drift")

```python
def check_and_warn_drift(df: DataFrame, columns: list[str],
                         record: AnomalyModelRecord, model_name: str,
                         drift_threshold: float | None,
                         drift_threshold_value: float) -> DriftResult | None

```

Check for data drift and issue warning if detected. Returns the DriftResult when computed.

### format\_drift\_summary[​](#format_drift_summary "Direct link to format_drift_summary")

```python
def format_drift_summary(drift_result: "DriftResult | None",
                         redact_columns: Iterable[str] | None = None) -> str

```

Format a DriftResult for inclusion in the LLM prompt.

Returns 'none' when no drift was computed or no drift detected; otherwise a semicolon-separated list of drifted feature names with their drift scores, e.g. 'drift detected: amount=4.12; quantity=3.55'.

Drifted columns listed in *redact\_columns* are excluded from the per-feature breakdown so their names are never sent to the LLM. When every drifted column is redacted, the output collapses to an opaque 'drift detected (N features)' indicator so the prompt still signals drift without leaking names.
