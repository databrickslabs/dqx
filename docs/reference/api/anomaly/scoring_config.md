# databricks.labs.dqx.anomaly.scoring\_config

Scoring configuration and constants for row anomaly detection.

## ScoringOutputColumns Objects[​](#scoringoutputcolumns-objects "Direct link to ScoringOutputColumns Objects")

```python
@dataclass
class ScoringOutputColumns()

```

Internal output column names produced by anomaly scoring.

## ScoringConfig Objects[​](#scoringconfig-objects "Direct link to ScoringConfig Objects")

```python
@dataclass
class ScoringConfig()

```

Configuration for anomaly scoring.

### drift\_threshold\_value[​](#drift_threshold_value "Direct link to drift_threshold_value")

```python
@property
def drift_threshold_value() -> float

```

Effective drift threshold used by drift computation; falls back to 3.0 when disabled.
