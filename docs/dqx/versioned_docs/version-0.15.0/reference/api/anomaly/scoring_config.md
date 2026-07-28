---
sidebar_label: scoring_config
title: databricks.labs.dqx.anomaly.scoring_config
---

Scoring configuration and constants for row anomaly detection.

## ScoringOutputColumns Objects

```python
@dataclass
class ScoringOutputColumns()
```

Internal output column names produced by anomaly scoring.

## ScoringConfig Objects

```python
@dataclass
class ScoringConfig()
```

Configuration for anomaly scoring.

#### drift\_threshold\_value

```python
@property
def drift_threshold_value() -> float
```

Effective drift threshold used by drift computation; falls back to 3.0 when disabled.

