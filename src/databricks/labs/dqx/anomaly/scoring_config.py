"""Scoring configuration and constants for row anomaly detection."""

from __future__ import annotations

from dataclasses import dataclass, field

from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.reporting_columns import DefaultColumnNames

SEVERITY_QUANTILE_KEYS: list[tuple[float, str]] = [
    (0.0, "p00"),
    (1.0, "p01"),
    (5.0, "p05"),
    (10.0, "p10"),
    (25.0, "p25"),
    (50.0, "p50"),
    (75.0, "p75"),
    (90.0, "p90"),
    (95.0, "p95"),
    (99.0, "p99"),
    (100.0, "p100"),
]


@dataclass
class ScoringConfig:
    """Configuration for anomaly scoring."""

    columns: list[str]
    model_name: str
    registry_table: str
    threshold: float
    merge_columns: list[str]
    row_filter: str | None = None
    drift_threshold: float | None = None
    drift_threshold_value: float = 3.0
    enable_contributions: bool = False
    enable_confidence_std: bool = False
    segment_by: list[str] | None = None
    driver_only: bool = False
    enable_ai_explanation: bool = False
    ai_explanation_col: str = "ai_explanation"
    llm_model_config: LLMModelConfig | None = None
    redact_columns: list[str] = field(default_factory=list)
    max_groups: int = 500
    score_col: str = "anomaly_score"
    score_std_col: str = "anomaly_score_std"
    contributions_col: str = "anomaly_contributions"
    severity_col: str = "severity_percentile"
    info_col: str = DefaultColumnNames.INFO.value
