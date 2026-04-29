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


_DEFAULT_DRIFT_THRESHOLD_VALUE = 3.0


@dataclass
class ScoringOutputColumns:
    """Internal output column names produced by anomaly scoring."""

    score: str = "anomaly_score"
    score_std: str = "anomaly_score_std"
    contributions: str = "anomaly_contributions"
    severity: str = "severity_percentile"
    info: str = DefaultColumnNames.INFO.value
    ai_explanation: str = "ai_explanation"


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
    enable_contributions: bool = False
    enable_confidence_std: bool = False
    segment_by: list[str] | None = None
    driver_only: bool = False
    enable_ai_explanation: bool = False
    llm_model_config: LLMModelConfig | None = None
    redact_columns: list[str] = field(default_factory=list)
    max_groups: int = 500
    output_columns: ScoringOutputColumns = field(default_factory=ScoringOutputColumns)

    @property
    def drift_threshold_value(self) -> float:
        """Effective drift threshold used by drift computation; falls back to 3.0 when disabled."""
        return self.drift_threshold if self.drift_threshold is not None else _DEFAULT_DRIFT_THRESHOLD_VALUE

    @property
    def score_col(self) -> str:
        return self.output_columns.score

    @property
    def score_std_col(self) -> str:
        return self.output_columns.score_std

    @property
    def contributions_col(self) -> str:
        return self.output_columns.contributions

    @property
    def severity_col(self) -> str:
        return self.output_columns.severity

    @property
    def info_col(self) -> str:
        return self.output_columns.info

    @property
    def ai_explanation_col(self) -> str:
        return self.output_columns.ai_explanation
