"""Scoring configuration and constants for row anomaly detection."""

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

#: The two percentiles the severity tail is anchored to. Both are keys in SEVERITY_QUANTILE_KEYS above, so
#: the tail stores nothing new and applies to models trained by any earlier release. Severity is exact at
#: both, which is what keeps the default threshold of 95 a fixed point.
TAIL_ANCHOR_PERCENTILE = 95.0
TAIL_RATE_PERCENTILE = 99.0


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
    # Intermediate (segment, pattern) group-key column used only while building AI explanations.
    # UUID-suffixed in production (see check_funcs.has_no_row_anomalies) so it can never collide
    # with a user column; dropped before the scored DataFrame is returned.
    pattern: str = "anomaly_pattern"


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
    enable_contributions: bool = True
    enable_confidence_std: bool = False
    driver_only: bool = False
    enable_ai_explanation: bool = True
    llm_model_config: LLMModelConfig | None = None
    redact_columns: list[str] = field(default_factory=list)
    # Global upper bound on the number of LLM calls per scoring run when
    # *enable_ai_explanation* is True. Anomalous rows are bucketed by their contribution pattern
    # and one LLM call per bucket is made; *max_groups* caps the number of buckets that actually
    # receive an explanation, ranked by ``group_size * group_avg_severity``.
    max_groups: int = 500
    # Whether a row whose group was absent from training counts as a violation. Such a row gets a
    # null score and severity because neither encoder can represent an unseen category honestly,
    # so `severity >= threshold` is null and the verdict has to be chosen rather than computed.
    #
    # Deliberately not exposed on has_no_row_anomalies. "Is this group value one I recognise?" is a
    # set-membership question, and DQX already has foreign_key / is_in_list for exactly that — a
    # flag here would duplicate a better-suited check while pushing the anomaly check past the
    # argument count the project holds itself to. Kept as an internal seam so the behaviour is
    # testable and reachable programmatically; False keeps "could not judge" distinct from
    # "is anomalous", and is_new_baseline reports the fact either way.
    flag_unseen_baseline_as_violation: bool = False
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

    @property
    def pattern_col(self) -> str:
        return self.output_columns.pattern
