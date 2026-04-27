"""LLM-based group explanation for row anomaly detection.

The algorithm is group-based: anomalous rows are grouped by a deterministic
(segment, pattern) key — pattern being the sorted top-2 contributing features —
and the LLM is invoked once per group. Every row in a
group shares the same narrative/business_impact/action; group_size and
group_avg_severity signal that the explanation describes a pattern, not a row.

Requires the 'llm' extra: pip install databricks-labs-dqx[anomaly,llm]
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

try:
    import dspy  # type: ignore

    DSPY_AVAILABLE = True
except ImportError:
    dspy = None
    DSPY_AVAILABLE = False

from databricks.labs.dqx.anomaly.anomaly_info_schema import ai_explanation_struct_schema
from databricks.labs.dqx.anomaly.explainability import format_contributions_map
from databricks.labs.dqx.config import LLMModelConfig

if TYPE_CHECKING:
    from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig

logger = logging.getLogger(__name__)

_TOP_N = 5
_PATTERN_COL = "__dqx_pattern"


@dataclass(frozen=True)
class ExplanationContext:
    """Decoupled inputs for the LLM group explainer.

    Any anomaly check that produces a severity column + contributions map can build one of
    these and call ``add_explanation_column`` directly — no ScoringConfig required.
    """

    severity_col: str
    contributions_col: str
    score_std_col: str
    ai_explanation_col: str
    threshold: float
    model_name: str
    llm_model_config: LLMModelConfig | None = None
    max_groups: int = 500
    redact_columns: tuple[str, ...] = ()

    @classmethod
    def from_scoring_config(cls, config: "ScoringConfig") -> "ExplanationContext":
        return cls(
            severity_col=config.severity_col,
            contributions_col=config.contributions_col,
            score_std_col=config.score_std_col,
            ai_explanation_col=config.ai_explanation_col,
            threshold=config.threshold,
            model_name=config.model_name,
            llm_model_config=config.llm_model_config,
            max_groups=config.max_groups,
            redact_columns=tuple(config.redact_columns or ()),
        )


if DSPY_AVAILABLE:

    class AnomalyGroupExplanationSignature(dspy.Signature):
        """You are a data quality analyst. Given aggregate metadata for a GROUP of anomalous rows
        sharing the same root-cause pattern, explain in plain business language why this group was
        flagged. Your explanation will be shown for every row in the group — describe the pattern,
        not a specific row."""

        feature_contributions: str = dspy.InputField(
            desc=(
                "Mean SHAP contributions across the group, e.g. "
                "'amount (82%), quantity (11%), discount (5%)'. "
                "These are aggregated relative importances — not raw data values."
            )
        )
        group_size: str = dspy.InputField(desc="Number of rows in this group, e.g. '312 rows'.")
        severity_range: str = dspy.InputField(
            desc="Severity percentile range across the group, e.g. 'mean 97.4, min 95.1, max 99.8'."
        )
        confidence: str = dspy.InputField(
            desc=(
                "Model confidence label across the group. 'high' / 'mixed' / 'low' for ensemble, "
                "'n/a' for single-model scoring."
            )
        )
        segment: str = dspy.InputField(
            desc=(
                "Data segment this group belongs to, e.g. 'region=US, product=electronics'. "
                "Empty string if no segmentation was used."
            )
        )
        threshold: str = dspy.InputField(desc="The severity percentile threshold configured by the user (0–100).")
        model_name: str = dspy.InputField(desc="Name of the anomaly detection model that scored this group.")
        drift_summary: str = dspy.InputField(
            desc=(
                "Baseline drift signal from the scoring run, e.g. "
                "'drift detected: amount=4.12; quantity=3.55' or 'none'. "
                "If drift is present, explicitly frame the narrative vs baseline."
            )
        )

        narrative: str = dspy.OutputField(
            desc=(
                "Max 2 sentences, max 40 words total. Describe the GROUP pattern, not a single row. "
                "Reference the top contributing features and the group size. "
                "If drift_summary != 'none', frame at least one feature vs baseline."
            )
        )
        business_impact: str = dspy.OutputField(
            desc=(
                "One sentence, max 25 words. Likely downstream business impact if this group of "
                "rows is processed unchanged. Concrete, tied to the contributing features."
            )
        )
        action: str = dspy.OutputField(
            desc="One sentence, max 20 words. What a data analyst should investigate for this group."
        )


def _build_lm_config(llm_model_config: LLMModelConfig) -> dict:
    """Convert LLMModelConfig to a dict suitable for dspy.LM(**config).

    Routing rules:
      - model_name already carries a litellm provider prefix ("provider/model") → pass through as-is.
      - api_base is provided (explicitly or via OPENAI_API_BASE) → force the "openai/"
        provider prefix so litellm uses its OpenAI-compatible adapter against that base URL.
        Without this, a bare "databricks-foo" model name triggers litellm's native Databricks
        provider and the custom endpoint is ignored (→ ENDPOINT_NOT_FOUND).
      - Otherwise → pass through; litellm's default auto-detection applies.

    api_key / api_base fall back to OPENAI_API_KEY / OPENAI_API_BASE env vars when
    empty on the config, matching the common dspy/litellm usage pattern.
    """
    api_key = llm_model_config.api_key or os.environ.get("OPENAI_API_KEY", "")
    api_base = llm_model_config.api_base or os.environ.get("OPENAI_API_BASE", "")

    model = llm_model_config.model_name
    if "/" not in model and api_base:
        model = f"openai/{model}"

    config: dict = {
        "model": model,
        "model_type": "chat",
        "max_retries": 3,
    }
    if api_key:
        config["api_key"] = api_key
    if api_base:
        config["api_base"] = api_base
    return config


def _derive_confidence(mean_std: float | None, is_ensemble: bool) -> str:
    """Map aggregated score_std + is_ensemble flag to a human-readable confidence label."""
    if not is_ensemble or mean_std is None:
        return "n/a"
    if mean_std < 0.05:
        return "high"
    if mean_std < 0.15:
        return "mixed"
    return "low"


def _pattern_spark_expr(contributions_col: str, redact_set: frozenset[str]) -> Column:
    """Pattern key as a pure-Spark-SQL expression (no Python UDFs shipped to executors).

    Drops null and redacted entries, takes the top-2 features by |value| desc,
    sorts their names asc, and joins with '+'. Empty or null maps yield 'unknown'.
    Ranking uses absolute value so signed SHAP contributions pick the same top-2
    as `format_contributions_map`. Implemented in SQL so Databricks Connect /
    serverless workers don't need the dqx package installed.
    """
    col = f"`{contributions_col}`"
    if redact_set:
        escaped = [r.replace("'", "''") for r in redact_set]
        redact_arr = "array(" + ", ".join(f"'{r}'" for r in escaped) + ")"
        entries = (
            f"filter(map_entries({col}), e -> e.value is not null " f"and not array_contains({redact_arr}, e.key))"
        )
    else:
        entries = f"filter(map_entries({col}), e -> e.value is not null)"
    sql = (
        f"case when {col} is null or size({entries}) = 0 then 'unknown' "
        f"else concat_ws('+', array_sort(transform(slice(array_sort({entries}, "
        f"(a, b) -> case when abs(b.value) > abs(a.value) then 1 "
        f"when abs(b.value) < abs(a.value) then -1 else 0 end), 1, 2), e -> e.key))) end"
    )
    return F.expr(sql)


def _format_segment(segment_values: dict[str, str] | None) -> str:
    """Format segment values as 'k1=v1, k2=v2' or empty string."""
    if not segment_values:
        return ""
    return ", ".join(f"{k}={v}" for k, v in segment_values.items())


def _format_severity_range(mean: float, min_: float, max_: float) -> str:
    return f"mean {mean:.1f}, min {min_:.1f}, max {max_:.1f}"


def _aggregate_groups(
    anomalous: DataFrame,
    contributions_col: str,
    severity_col: str,
    score_std_col: str,
    redact_set: frozenset[str],
    max_groups: int,
) -> tuple[list[dict], int, int]:
    """Aggregate anomalous rows into per-pattern group metadata, capped by ``max_groups``.

    Ranking and the cap are applied inside Spark (``orderBy(...).limit(max_groups)``) so the
    driver only ever collects at most ``max_groups`` rows, regardless of pattern cardinality.
    Some driver-side materialization is unavoidable because the LLM call is driver-side by
    design (DSPy is not shipped to executors).

    Tie-break: secondary sort on the pattern key keeps the result deterministic across runs
    when several patterns share the same ``group_size * group_avg_severity``.

    Returns:
        (kept_rows, dropped_groups_count, dropped_rows_count) where the counts describe the
        groups that exceeded the cap and whose rows will receive a null ai_explanation.
    """
    primary = anomalous.groupBy(_PATTERN_COL).agg(
        F.count(F.lit(1)).alias("group_size"),
        F.avg(severity_col).alias("group_avg_severity"),
        F.min(severity_col).alias("severity_min"),
        F.max(severity_col).alias("severity_max"),
        F.avg(score_std_col).alias("mean_std"),
    )

    exploded = anomalous.select(F.col(_PATTERN_COL), F.explode(F.col(contributions_col)).alias("__k", "__v"))
    if redact_set:
        exploded = exploded.filter(~F.col("__k").isin(list(redact_set)))
    per_key_mean = exploded.groupBy(_PATTERN_COL, "__k").agg(F.avg("__v").alias("__mean"))
    per_pattern_contrib = per_key_mean.groupBy(_PATTERN_COL).agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("__k"), F.col("__mean")))).alias("mean_contributions")
    )

    joined = primary.join(per_pattern_contrib, on=_PATTERN_COL, how="left").cache()
    try:
        totals = joined.agg(
            F.count(F.lit(1)).alias("total_groups"),
            F.sum("group_size").alias("total_rows"),
        ).collect()[0]
        total_groups = int(totals["total_groups"] or 0)
        total_rows = int(totals["total_rows"] or 0)

        ranked = (
            joined.withColumn("__rank_score", F.col("group_size") * F.col("group_avg_severity"))
            .orderBy(F.desc("__rank_score"), F.asc(_PATTERN_COL))
            .limit(max_groups)
        )
        kept_rows = [row.asDict(recursive=True) for row in ranked.collect()]
    finally:
        joined.unpersist()

    kept_rows_count = sum(int(r.get("group_size") or 0) for r in kept_rows)
    dropped_groups_count = max(0, total_groups - len(kept_rows))
    dropped_rows_count = max(0, total_rows - kept_rows_count)
    return kept_rows, dropped_groups_count, dropped_rows_count


def _build_empty_explanation_column() -> Column:
    return F.lit(None).cast(ai_explanation_struct_schema)


def _build_group_result_schema() -> StructType:
    return StructType(
        [
            StructField(_PATTERN_COL, StringType(), True),
            StructField("narrative", StringType(), True),
            StructField("business_impact", StringType(), True),
            StructField("action", StringType(), True),
            StructField("group_size", LongType(), True),
            StructField("group_avg_severity", DoubleType(), True),
        ]
    )


def _call_llm_for_groups(
    kept_groups: list[dict],
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
    predictor,
) -> list[tuple]:
    """Invoke the LLM once per retained group. Returns rows for the result DataFrame."""
    result_rows: list[tuple] = []
    for group in kept_groups:
        contrib_str = format_contributions_map(group.get("mean_contributions") or {}, top_n=_TOP_N)
        severity_range = _format_severity_range(
            float(group["group_avg_severity"]),
            float(group["severity_min"]),
            float(group["severity_max"]),
        )
        prediction = predictor(
            feature_contributions=contrib_str,
            group_size=f"{int(group['group_size'])} rows",
            severity_range=severity_range,
            confidence=_derive_confidence(group.get("mean_std"), is_ensemble),
            segment=segment_str,
            threshold=str(ctx.threshold),
            model_name=ctx.model_name,
            drift_summary=drift_summary or "none",
        )
        result_rows.append(
            (
                group[_PATTERN_COL],
                prediction.narrative,
                prediction.business_impact,
                prediction.action,
                int(group["group_size"]),
                float(group["group_avg_severity"]),
            )
        )
    return result_rows


def _attach_explanation_struct(
    df_with_pattern: DataFrame,
    result_sdf: DataFrame,
    ctx: ExplanationContext,
) -> DataFrame:
    """Join per-pattern LLM results back onto the scored DataFrame and wrap as a struct.

    Rows below threshold or in dropped groups get a null struct.
    """
    joined = df_with_pattern.join(result_sdf, on=_PATTERN_COL, how="left")
    return joined.withColumn(
        ctx.ai_explanation_col,
        F.when(
            (F.col(ctx.severity_col) >= F.lit(ctx.threshold)) & F.col("narrative").isNotNull(),
            F.struct(
                F.col("narrative").alias("narrative"),
                F.col("business_impact").alias("business_impact"),
                F.col(_PATTERN_COL).alias("pattern"),
                F.col("action").alias("action"),
                F.col("group_size").alias("group_size"),
                F.col("group_avg_severity").alias("group_avg_severity"),
            ),
        ).otherwise(_build_empty_explanation_column()),
    ).drop(
        _PATTERN_COL,
        "narrative",
        "business_impact",
        "action",
        "group_size",
        "group_avg_severity",
    )


def add_explanation_column(
    df: DataFrame,
    ctx: ExplanationContext,
    segment_values: dict[str, str] | None,
    is_ensemble: bool,
    drift_summary: str = "none",
) -> DataFrame:
    """Add the AI explanation column to df using the group-based algorithm.

    Anomalous rows are bucketed by a deterministic (segment, pattern) key — pattern =
    sorted top-2 contributing SHAP features. The LLM is called once per group and every
    row in that group receives the same narrative/business_impact/action, plus the
    group's size and mean severity. Rows below threshold or in groups exceeding
    ``ctx.max_groups`` receive a null struct.

    Preconditions (caller's responsibility):
      - dspy is importable
      - df has ctx.score_std_col, ctx.severity_col, and ctx.contributions_col.
    """
    llm_cfg = ctx.llm_model_config or LLMModelConfig()
    lm_config = _build_lm_config(llm_cfg)
    redact_set = frozenset(ctx.redact_columns)
    segment_str = _format_segment(segment_values)

    df_with_pattern = df.withColumn(_PATTERN_COL, _pattern_spark_expr(ctx.contributions_col, redact_set))

    anomalous = df_with_pattern.filter(F.col(ctx.severity_col) >= F.lit(ctx.threshold))
    kept, dropped_groups_count, dropped_rows_count = _aggregate_groups(
        anomalous,
        contributions_col=ctx.contributions_col,
        severity_col=ctx.severity_col,
        score_std_col=ctx.score_std_col,
        redact_set=redact_set,
        max_groups=ctx.max_groups,
    )

    if not kept:
        return df_with_pattern.withColumn(ctx.ai_explanation_col, _build_empty_explanation_column()).drop(_PATTERN_COL)

    if dropped_groups_count:
        logger.warning(
            "ai_explanation: %s groups covering %s rows exceeded max_groups=%s; " "their ai_explanation will be null.",
            dropped_groups_count,
            dropped_rows_count,
            ctx.max_groups,
        )

    language_model = dspy.LM(**lm_config)
    predictor = dspy.Predict(AnomalyGroupExplanationSignature)
    with dspy.settings.context(lm=language_model):
        result_rows = _call_llm_for_groups(kept, ctx, segment_str, is_ensemble, drift_summary, predictor)

    result_sdf = df.sparkSession.createDataFrame(result_rows, schema=_build_group_result_schema())
    return _attach_explanation_struct(df_with_pattern, result_sdf, ctx)
