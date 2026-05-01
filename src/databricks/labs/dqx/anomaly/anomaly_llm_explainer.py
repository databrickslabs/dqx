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
import re
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.dqx.anomaly.anomaly_info_schema import ai_explanation_struct_schema
from databricks.labs.dqx.anomaly.explainability import format_contributions_map
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.llm.llm_core import LLMModelConfigurator

# Single source of truth for the per-group prompt — used by both executors:
#   - "driver": consumed structurally by *AnomalyGroupExplanationSignature* (DSPy reads the
#     class docstring + InputField/OutputField descriptions).
#   - "ai_query": rendered into a single text prompt by *_render_ai_query_prompt* so the same
#     instructions and field semantics flow to the Databricks serving endpoint via SQL.
# Update either path by editing this dict — both stay in sync.
_PROMPT_INSTRUCTIONS = (
    "You are a data quality analyst. Given aggregate metadata for a GROUP of anomalous rows "
    "sharing the same root-cause pattern, explain in plain business language why this group was "
    "flagged. Your explanation will be shown for every row in the group — describe the pattern, "
    "not a specific row."
)
_PROMPT_INPUT_FIELDS: tuple[tuple[str, str], ...] = (
    (
        "feature_contributions",
        "Mean SHAP contributions across the group, e.g. 'amount (82%), quantity (11%), "
        "discount (5%)'. These are aggregated relative importances — not raw data values.",
    ),
    ("group_size", "Number of rows in this group, e.g. '312 rows'."),
    ("severity_range", "Severity percentile range across the group, e.g. 'mean 97.4, min 95.1, max 99.8'."),
    (
        "confidence",
        "Model confidence label across the group. 'high' / 'mixed' / 'low' for ensemble, 'n/a' "
        "for single-model scoring.",
    ),
    (
        "segment",
        "Data segment this group belongs to, e.g. 'region=US, product=electronics'. Empty string "
        "if no segmentation was used.",
    ),
    ("threshold", "The severity percentile threshold configured by the user (0–100)."),
    ("model_name", "Name of the anomaly detection model that scored this group."),
    (
        "drift_summary",
        "Baseline drift signal from the scoring run, e.g. 'drift detected: amount=4.12; "
        "quantity=3.55' or 'none'. If drift is present, explicitly frame the narrative vs "
        "baseline.",
    ),
)
_PROMPT_OUTPUT_FIELDS: tuple[tuple[str, str], ...] = (
    (
        "narrative",
        "Max 2 sentences, max 40 words total. Describe the GROUP pattern, not a single row. "
        "Reference the top contributing features and the group size. If drift_summary != 'none', "
        "frame at least one feature vs baseline.",
    ),
    (
        "business_impact",
        "One sentence, max 25 words. Likely downstream business impact if this group of rows is "
        "processed unchanged. Concrete, tied to the contributing features.",
    ),
    (
        "action",
        "One sentence, max 20 words. What a data analyst should investigate for this group.",
    ),
)
# JSON schema the ai_query path sends as ``responseFormat`` so the endpoint returns
# {narrative, business_impact, action} as a structured object — no string parsing needed.
# NB: Databricks ai_query rejects ``maxLength`` on string types (BAD_REQUEST). Length-capping
# is enforced post-parse via *_sanitize* in *_call_llm_for_groups_ai_query* (matches the driver
# path's *_sanitize_llm_field*) so we don't need it on the schema here.
_AI_QUERY_RESPONSE_FORMAT = (
    '{"type":"json_schema","json_schema":{"name":"explanation","strict":true,'
    '"schema":{"type":"object","additionalProperties":false,'
    '"required":["narrative","business_impact","action"],'
    '"properties":{'
    '"narrative":{"type":"string"},'
    '"business_impact":{"type":"string"},'
    '"action":{"type":"string"}}}}}'
)

try:
    import dspy  # type: ignore

    DSPY_AVAILABLE = True
except ImportError:
    dspy = None
    DSPY_AVAILABLE = False

if TYPE_CHECKING:
    from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig

logger = logging.getLogger(__name__)

_TOP_N = 5
_PATTERN_COL = "__dqx_pattern"
_LLM_FIELD_MAX_LEN = 500
# Strip ASCII C0 controls (\x00-\x1f, includes \n \r \t \x1b) and DEL (\x7f).
# These are the chars that can forge log entries (CWE-117) when LLM output is logged.
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")


def _sanitize_llm_field(value: str | None) -> str | None:
    """Coerce, strip control chars, and length-cap an LLM output field.

    DSPy ``OutputField(desc=...)`` is guidance, not enforcement — a misbehaving or
    jailbroken model can return non-str values, multi-KB strings, or control characters.
    Treat LLM output as untrusted (OWASP LLM06).
    """
    if value is None:
        return None
    return _CONTROL_CHAR_RE.sub(" ", str(value))[:_LLM_FIELD_MAX_LEN]


_DSPY_MISSING_MSG = (
    "The 'dspy' dependency is required for AI explanations. Install: pip install databricks-labs-dqx[anomaly,llm]"
)


def _require_dspy() -> None:
    """Raise a clear ImportError when dspy is not installed."""
    if not DSPY_AVAILABLE:
        raise ImportError(_DSPY_MISSING_MSG)


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


def _build_dspy_signature() -> type:
    """Construct the DSPy Signature dynamically from the shared *_PROMPT_* tables.

    Built at import time when DSPy is available. Defining it dynamically (instead of as a
    static class with hard-coded fields) keeps the driver and ai_query paths sourcing prompt
    text from one place — *_PROMPT_INSTRUCTIONS*, *_PROMPT_INPUT_FIELDS*, *_PROMPT_OUTPUT_FIELDS*.
    """
    namespace: dict[str, object] = {"__doc__": _PROMPT_INSTRUCTIONS, "__annotations__": {}}
    for name, desc in _PROMPT_INPUT_FIELDS:
        namespace["__annotations__"][name] = str  # type: ignore[index]
        namespace[name] = dspy.InputField(desc=desc)
    for name, desc in _PROMPT_OUTPUT_FIELDS:
        namespace["__annotations__"][name] = str  # type: ignore[index]
        namespace[name] = dspy.OutputField(desc=desc)
    return type("AnomalyGroupExplanationSignature", (dspy.Signature,), namespace)


# Default to None so the symbol is defined at module scope even when DSPy is missing. The driver
# path calls *_require_dspy()* before referencing it, so the runtime invariant is preserved.
AnomalyGroupExplanationSignature: type | None = None
if DSPY_AVAILABLE:
    AnomalyGroupExplanationSignature = _build_dspy_signature()


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

    # Rank + cap the per-pattern aggregates before joining contributions, so the join is on
    # at most ``max_groups`` rows. Totals are read from the same ``primary`` aggregate via a
    # separate action — we deliberately avoid ``.cache()`` here because PERSIST TABLE is not
    # supported on Databricks serverless compute. ``primary`` is a per-pattern roll-up so the
    # second action is cheap relative to the LLM calls that follow.
    totals_row = primary.agg(
        F.count(F.lit(1)).alias("total_groups"),
        F.sum("group_size").alias("total_rows"),
    ).collect()[0]
    total_groups = int(totals_row["total_groups"] or 0)
    total_rows = int(totals_row["total_rows"] or 0)

    ranked = (
        primary.withColumn("__rank_score", F.col("group_size") * F.col("group_avg_severity"))
        .orderBy(F.desc("__rank_score"), F.asc(_PATTERN_COL))
        .limit(max_groups)
    )
    kept_rows = [
        row.asDict(recursive=True) for row in ranked.join(per_pattern_contrib, on=_PATTERN_COL, how="left").collect()
    ]

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


def _explain_one_group(
    group: dict,
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
    predictor,
    language_model,
) -> tuple:
    """Invoke the LLM for a single group. Returns a result tuple; on failure logs and emits a
    null-explanation tuple so the surrounding scoring run isn't aborted by one bad call.

    The dspy LM binding is applied inside this function because it's executed on a worker
    thread (via *Threads.gather*) — *dspy.settings.context* uses contextvars and Python's
    ThreadPoolExecutor does not propagate the submitter's context to worker threads, so the
    binding from the caller would otherwise be lost.
    """
    pattern = group[_PATTERN_COL]
    group_size = int(group["group_size"])
    group_avg_severity = float(group["group_avg_severity"])
    contrib_str = format_contributions_map(group.get("mean_contributions") or {}, top_n=_TOP_N)
    severity_range = _format_severity_range(
        group_avg_severity,
        float(group["severity_min"]),
        float(group["severity_max"]),
    )
    try:
        with dspy.settings.context(lm=language_model):
            prediction = predictor(
                feature_contributions=contrib_str,
                group_size=f"{group_size} rows",
                severity_range=severity_range,
                confidence=_derive_confidence(group.get("mean_std"), is_ensemble),
                segment=segment_str,
                threshold=str(ctx.threshold),
                model_name=ctx.model_name,
                drift_summary=drift_summary or "none",
            )
    except Exception as exc:  # pylint: disable=broad-except
        # Sanitise the pattern key for the log line — it derives from column names that may
        # contain newlines or control chars (CWE-117). Strip both before interpolating.
        safe_pattern = pattern.replace("\n", "_").replace("\r", "_") if isinstance(pattern, str) else "<non-str>"
        logger.warning(f"ai_explanation: LLM call failed for pattern '{safe_pattern}': {exc!r}")
        return (pattern, None, None, None, group_size, group_avg_severity)
    return (
        pattern,
        _sanitize_llm_field(prediction.narrative),
        _sanitize_llm_field(prediction.business_impact),
        _sanitize_llm_field(prediction.action),
        group_size,
        group_avg_severity,
    )


def _call_llm_for_groups(
    kept_groups: list[dict],
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
    predictor,
    language_model,
) -> list[tuple]:
    """Invoke the LLM once per retained group, in parallel. Returns rows for the result DataFrame.

    Uses *databricks.labs.blueprint.parallel.Threads.gather* so per-group failures are isolated
    (logged + emitted as null-explanation tuples by *_explain_one_group*) instead of aborting
    the whole scoring run. With the default *max_groups=500* and a sequential loop, p95 LLM
    latency dominates wall time; thread-level concurrency cuts this dramatically because the
    workload is IO-bound.
    """
    if not kept_groups:
        return []
    tasks = [
        partial(_explain_one_group, group, ctx, segment_str, is_ensemble, drift_summary, predictor, language_model)
        for group in kept_groups
    ]
    successes, errors = Threads.gather("ai_explanation", tasks)
    if errors:
        # _explain_one_group catches and logs per-group failures itself; anything here is a
        # programming error in the worker (e.g. malformed group dict). Surface counts only.
        logger.warning(f"ai_explanation: {len(errors)} unexpected worker errors (see prior logs).")
    return list(successes)


def _render_ai_query_prompt_header() -> str:
    """Plain-text prompt assembled from the same shared dict the DSPy signature consumes.

    Both executor paths read from *_PROMPT_INSTRUCTIONS* / *_PROMPT_INPUT_FIELDS* /
    *_PROMPT_OUTPUT_FIELDS* — driver path via DSPy field metadata, ai_query path via this
    rendered string — so updating either is a one-line change.
    """
    lines = [_PROMPT_INSTRUCTIONS, "", "Inputs:"]
    for name, desc in _PROMPT_INPUT_FIELDS:
        lines.append(f"- {name}: {desc}")
    lines.append("")
    lines.append("Respond with ONLY a JSON object. Field rules:")
    for name, desc in _PROMPT_OUTPUT_FIELDS:
        lines.append(f"- {name}: {desc}")
    lines.append("")
    return "\n".join(lines)


_AI_QUERY_PROMPT_HEADER = _render_ai_query_prompt_header()


def _resolve_ai_query_endpoint(model_name: str) -> str:
    """Map *LLMModelConfig.model_name* onto a Databricks Model Serving endpoint name.

    The default value carries a ``databricks/`` provider prefix because DSPy/litellm needs it; the
    SQL ``ai_query`` function takes the bare endpoint name. A non-Databricks prefix means the user
    pointed at another provider — incompatible with ``executor='ai_query'``, surface a clear
    error rather than silently producing a malformed call.
    """
    if not model_name:
        raise InvalidParameterError("model_name is required when executor='ai_query'.")
    if "/" not in model_name:
        return model_name
    provider, _, endpoint = model_name.partition("/")
    if provider != "databricks":
        raise InvalidParameterError(
            f"executor='ai_query' requires a Databricks serving endpoint, got provider {provider!r} "
            f"in model_name={model_name!r}. Use executor='driver' for non-Databricks providers."
        )
    return endpoint


def _format_contributions_sql(top_n: int) -> Column:
    """Spark expression producing 'feat_a (82%), feat_b (11%)' from a ``mean_contributions`` map.

    Mirrors *format_contributions_map* but stays inside Spark so per-group prompts can be
    assembled without a driver-side loop. Null/empty maps yield 'unknown'; entries are sorted by
    absolute value descending and percentages are normalised against the L1 sum of |value|.
    """
    entries = "filter(map_entries(`mean_contributions`), e -> e.value is not null)"
    sorted_entries = (
        f"array_sort({entries}, (a, b) -> "
        f"case when abs(b.value) > abs(a.value) then 1 "
        f"when abs(b.value) < abs(a.value) then -1 else 0 end)"
    )
    top = f"slice({sorted_entries}, 1, {int(top_n)})"
    abs_sum = f"aggregate({sorted_entries}, 0.0D, (acc, e) -> acc + abs(e.value))"
    formatted = (
        f"transform({top}, e -> concat(e.key, ' (', "
        f"cast(round((abs(e.value) / case when {abs_sum} = 0 then 1 else {abs_sum} end) * 100) as int), '%)'))"
    )
    sql = (
        f"case when `mean_contributions` is null or size({entries}) = 0 then 'unknown' "
        f"else concat_ws(', ', {formatted}) end"
    )
    return F.expr(sql)


def _build_ai_query_prompt_column(
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
) -> Column:
    """Assemble the per-row prompt string sent to ``ai_query``.

    Per-group fields come from columns added by *_aggregate_groups_spark*; per-run fields are
    constants for the whole call. The shared header (*_AI_QUERY_PROMPT_HEADER*) holds the
    instructions and field semantics so both executor paths stay aligned.
    """
    confidence_expr = (
        F.when((F.col("mean_std").isNull()) | F.lit(not is_ensemble), F.lit("n/a"))
        .when(F.col("mean_std") < F.lit(0.05), F.lit("high"))
        .when(F.col("mean_std") < F.lit(0.15), F.lit("mixed"))
        .otherwise(F.lit("low"))
    )
    severity_range_expr = F.format_string(
        "mean %.1f, min %.1f, max %.1f",
        F.col("group_avg_severity"),
        F.col("severity_min"),
        F.col("severity_max"),
    )
    group_size_expr = F.concat(F.col("group_size").cast(StringType()), F.lit(" rows"))
    return F.concat(
        F.lit(_AI_QUERY_PROMPT_HEADER),
        F.lit("feature_contributions: "),
        F.col("feature_contributions"),
        F.lit("\n"),
        F.lit("group_size: "),
        group_size_expr,
        F.lit("\n"),
        F.lit("severity_range: "),
        severity_range_expr,
        F.lit("\n"),
        F.lit("confidence: "),
        confidence_expr,
        F.lit("\n"),
        F.lit("segment: "),
        F.lit(segment_str),
        F.lit("\n"),
        F.lit("threshold: "),
        F.lit(str(ctx.threshold)),
        F.lit("\n"),
        F.lit("model_name: "),
        F.lit(ctx.model_name),
        F.lit("\n"),
        F.lit("drift_summary: "),
        F.lit(drift_summary or "none"),
    )


def _aggregate_groups_spark(
    anomalous: DataFrame,
    contributions_col: str,
    severity_col: str,
    score_std_col: str,
    redact_set: frozenset[str],
    max_groups: int,
) -> tuple[DataFrame, int, int, int]:
    """Spark-resident counterpart to *_aggregate_groups* used by the ai_query executor path.

    Returns the per-pattern aggregate as a DataFrame so the LLM call can run on executors via
    ``ai_query``, with no driver collect of LLM payloads. ``max_groups`` is honoured as a cost
    cap (top-N by ``group_size * group_avg_severity``); rows beyond the cap fall through with
    null explanations via the left-join in *_attach_explanation_struct*.
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

    totals_row = primary.agg(
        F.count(F.lit(1)).alias("total_groups"),
        F.sum("group_size").alias("total_rows"),
    ).collect()[0]
    total_groups = int(totals_row["total_groups"] or 0)
    total_rows = int(totals_row["total_rows"] or 0)

    ranked = (
        primary.withColumn("__rank_score", F.col("group_size") * F.col("group_avg_severity"))
        .orderBy(F.desc("__rank_score"), F.asc(_PATTERN_COL))
        .limit(max_groups)
        .drop("__rank_score")
    )
    kept = ranked.join(per_pattern_contrib, on=_PATTERN_COL, how="left")

    kept_totals = kept.agg(
        F.count(F.lit(1)).alias("kept_groups"),
        F.sum("group_size").alias("kept_rows"),
    ).collect()[0]
    kept_groups_count = int(kept_totals["kept_groups"] or 0)
    kept_rows_count = int(kept_totals["kept_rows"] or 0)
    dropped_groups_count = max(0, total_groups - kept_groups_count)
    dropped_rows_count = max(0, total_rows - kept_rows_count)
    return kept, dropped_groups_count, dropped_rows_count, total_groups


def _call_llm_for_groups_ai_query(
    kept_groups_sdf: DataFrame,
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
) -> DataFrame:
    """Invoke ``ai_query`` once per retained group inside Spark and return rows shaped like
    *_build_group_result_schema* so *_attach_explanation_struct* works unchanged.

    Budget caps (max_tokens, temperature) come from *LLMModelConfig* via *modelParameters*.
    Sanitisation of the response (control-char strip + length cap) is applied as Spark
    expressions to match the driver-path *_sanitize_llm_field*.
    """
    llm_cfg = ctx.llm_model_config or LLMModelConfig()
    endpoint = _resolve_ai_query_endpoint(llm_cfg.model_name)
    enriched = kept_groups_sdf.withColumn(
        "feature_contributions",
        _format_contributions_sql(_TOP_N),
    ).withColumn(
        "__prompt",
        _build_ai_query_prompt_column(ctx, segment_str, is_ensemble, drift_summary),
    )

    # ai_query is parameterised through the SQL string. *endpoint* is matched against the strict
    # serving-endpoint pattern in *_resolve_ai_query_endpoint*; max_tokens/temperature come from
    # validated config fields. responseFormat is a constant JSON literal (no user input).
    raw = enriched.withColumn(
        "__raw_response",
        F.expr(
            f"ai_query('{endpoint}', __prompt, "
            f"modelParameters => named_struct('max_tokens', {int(llm_cfg.max_tokens)}, "
            f"'temperature', {float(llm_cfg.temperature)}), "
            f"responseFormat => '{_AI_QUERY_RESPONSE_FORMAT}')"
        ),
    )

    response_schema = StructType(
        [
            StructField("narrative", StringType(), True),
            StructField("business_impact", StringType(), True),
            StructField("action", StringType(), True),
        ]
    )
    parsed = raw.withColumn("__parsed", F.from_json(F.col("__raw_response"), response_schema))

    def _sanitize(col_name: str) -> Column:
        # Strip C0/DEL control chars and length-cap; matches _sanitize_llm_field on the driver path.
        return F.expr(
            f"substring(regexp_replace(__parsed.{col_name}, '[\\\\x00-\\\\x1f\\\\x7f]', ' '), "
            f"1, {_LLM_FIELD_MAX_LEN})"
        )

    return parsed.select(
        F.col(_PATTERN_COL),
        _sanitize("narrative").alias("narrative"),
        _sanitize("business_impact").alias("business_impact"),
        _sanitize("action").alias("action"),
        F.col("group_size").cast(LongType()).alias("group_size"),
        F.col("group_avg_severity").cast(DoubleType()).alias("group_avg_severity"),
    )


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


def build_language_model(ctx: ExplanationContext) -> object:
    """Construct a dspy.LM from the context's LLM config.

    Only used by the ``executor='driver'`` path. Call once and pass the result to
    *add_explanation_column* when scoring multiple segments so the same LM instance is reused
    across all segment calls instead of being reconstructed per segment. The ``ai_query`` path
    does not use DSPy at all.
    """
    _require_dspy()
    llm_cfg = ctx.llm_model_config or LLMModelConfig()
    return LLMModelConfigurator(llm_cfg).create_lm()


def _log_dropped_groups(dropped_groups_count: int, dropped_rows_count: int, max_groups: int) -> None:
    if dropped_groups_count:
        logger.warning(
            f"ai_explanation: {dropped_groups_count} groups covering {dropped_rows_count} rows "
            f"exceeded max_groups={max_groups}; their ai_explanation will be null."
        )


def _add_explanation_column_driver(
    df: DataFrame,
    df_with_pattern: DataFrame,
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
    language_model: object | None,
) -> DataFrame:
    """Driver-path explainer: DSPy + threaded fan-out, group results collected through the driver."""
    _require_dspy()
    if language_model is None:
        language_model = build_language_model(ctx)
    redact_set = frozenset(ctx.redact_columns)
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
    _log_dropped_groups(dropped_groups_count, dropped_rows_count, ctx.max_groups)
    predictor = dspy.Predict(AnomalyGroupExplanationSignature)
    result_rows = _call_llm_for_groups(kept, ctx, segment_str, is_ensemble, drift_summary, predictor, language_model)
    result_sdf = df.sparkSession.createDataFrame(result_rows, schema=_build_group_result_schema())
    return _attach_explanation_struct(df_with_pattern, result_sdf, ctx)


def _add_explanation_column_ai_query(
    df_with_pattern: DataFrame,
    ctx: ExplanationContext,
    segment_str: str,
    is_ensemble: bool,
    drift_summary: str,
) -> DataFrame:
    """ai_query-path explainer: SQL ``ai_query`` on executors, no DSPy, no driver collect of LLM output."""
    redact_set = frozenset(ctx.redact_columns)
    anomalous = df_with_pattern.filter(F.col(ctx.severity_col) >= F.lit(ctx.threshold))
    kept_sdf, dropped_groups_count, dropped_rows_count, total_groups = _aggregate_groups_spark(
        anomalous,
        contributions_col=ctx.contributions_col,
        severity_col=ctx.severity_col,
        score_std_col=ctx.score_std_col,
        redact_set=redact_set,
        max_groups=ctx.max_groups,
    )
    # No anomalies above threshold → short-circuit to a null struct, no ai_query call.
    if total_groups == 0:
        return df_with_pattern.withColumn(ctx.ai_explanation_col, _build_empty_explanation_column()).drop(_PATTERN_COL)
    _log_dropped_groups(dropped_groups_count, dropped_rows_count, ctx.max_groups)
    result_sdf = _call_llm_for_groups_ai_query(kept_sdf, ctx, segment_str, is_ensemble, drift_summary)
    return _attach_explanation_struct(df_with_pattern, result_sdf, ctx)


def add_explanation_column(
    df: DataFrame,
    ctx: ExplanationContext,
    segment_values: dict[str, str] | None,
    is_ensemble: bool,
    drift_summary: str = "none",
    language_model: object | None = None,
) -> DataFrame:
    """Add the AI explanation column to df using the group-based algorithm.

    Anomalous rows are bucketed by a deterministic (segment, pattern) key — pattern =
    sorted top-2 contributing SHAP features. The LLM is called once per group and every
    row in that group receives the same narrative/business_impact/action, plus the
    group's size and mean severity. Rows below threshold or in groups exceeding
    ``ctx.max_groups`` receive a null struct.

    Executor selection is read from ``ctx.llm_model_config.executor``:

    - ``"ai_query"`` (default): runs the LLM call inside Spark via the SQL ``ai_query`` function
      against a Databricks Model Serving endpoint. Scales with the cluster, no DSPy required.
    - ``"driver"``: runs DSPy on the driver with threaded fan-out — use this for non-Databricks
      providers (any endpoint DSPy supports via *api_base*). Pass a pre-built *language_model*
      (from *build_language_model*) when scoring multiple segments to reuse one dspy.LM.

    Preconditions (caller's responsibility):
      - df has ctx.score_std_col, ctx.severity_col, and ctx.contributions_col.

    Raises:
      ImportError: When ``executor='driver'`` and the 'dspy' dependency is not installed.
      InvalidParameterError: When ``executor='ai_query'`` and *model_name* points at a non-Databricks provider.
    """
    redact_set = frozenset(ctx.redact_columns)
    segment_str = _format_segment(segment_values)
    df_with_pattern = df.withColumn(_PATTERN_COL, _pattern_spark_expr(ctx.contributions_col, redact_set))

    llm_cfg = ctx.llm_model_config or LLMModelConfig()
    if llm_cfg.executor == "ai_query":
        return _add_explanation_column_ai_query(df_with_pattern, ctx, segment_str, is_ensemble, drift_summary)
    return _add_explanation_column_driver(
        df, df_with_pattern, ctx, segment_str, is_ensemble, drift_summary, language_model
    )
