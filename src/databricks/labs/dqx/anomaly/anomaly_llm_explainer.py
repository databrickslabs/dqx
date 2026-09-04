"""LLM-based group explanation for row anomaly detection.

The algorithm is group-based: anomalous rows are grouped by a deterministic
pattern key — the sorted top-2 contributing features — and the LLM is invoked
once per group. Every row in a group shares the same
narrative/business_impact/action; group_size and group_avg_severity signal that
the explanation describes a pattern, not a row.

The LLM call runs entirely inside Spark via the SQL ``ai_query`` function against a
Databricks Model Serving endpoint — no driver collect of LLM output, scales with the
cluster, and needs no extra Python dependency.
"""

import json
import logging
import re
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from databricks.labs.dqx.anomaly.anomaly_info_schema import ai_explanation_struct_schema
from databricks.labs.dqx.anomaly.feature_naming import engineered_from, human_label
from databricks.labs.dqx.anomaly.transformers import BASELINE_RELATIVE_SUFFIX, SparkFeatureMetadata
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.errors import InvalidParameterError

# Single source of truth for the per-group prompt. The instructions, input-field semantics, and
# output-field rules below are rendered into one text prompt by *_render_ai_query_prompt_header*
# and sent to the Databricks serving endpoint via SQL ``ai_query``. Update the prompt by editing
# these tables — the rendered header and the structured-output schema both derive from them.
_PROMPT_INSTRUCTIONS = (
    "You are a data quality analyst. Given aggregate metadata for a GROUP of anomalous rows "
    "sharing the same root-cause pattern, explain in plain business language why this group was "
    "flagged. Your explanation will be shown for every row in the group — describe the pattern, "
    "not a specific row.\n"
    "Be direct and concrete. Avoid hedging phrases like 'The data shows', 'It appears that', or "
    "'might indicate'. Do not restate the input field names back to the user, and do not invent "
    "feature names, values, or baseline groups that are not present in the input."
)
# What a contribution means, per detector family. Keyed by the ``ModelIdentity.algorithm`` prefix that is
# persisted in the registry, so a model trained by any version resolves as long as that string is stable.
# The fallback is the value-based reading, which is what every algorithm before the correlation-aware one
# meant and is the safer default: it claims less about relationships than the other way round would.
_ATTRIBUTION_SEMANTICS: tuple[tuple[str, str], ...] = (
    (
        "Mahalanobis",
        "relationships between metrics. A high contribution means this metric departed from its usual "
        "relationship with the others -- its own value may sit well inside its normal range. Describe the "
        "pattern as a broken relationship between metrics, and do NOT call an individual metric abnormal, "
        "high, low, or deviating unless the contributions are concentrated in a single metric.",
    ),
    (
        "IsolationForest",
        "individual feature values. A high contribution means this feature's own value was unusual for the "
        "rows it was compared against.",
    ),
)
_DEFAULT_ATTRIBUTION_SEMANTICS = _ATTRIBUTION_SEMANTICS[-1][1]


def attribution_semantics(algorithm: str | None) -> str:
    """What a high contribution means for *algorithm*, as a sentence for the prompt.

    Falls back to the value-based reading when the algorithm is unknown or absent, which is both the
    historical behaviour and the more conservative claim.
    """
    for prefix, meaning in _ATTRIBUTION_SEMANTICS:
        if algorithm and algorithm.startswith(prefix):
            return meaning
    return _DEFAULT_ATTRIBUTION_SEMANTICS


_PROMPT_INPUT_FIELDS: tuple[tuple[str, str], ...] = (
    (
        "attribution_basis",
        "What the feature_contributions below are measuring. Read them accordingly -- this decides "
        "whether the pattern is 'these values were extreme' or 'these metrics stopped agreeing'.",
    ),
    (
        "feature_contributions",
        "Mean contributions across the group, already named for a reader, e.g. 'amount vs its "
        "group baseline (82%), quantity (11%), discount (5%)'. A phrase like 'X vs its group "
        "baseline' means X was unusual relative to its own baseline group, not in absolute terms. "
        "These are aggregated relative importances — not raw data values.",
    ),
    ("group_size", "Number of rows in this group, e.g. '312 rows'."),
    ("severity_range", "Severity percentile range across the group, e.g. 'mean 97.4, min 95.1, max 99.8'."),
    (
        "confidence",
        "Model confidence label across the group. 'high' / 'mixed' / 'low' for ensemble, 'n/a' "
        "for single-model scoring.",
    ),
    (
        "baseline_grouping",
        "The columns whose values define each row's baseline group, e.g. 'region' or "
        "'region, product'. Anomalies are judged relative to the row's own group baseline; "
        "'none' when the model is not grouped.",
    ),
    ("threshold", "The severity percentile threshold configured by the user (0–100)."),
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
# Two few-shot exemplars (one without drift, one with) pin the desired style and JSON shape for
# smaller serving models. Kept short so the prompt stays well within token budgets.
_PROMPT_EXAMPLES = (
    "Example (no drift):\n"
    "feature_contributions: amount vs its group baseline (61%), quantity (22%)\n"
    "group_size: 312 rows\n"
    "severity_range: mean 97.4, min 95.1, max 99.8\n"
    "confidence: high\n"
    "baseline_grouping: region\n"
    "threshold: 95.0\n"
    "drift_summary: none\n"
    'Response: {"narrative":"312 rows are driven mainly by amount, which sits far above the norm '
    'for its own region (61%), with quantity secondary (22%).","business_impact":"Inflated amount '
    'fields overstate revenue if these rows are processed unchanged.","action":"Reconcile amount '
    'against source orders within each affected region."}\n\n'
    "Example (with drift):\n"
    "feature_contributions: latency_ms (74%), retries (12%)\n"
    "group_size: 88 rows\n"
    "severity_range: mean 98.9, min 97.0, max 99.9\n"
    "confidence: mixed\n"
    "baseline_grouping: none\n"
    "threshold: 95.0\n"
    "drift_summary: drift detected: latency_ms=4.12\n"
    'Response: {"narrative":"88 rows are dominated by latency_ms (74%), which has also drifted from '
    'baseline; retries contribute modestly (12%).","business_impact":"Elevated latency risks SLA '
    'breaches for downstream consumers.","action":"Investigate latency_ms regressions against the '
    'training baseline."}'
)

if TYPE_CHECKING:
    from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig

logger = logging.getLogger(__name__)

_TOP_N = 5
# Default working-column name for the pattern group key. Production scoring overrides
# this with a UUID-suffixed name via *ScoringConfig.pattern_col* (threaded through
# *ExplanationContext.pattern_col*) so it can never collide with a user column; the constant is
# only the fallback for direct *ExplanationContext* construction.
_DEFAULT_PATTERN_COL = "__dqx_pattern"
_LLM_FIELD_MAX_LEN = 500
# Confidence-label thresholds on the per-group mean ensemble std. Single source of truth for the
# ai_query confidence expression (see *_build_ai_query_prompt_column*).
_CONFIDENCE_HIGH_BELOW = 0.05
_CONFIDENCE_MIXED_BELOW = 0.15


def _sql_string_literal(value: str) -> str:
    """Escape a string for safe interpolation inside a single-quoted Spark SQL literal.

    Spark SQL treats backslash as an escape character inside string literals, so both the
    backslash and the single quote must be escaped (not just the quote). Used for
    *redact_columns* values — already validated as non-empty strings — before they are embedded
    in the pattern-key SQL built by *_pattern_spark_expr*.
    """
    return value.replace("\\", "\\\\").replace("'", "''")


def _build_ai_query_response_format() -> str:
    """Build the ai_query ``responseFormat`` JSON schema from *_PROMPT_OUTPUT_FIELDS*.

    Single source of truth: adding or removing an output field updates both the prompt rules and
    the structured-output schema, so they cannot drift. NB: Databricks ai_query rejects
    ``maxLength`` on string types (BAD_REQUEST); length-capping is enforced post-parse via
    *_sanitize* in *_call_llm_for_groups_ai_query*.
    """
    fields = [name for name, _ in _PROMPT_OUTPUT_FIELDS]
    return json.dumps(
        {
            "type": "json_schema",
            "json_schema": {
                "name": "explanation",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": fields,
                    "properties": {name: {"type": "string"} for name in fields},
                },
            },
        },
        separators=(",", ":"),
    )


_AI_QUERY_RESPONSE_FORMAT = _build_ai_query_response_format()


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
    # Cap on LLM calls made by a single ``add_explanation_column`` invocation. When the
    # caller is *score_segmented* this is the *per-segment budget* (split equally across
    # segments by the caller); the *user-facing* global cap lives on
    # ``ScoringConfig.max_groups`` and is divided before being threaded through here.
    # When calling ``add_explanation_column`` directly (no segmentation), this value is
    # the absolute cap on LLM calls for that one call.
    max_groups: int = 500
    redact_columns: tuple[str, ...] = ()
    # Internal working-column name for the (baseline group, pattern) group key. Defaults to a fixed
    # name for direct construction; production scoring passes a UUID-suffixed name so it can
    # never collide with a user-supplied column.
    pattern_col: str = _DEFAULT_PATTERN_COL
    # The model's feature metadata, threaded through so redaction can drop every feature derived
    # from a redacted column (not just the column itself) and so contribution keys can be rendered
    # as human labels. Optional: a caller that builds the context directly without it falls back to
    # best-effort redaction of the baseline-relative feature only, and to raw engineered keys.
    feature_metadata: SparkFeatureMetadata | None = None
    # The trained model's algorithm, from ``ModelIdentity.algorithm``. Decides how the prompt tells the
    # model to read a contribution -- as an extreme value or as a broken relationship between metrics.
    # Optional, and absent means the value-based reading, which is what every algorithm before the
    # correlation-aware one meant.
    algorithm: str | None = None

    @classmethod
    def from_scoring_config(
        cls,
        config: "ScoringConfig",
        feature_metadata: SparkFeatureMetadata | None = None,
        algorithm: str | None = None,
    ) -> "ExplanationContext":
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
            pattern_col=config.pattern_col,
            feature_metadata=feature_metadata,
            algorithm=algorithm,
        )


def redaction_set(redact_columns: tuple[str, ...], metadata: SparkFeatureMetadata | None = None) -> frozenset[str]:
    """Columns to redact, plus every engineered feature derived from them.

    Redaction matches contribution keys exactly, and contribution keys are *engineered* feature
    names. So redacting ``amount`` must also stop ``amount_rel_baseline`` -- a signed log-ratio of
    the same column -- and redacting ``country`` must stop ``country_US``, ``country_DE``,
    ``country_freq`` and ``country_is_null``. A caller naming a column sensitive means every feature
    derived from it is sensitive too.

    With *metadata*, the derived features are enumerated exactly via *engineered_from*, which closes
    the one-hot and frequency gap that the source column alone could not. Without it (a caller who
    built the context directly and did not thread metadata through), only the baseline-relative
    feature is reconstructable from the column name, so that alone is covered -- best effort.
    """
    expanded = set(redact_columns)
    if metadata is not None:
        for column in redact_columns:
            expanded.update(engineered_from(column, metadata))
    else:
        expanded.update(f"{column}{BASELINE_RELATIVE_SUFFIX}" for column in redact_columns)
    return frozenset(expanded)


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
        redact_arr = "array(" + ", ".join(f"'{_sql_string_literal(r)}'" for r in sorted(redact_set)) + ")"
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


def _baseline_grouping_str(metadata: SparkFeatureMetadata | None) -> str:
    """The baseline grouping columns as a prompt string, e.g. 'region, product' or 'none'.

    A per-run constant: the anomalies in this run are all judged against a group baseline defined by
    these columns (or against a global baseline when the model is not grouped). The column *names*
    are structural, not row values, so unlike the old segment values they carry no PII and need no
    redaction.
    """
    if metadata is None or not metadata.baseline_by:
        return "none"
    return ", ".join(metadata.baseline_by)


def _human_labels(metadata: SparkFeatureMetadata | None) -> dict[str, str]:
    """Engineered-name -> human-label map for the model's features, omitting identity labels.

    Used to render contribution keys for a reader (``amount_rel_baseline`` ->
    ``amount vs its group baseline``). Only entries whose label differs from the raw name are
    included, so the SQL lookup stays small; anything not in the map falls back to its raw name.
    Empty when no metadata was threaded through, in which case raw engineered names are shown.
    """
    if metadata is None:
        return {}
    labels: dict[str, str] = {}
    for name in metadata.engineered_feature_names:
        label = human_label(name, metadata)
        if label != name:
            labels[name] = label
    return labels


def _build_empty_explanation_column() -> Column:
    return F.lit(None).cast(ai_explanation_struct_schema)


def _render_ai_query_prompt_header() -> str:
    """Plain-text prompt assembled from the shared *_PROMPT_* tables.

    The instructions, input-field semantics, output-field rules, and few-shot exemplars are all
    rendered from one place so the prompt has a single source of truth.
    """
    lines = [_PROMPT_INSTRUCTIONS, "", "Inputs:"]
    for name, desc in _PROMPT_INPUT_FIELDS:
        lines.append(f"- {name}: {desc}")
    lines.append("")
    lines.append("Respond with ONLY a JSON object. Field rules:")
    for name, desc in _PROMPT_OUTPUT_FIELDS:
        lines.append(f"- {name}: {desc}")
    lines.append("")
    lines.append(_PROMPT_EXAMPLES)
    lines.append("")
    return "\n".join(lines)


_AI_QUERY_PROMPT_HEADER = _render_ai_query_prompt_header()

# Databricks Model Serving endpoint name rules: 1–63 chars, must start with a letter, then any
# of [letter, digit, hyphen, underscore]. This is the *platform's own* naming constraint — any
# value Databricks would accept as an endpoint name passes; any value Databricks would reject
# as an endpoint also fails here. The character class doubles as an anti-injection filter for
# *_call_llm_for_groups_ai_query*, which f-string-interpolates this name into a SQL string.
_AI_QUERY_ENDPOINT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,62}$")


def _resolve_ai_query_endpoint(model_name: str) -> str:
    """Map *LLMModelConfig.model_name* onto a Databricks Model Serving endpoint name.

    The default value carries a ``databricks/`` provider prefix for litellm compatibility; the
    SQL ``ai_query`` function takes the bare endpoint name. A non-Databricks prefix means the user
    pointed at another provider, which the ai_query path does not support.

    The returned name is also character-validated against *_AI_QUERY_ENDPOINT_RE*: the value is
    interpolated into a SQL string built by *_call_llm_for_groups_ai_query*, so any character
    outside Databricks's own endpoint-name rules is treated as a SQL-injection attempt and
    rejected.
    """
    if not model_name:
        raise InvalidParameterError("model_name is required for AI explanations.")
    if "/" in model_name:
        provider, _, endpoint = model_name.partition("/")
        if provider != "databricks":
            raise InvalidParameterError(
                f"AI explanations require a Databricks serving endpoint, got provider {provider!r} "
                f"in model_name={model_name!r}. Use a Databricks Model Serving endpoint name."
            )
    else:
        endpoint = model_name
    if not _AI_QUERY_ENDPOINT_RE.match(endpoint):
        raise InvalidParameterError(
            f"ai_query endpoint name {endpoint!r} is not a valid Databricks Model Serving name. "
            f"Names must start with a letter and contain only letters, digits, '-', or '_' "
            f"(max 63 chars)."
        )
    return endpoint


def _format_contributions_sql(top_n: int, labels: dict[str, str] | None = None) -> Column:
    """Spark expression producing 'feat_a (82%), feat_b (11%)' from a ``mean_contributions`` map.

    Mirrors *format_contributions_map* but stays inside Spark so per-group prompts can be
    assembled without a driver-side loop. Null/empty maps yield 'unknown'; entries are sorted by
    absolute value descending and percentages are normalised against the L1 sum of |value|.

    *labels* maps an engineered feature name to its human label; when supplied, each key is
    rendered as its label ('amount_rel_baseline' -> 'amount vs its group baseline'), falling back
    to the raw key for anything unmapped. The map keys and values are user-derived (column names),
    so both are escaped before interpolation. Rendering happens here, after redaction has already
    dropped sensitive keys upstream in *_aggregate_groups_spark*, so labelling never re-exposes a
    redacted feature.
    """
    entries = "filter(map_entries(`mean_contributions`), e -> e.value is not null)"
    sorted_entries = (
        f"array_sort({entries}, (a, b) -> "
        f"case when abs(b.value) > abs(a.value) then 1 "
        f"when abs(b.value) < abs(a.value) then -1 else 0 end)"
    )
    top = f"slice({sorted_entries}, 1, {int(top_n)})"
    abs_sum = f"aggregate({sorted_entries}, 0.0D, (acc, e) -> acc + abs(e.value))"
    if labels:
        pairs = ", ".join(f"'{_sql_string_literal(k)}', '{_sql_string_literal(v)}'" for k, v in labels.items())
        key_expr = f"coalesce(element_at(map({pairs}), e.key), e.key)"
    else:
        key_expr = "e.key"
    formatted = (
        f"transform({top}, e -> concat({key_expr}, ' (', "
        f"cast(round((abs(e.value) / case when {abs_sum} = 0 then 1 else {abs_sum} end) * 100) as int), '%)'))"
    )
    sql = (
        f"case when `mean_contributions` is null or size({entries}) = 0 then 'unknown' "
        f"else concat_ws(', ', {formatted}) end"
    )
    return F.expr(sql)


def _build_ai_query_prompt_column(
    ctx: ExplanationContext,
    is_ensemble: bool,
    drift_summary: str,
) -> Column:
    """Assemble the per-row prompt string sent to ``ai_query``.

    Per-group fields come from columns added by *_aggregate_groups_spark*; per-run fields are
    constants for the whole call. The shared header (*_AI_QUERY_PROMPT_HEADER*) holds the
    instructions and field semantics.
    """
    baseline_grouping = _baseline_grouping_str(ctx.feature_metadata)
    confidence_expr = (
        F.when((F.col("mean_std").isNull()) | F.lit(not is_ensemble), F.lit("n/a"))
        .when(F.col("mean_std") < F.lit(_CONFIDENCE_HIGH_BELOW), F.lit("high"))
        .when(F.col("mean_std") < F.lit(_CONFIDENCE_MIXED_BELOW), F.lit("mixed"))
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
        F.lit("attribution_basis: "),
        F.lit(attribution_semantics(ctx.algorithm)),
        F.lit("\n"),
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
        F.lit("baseline_grouping: "),
        F.lit(baseline_grouping),
        F.lit("\n"),
        F.lit("threshold: "),
        F.lit(str(ctx.threshold)),
        F.lit("\n"),
        F.lit("drift_summary: "),
        F.lit(drift_summary or "none"),
    )


def _aggregate_groups_spark(
    anomalous: DataFrame,
    pattern_col: str,
    contributions_col: str,
    severity_col: str,
    score_std_col: str,
    redact_set: frozenset[str],
    max_groups: int,
) -> tuple[DataFrame, int, int, int]:
    """Aggregate anomalous rows into per-pattern group metadata, kept inside Spark.

    Returns the per-pattern aggregate as a DataFrame so the LLM call can run on executors via
    ``ai_query``, with no driver collect of LLM payloads. ``max_groups`` is honoured as a cost
    cap (top-N by ``group_size * group_avg_severity``); rows beyond the cap fall through with
    null explanations via the left-join in *_attach_explanation_struct*.
    """
    primary = anomalous.groupBy(pattern_col).agg(
        F.count(F.lit(1)).alias("group_size"),
        F.avg(severity_col).alias("group_avg_severity"),
        F.min(severity_col).alias("severity_min"),
        F.max(severity_col).alias("severity_max"),
        F.avg(score_std_col).alias("mean_std"),
    )
    exploded = anomalous.select(F.col(pattern_col), F.explode(F.col(contributions_col)).alias("__k", "__v"))
    if redact_set:
        exploded = exploded.filter(~F.col("__k").isin(list(redact_set)))
    per_key_mean = exploded.groupBy(pattern_col, "__k").agg(F.avg("__v").alias("__mean"))
    per_pattern_contrib = per_key_mean.groupBy(pattern_col).agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("__k"), F.col("__mean")))).alias("mean_contributions")
    )

    # Single-action total/kept accounting: window aggregates over ``primary`` carry run-level
    # totals onto each ranked row, so collecting the small (<= ``max_groups``) ranked
    # projection gives us totals + kept counts without a second action on ``primary`` and
    # without forcing materialisation of the join with ``per_pattern_contrib`` (which stays
    # lazy and is consumed by ``ai_query`` on executors). We deliberately do not ``.cache()``
    # ``anomalous``: PERSIST TABLE is unsupported on Databricks serverless and a library should
    # not make storage decisions for the caller.
    # Spark Connect emits the single-partition window warning at construction time, so the
    # suppression must wrap both the DataFrame build and the action.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*No Partition Defined for Window operation.*")
        whole = Window.partitionBy()
        rank_window = Window.orderBy(F.desc("__rank_score"), F.asc(pattern_col))
        ranked_with_totals = (
            primary.withColumn("__rank_score", F.col("group_size") * F.col("group_avg_severity"))
            .withColumn("__total_groups", F.count(F.lit(1)).over(whole))
            .withColumn("__total_rows", F.sum("group_size").over(whole))
            .withColumn("__rn", F.row_number().over(rank_window))
            .filter(F.col("__rn") <= max_groups)
            .drop("__rn", "__rank_score")
        )
        ranked_local = ranked_with_totals.select(pattern_col, "group_size", "__total_groups", "__total_rows").collect()

    if ranked_local:
        total_groups = int(ranked_local[0]["__total_groups"] or 0)
        total_rows = int(ranked_local[0]["__total_rows"] or 0)
    else:
        total_groups = 0
        total_rows = 0
    kept_rows_count = sum(int(r["group_size"] or 0) for r in ranked_local)
    dropped_groups_count = max(0, total_groups - len(ranked_local))
    dropped_rows_count = max(0, total_rows - kept_rows_count)

    # Rebuild the kept projection by filtering ``primary`` to the already-selected top-N pattern
    # keys, rather than carrying the ranking window into the returned (lazy) DataFrame. ``ai_query``
    # consumes this downstream — outside the warning-suppression block above — and callers re-action
    # the scored DataFrame repeatedly, so a window left in this lineage re-emits the single-partition
    # warning on every action. The kept keys are already known from ``ranked_local``, so an ``isin``
    # filter is equivalent to the windowed top-N selection and keeps the returned lineage window-free.
    kept_pattern_values = [r[pattern_col] for r in ranked_local]
    kept = primary.filter(F.col(pattern_col).isin(kept_pattern_values)).join(
        per_pattern_contrib, on=pattern_col, how="left"
    )
    return kept, dropped_groups_count, dropped_rows_count, total_groups


def _call_llm_for_groups_ai_query(
    kept_groups_sdf: DataFrame,
    ctx: ExplanationContext,
    is_ensemble: bool,
    drift_summary: str,
) -> DataFrame:
    """Invoke ``ai_query`` once per retained group inside Spark and return rows shaped for
    *_attach_explanation_struct* (pattern key + narrative/business_impact/action + group stats).

    Budget caps (max_tokens, temperature) come from *LLMModelConfig* via *modelParameters*.
    Sanitisation of the response (control-char strip + length cap) is applied as Spark
    expressions so a misbehaving model can't forge log entries or write multi-KB output.
    """
    llm_cfg = ctx.llm_model_config or LLMModelConfig()
    endpoint = _resolve_ai_query_endpoint(llm_cfg.model_name)
    pattern_col = ctx.pattern_col
    # feature_contributions is rendered with human labels (empty map -> raw keys); it feeds both the
    # prompt and the struct's top_drivers, so a reader sees the same human phrasing the LLM did.
    enriched = kept_groups_sdf.withColumn(
        "feature_contributions",
        _format_contributions_sql(_TOP_N, _human_labels(ctx.feature_metadata)),
    ).withColumn(
        "__prompt",
        _build_ai_query_prompt_column(ctx, is_ensemble, drift_summary),
    )

    # ai_query is parameterised through the SQL string. *endpoint* is matched against the strict
    # serving-endpoint pattern in *_resolve_ai_query_endpoint*; max_tokens/temperature come from
    # validated config fields. responseFormat is built from *_PROMPT_OUTPUT_FIELDS* (no user
    # input). failOnError => false: per-row failures (after the platform's internal retries)
    # yield NULL instead of aborting the whole job, so one bad completion doesn't kill an
    # otherwise-successful scoring run. The platform's retry count is not exposed, so
    # *max_retries* on *LLMModelConfig* has no effect on this path.
    raw = enriched.withColumn(
        "__raw_response",
        F.expr(
            f"ai_query('{endpoint}', __prompt, "
            f"modelParameters => named_struct('max_tokens', {int(llm_cfg.max_tokens)}, "
            f"'temperature', {float(llm_cfg.temperature)}), "
            f"responseFormat => '{_sql_string_literal(_AI_QUERY_RESPONSE_FORMAT)}', "
            f"failOnError => false)"
        ),
    )

    # ``ai_query`` is endpoint-agnostic at the SQL level, but its *output column type* depends
    # on the endpoint. Two shapes seen in practice:
    #   - STRING: the raw JSON text (e.g. ``databricks-llama-4-maverick``).
    #   - STRUCT<result: STRING, errorMessage: STRING>: the failOnError envelope used by some
    #     endpoints (e.g. ``databricks-gemma-3-12b``); ``result`` holds the JSON text and
    #     ``errorMessage`` is set on per-row failures.
    # Detect the shape from the actual schema rather than hard-coding either, so the same code
    # works against any serving endpoint without per-endpoint configuration.
    raw_field = raw.schema["__raw_response"]
    if isinstance(raw_field.dataType, StructType):
        json_text_expr: Column = F.col("__raw_response.result")
    else:
        json_text_expr = F.col("__raw_response")

    response_schema = StructType([StructField(name, StringType(), True) for name, _ in _PROMPT_OUTPUT_FIELDS])
    parsed = raw.withColumn("__parsed", F.from_json(json_text_expr, response_schema))

    def _sanitize(col_name: str) -> Column:
        # Strip C0/DEL control chars (CWE-117) and length-cap each LLM output field. Treat the
        # response as untrusted (OWASP LLM06): a jailbroken model can return control chars or
        # multi-KB strings. When the field overruns the cap, trim back to a word boundary
        # (drop the trailing partial word) instead of cutting mid-word.
        cleaned = f"regexp_replace(__parsed.{col_name}, '[\\\\x00-\\\\x1f\\\\x7f]', ' ')"
        capped = f"substring({cleaned}, 1, {_LLM_FIELD_MAX_LEN})"
        return F.expr(
            f"case when length({cleaned}) > {_LLM_FIELD_MAX_LEN} "
            f"then regexp_replace({capped}, '\\\\s\\\\S*$', '') else {cleaned} end"
        )

    return parsed.select(
        F.col(pattern_col),
        _sanitize("narrative").alias("narrative"),
        _sanitize("business_impact").alias("business_impact"),
        _sanitize("action").alias("action"),
        # Human-labelled drivers carried through for the struct's top_drivers. Built by us from the
        # contributions map, not the LLM, so it needs no sanitisation.
        F.col("feature_contributions").alias("top_drivers"),
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
    pattern_col = ctx.pattern_col
    joined = df_with_pattern.join(result_sdf, on=pattern_col, how="left")
    return joined.withColumn(
        ctx.ai_explanation_col,
        F.when(
            (F.col(ctx.severity_col) >= F.lit(ctx.threshold)) & F.col("narrative").isNotNull(),
            F.struct(
                F.col("narrative").alias("narrative"),
                F.col("business_impact").alias("business_impact"),
                F.col(pattern_col).alias("top_features"),
                F.col("top_drivers").alias("top_drivers"),
                F.col("action").alias("action"),
                F.col("group_size").alias("group_size"),
                F.col("group_avg_severity").alias("group_avg_severity"),
            ),
        ).otherwise(_build_empty_explanation_column()),
    ).drop(
        pattern_col,
        "narrative",
        "business_impact",
        "top_drivers",
        "action",
        "group_size",
        "group_avg_severity",
    )


def _log_dropped_groups(dropped_groups_count: int, dropped_rows_count: int, max_groups: int) -> None:
    if dropped_groups_count:
        logger.warning(
            f"ai_explanation: {dropped_groups_count} groups covering {dropped_rows_count} rows "
            f"exceeded max_groups={max_groups}; their ai_explanation will be null."
        )


def _endpoint_reachable(spark: object, endpoint: str) -> bool:
    """One-shot probe: can this workspace reach the ``ai_query`` serving endpoint?

    AI explanations are on by default, but ``ai_query`` runs lazily — a missing or un-entitled
    endpoint (no Foundation Model APIs, wrong region, etc.) would otherwise fail the whole scoring
    run at action time. Probe once with a 1-token call; on any failure the caller skips
    explanations and logs a warning instead of breaking scoring.
    """
    try:
        spark.sql(  # type: ignore[attr-defined]
            f"SELECT ai_query('{endpoint}', 'ping', modelParameters => named_struct('max_tokens', 1))"
        ).collect()
        return True
    except Exception as exc:
        detail = str(exc).replace("\n", " ").replace("\r", " ")[:200]
        logger.warning(
            f"ai_explanation: serving endpoint {endpoint!r} not reachable; skipping AI explanations. {detail}"
        )
        return False


def probe_endpoint_reachable(spark: object, llm_model_config: LLMModelConfig | None) -> bool:
    """Resolve the serving endpoint from *llm_model_config* and probe its reachability once.

    Public entry point so a caller that invokes ``add_explanation_column`` repeatedly within a
    single scoring run (e.g. *score_segmented*, once per segment) can probe **once** up front and
    pass the result down via ``add_explanation_column(..., endpoint_reachable=...)``, instead of
    paying one billable 1-token ``ai_query`` probe per segment.

    Raises:
        InvalidParameterError: When *model_name* does not resolve to a Databricks serving endpoint.
    """
    endpoint = _resolve_ai_query_endpoint((llm_model_config or LLMModelConfig()).model_name)
    return _endpoint_reachable(spark, endpoint)


def _add_explanation_column_ai_query(
    df_with_pattern: DataFrame,
    ctx: ExplanationContext,
    is_ensemble: bool,
    drift_summary: str,
    endpoint_reachable: bool | None = None,
) -> DataFrame:
    """ai_query-path explainer: SQL ``ai_query`` on executors, results pinned once per run.

    The per-group LLM results are materialised (collected and rebuilt as a local DataFrame)
    before being joined onto the scored rows. Left lazy, the join would re-invoke ``ai_query``
    for every downstream action on the scored DataFrame (each ``count``/``display``/write is a
    fresh execution of the lineage), multiplying LLM cost and latency per action and breaking
    the documented "one call per group per scoring run" cost model. The collected payload is
    small and bounded: at most ``max_groups`` rows, each holding three length-capped text fields.
    """
    redact_set = redaction_set(ctx.redact_columns, ctx.feature_metadata)
    anomalous = df_with_pattern.filter(F.col(ctx.severity_col) >= F.lit(ctx.threshold))
    kept_sdf, dropped_groups_count, dropped_rows_count, total_groups = _aggregate_groups_spark(
        anomalous,
        pattern_col=ctx.pattern_col,
        contributions_col=ctx.contributions_col,
        severity_col=ctx.severity_col,
        score_std_col=ctx.score_std_col,
        redact_set=redact_set,
        max_groups=ctx.max_groups,
    )
    # No anomalies above threshold → short-circuit to a null struct, no ai_query call.
    if total_groups == 0:
        return df_with_pattern.withColumn(ctx.ai_explanation_col, _build_empty_explanation_column()).drop(
            ctx.pattern_col
        )
    # Endpoint resolution is a config check (raises for a non-Databricks provider); reachability is
    # a runtime check — if the endpoint isn't available, degrade to null explanations + a warning
    # rather than failing the scoring run (AI explanations are on by default). *endpoint_reachable*
    # is passed by callers that already probed once for the whole run (e.g. score_segmented across
    # segments); when None we probe here, so direct single callers keep working unchanged.
    if endpoint_reachable is None:
        endpoint = _resolve_ai_query_endpoint((ctx.llm_model_config or LLMModelConfig()).model_name)
        endpoint_reachable = _endpoint_reachable(df_with_pattern.sparkSession, endpoint)
    if not endpoint_reachable:
        return df_with_pattern.withColumn(ctx.ai_explanation_col, _build_empty_explanation_column()).drop(
            ctx.pattern_col
        )
    _log_dropped_groups(dropped_groups_count, dropped_rows_count, ctx.max_groups)
    result_sdf = _call_llm_for_groups_ai_query(kept_sdf, ctx, is_ensemble, drift_summary)
    # Pin the LLM responses: one ai_query execution per scoring run, regardless of how many
    # actions the caller takes on the returned DataFrame afterwards.
    result_rows = result_sdf.collect()
    result_local = df_with_pattern.sparkSession.createDataFrame(result_rows, schema=result_sdf.schema)
    return _attach_explanation_struct(df_with_pattern, result_local, ctx)


def add_explanation_column(
    df: DataFrame,
    ctx: ExplanationContext,
    is_ensemble: bool,
    drift_summary: str = "none",
    endpoint_reachable: bool | None = None,
) -> DataFrame:
    """Add the AI explanation column to df using the group-based algorithm.

    Anomalous rows are bucketed by a deterministic pattern key — the sorted top-2 contributing SHAP
    features. The LLM is called once per group via the Spark SQL ``ai_query`` function against a
    Databricks Model Serving endpoint, and every row in that group receives the same
    narrative/business_impact/action, plus the group's size and mean severity. Rows below threshold
    or in groups exceeding ``ctx.max_groups`` receive a null struct.

    Preconditions (caller's responsibility):
      - df has ctx.score_std_col, ctx.severity_col, and ctx.contributions_col.

    Args:
        df: Scored DataFrame to annotate with the explanation column.
        ctx: Explanation inputs (columns, threshold, model, redaction, budget).
        is_ensemble: Whether the scoring model is an ensemble (drives the confidence label).
        drift_summary: Baseline-drift summary string for the prompt, or "none".
        endpoint_reachable: Pre-computed serving-endpoint reachability. When None (default) the
            endpoint is probed here with a single 1-token ai_query call.

    Raises:
      InvalidParameterError: When *model_name* does not resolve to a Databricks serving endpoint.
    """
    redact_set = redaction_set(ctx.redact_columns, ctx.feature_metadata)
    df_with_pattern = df.withColumn(ctx.pattern_col, _pattern_spark_expr(ctx.contributions_col, redact_set))
    return _add_explanation_column_ai_query(
        df_with_pattern, ctx, is_ensemble, drift_summary, endpoint_reachable=endpoint_reachable
    )
