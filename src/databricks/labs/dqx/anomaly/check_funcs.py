"""
Check functions for row anomaly detection.

Facade: public rule entry point. Orchestration and scoring live in sibling modules.
"""

import dataclasses
import logging
import uuid
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from databricks.labs.dqx.anomaly.model_discovery import fetch_model_columns_and_segments
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig, ScoringOutputColumns
from databricks.labs.dqx.anomaly.scoring_utils import check_reserved_row_id_columns
from databricks.labs.dqx.anomaly.scoring_orchestrator import run_anomaly_scoring
from databricks.labs.dqx.anomaly.explainability import SHAP_AVAILABLE
from databricks.labs.dqx.anomaly.validation import validate_fully_qualified_name
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import register_rule

logger = logging.getLogger(__name__)

_LLM_MODEL_CONFIG_KEYS = {f.name for f in dataclasses.fields(LLMModelConfig)}


def _coerce_llm_model_config(value: LLMModelConfig | dict | None) -> LLMModelConfig | None:
    """Accept either an LLMModelConfig instance or a plain dict (YAML/metadata path).

    Dicts are validated against the known field set so typos surface early; anything else
    raises InvalidParameterError. None passes through so downstream code applies its default.
    """
    if value is None or isinstance(value, LLMModelConfig):
        return value
    if isinstance(value, dict):
        unknown = set(value) - _LLM_MODEL_CONFIG_KEYS
        if unknown:
            raise InvalidParameterError(
                f"ai_explanation_llm_model_config has unknown keys: {sorted(unknown)}. "
                f"Allowed keys: {sorted(_LLM_MODEL_CONFIG_KEYS)}."
            )
        return LLMModelConfig(**value)
    raise InvalidParameterError(
        "ai_explanation_llm_model_config must be an LLMModelConfig instance or a dict with keys "
        "{model_name, api_key, api_base}."
    )


def _validate_required_names(model_name: str, registry_table: str) -> None:
    if not model_name:
        raise InvalidParameterError(
            "model_name parameter is required. Example: has_no_row_anomalies(model_name='catalog.schema.my_model', ...)"
        )
    if not registry_table:
        raise InvalidParameterError(
            "registry_table parameter is required. Example: registry_table='catalog.schema.dqx_anomaly_models'"
        )
    validate_fully_qualified_name(model_name, label="model_name")
    validate_fully_qualified_name(registry_table, label="registry_table")


def _validate_thresholds(threshold: float, drift_threshold: float | None) -> None:
    if not 0.0 <= float(threshold) <= 100.0:
        raise InvalidParameterError("threshold must be between 0.0 and 100.0.")
    if drift_threshold is not None and drift_threshold <= 0:
        raise InvalidParameterError("drift_threshold must be greater than 0 when provided.")


def _validate_explanation_flags(enable_contributions: bool) -> None:
    if enable_contributions and not SHAP_AVAILABLE:
        raise InvalidParameterError(
            "enable_contributions=True requires the 'shap' dependency. "
            "Install anomaly extras: pip install databricks-labs-dqx[anomaly]"
        )


def _resolve_ai_explanation_flag(enable_contributions: bool, enable_ai_explanation: bool) -> bool:
    """AI explanations use SHAP contributions as their input, so they require
    *enable_contributions*. Both default to True; if a caller turns contributions off (e.g. to
    skip the SHAP cost) we disable explanations with a warning rather than raising, so the cheap
    opt-out stays frictionless.
    """
    if enable_ai_explanation and not enable_contributions:
        logger.warning(
            "AI explanations require SHAP contributions; disabling enable_ai_explanation because "
            "enable_contributions=False."
        )
        return False
    return enable_ai_explanation


def _validate_anomaly_check_args(
    model_name: str,
    registry_table: str,
    threshold: float,
    drift_threshold: float | None,
    enable_contributions: bool,
    redact_columns: list[str] | None,
    max_groups: int,
) -> None:
    """Validate has_no_row_anomalies arguments. Raises InvalidParameterError on failure."""
    _validate_required_names(model_name, registry_table)
    _validate_thresholds(threshold, drift_threshold)
    _validate_explanation_flags(enable_contributions)

    if redact_columns is not None:
        if not isinstance(redact_columns, list) or not all(isinstance(c, str) and c for c in redact_columns):
            raise InvalidParameterError("redact_columns must be a list of non-empty column name strings.")

    if not isinstance(max_groups, int) or isinstance(max_groups, bool) or max_groups <= 0:
        raise InvalidParameterError("max_groups must be a positive integer.")


@register_rule("dataset")
def has_no_row_anomalies(
    model_name: str,
    registry_table: str,
    threshold: float = 95.0,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    enable_contributions: bool = True,
    enable_confidence_std: bool = False,
    enable_ai_explanation: bool = True,
    ai_explanation_llm_model_config: LLMModelConfig | dict | None = None,
    redact_columns: list[str] | None = None,
    max_groups: int = 500,
    *,
    driver_only: bool = False,
) -> tuple[Column, Any, str]:
    """Check that records are not anomalous according to a trained model(s).

    Auto-discovery:
    - columns: Inferred from model registry
    - segmentation: Inferred from model registry (checks if model is segmented)

    Output columns:
    - _dq_info: Array of structs (one element per dataset-level check). For example:
      - _dq_info[0].anomaly.score: Raw anomaly score (model-relative)
      - _dq_info[0].anomaly.severity_percentile: Severity percentile (0–100)
      - _dq_info[0].anomaly.is_anomaly: Boolean flag
      - _dq_info[0].anomaly.threshold: Severity percentile threshold used (0–100)
      - _dq_info[0].anomaly.model: Model name
      - _dq_info[0].anomaly.segment: Segment values (if segmented)
      - _dq_info[0].anomaly.contributions: SHAP contributions as percentages (0–100)
      - _dq_info[0].anomaly.confidence_std: Ensemble std (if requested)

    Notes:
        DQX always scores using the columns the model was trained on.
        DQX aligns scored rows back to the input using an internal row id and removes it before returning.
        Segmentation is inferred from the trained model configuration.

    Args:
        model_name: Model name (REQUIRED). Provide the fully qualified model name
            in catalog.schema.table format returned from train().
        registry_table: Registry table (REQUIRED). Provide the fully qualified table
            name in catalog.schema.table format.
        threshold: Severity percentile threshold (0–100, default 95).
            Records with severity_percentile >= threshold are flagged as anomalous.
            Higher threshold = stricter detection (fewer anomalies).
        row_filter: Optional SQL expression (e.g. \"region = 'US'\"). Only rows matching
            this expression are scored; others are left in the output with null anomaly
            result. Auto-injected from the check filter.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        enable_contributions: Include SHAP feature contributions for explainability (default True).
            Per-feature contributions are added to _dq_info; adds scoring cost. Requires the SHAP
            library (installed with the anomaly extra). Set False to skip the SHAP cost (this also
            disables AI explanations, since they use contributions as input).
        enable_confidence_std: Include ensemble confidence scores in _dq_info and top-level (default False).
            Automatically available when training with ensemble_size > 1 (default is 3).
        enable_ai_explanation: Add a human-readable LLM explanation for each anomalous row
            (default True). Uses enable_contributions as input; if contributions are off, explanations
            are disabled (with a warning) rather than erroring. The LLM call runs in Spark via
            ``ai_query`` against a Databricks Model Serving endpoint — no extra dependencies. If that
            endpoint is unreachable (e.g. no Foundation Model APIs in the workspace), explanations are
            skipped with a warning and scoring still completes. Output is in
            _dq_info[0].anomaly.ai_explanation, and is AI-generated from the anomaly signal (feature
            names + SHAP + severity), not grounded in catalog metadata.
        ai_explanation_llm_model_config: LLM model configuration for AI explanations (named
            distinctly from the check's *model_name* to avoid confusion). Defaults to
            LLMModelConfig() (model_name='databricks/databricks-claude-sonnet-4-5'). Its *model_name*
            must resolve to a Databricks Model Serving endpoint (with or without the ``databricks/``
            prefix); the ``ai_query`` call uses the bare endpoint name.

            When wrapping this check in DQDatasetRule / DQRowRule, applying it via
            apply_checks / apply_checks_by_metadata, or declaring it in YAML: **pass a dict**
            with keys ``{"model_name", "api_key", "api_base"}``. The rule pipeline
            normalizes check arguments and does not accept custom dataclass instances, so an
            LLMModelConfig object there raises ``TypeError: Unsupported type for normalization``.
            The dict is coerced back to LLMModelConfig inside the check.

            When calling this function directly (not via a rule), either a dict or an
            LLMModelConfig instance is accepted. The simplest dict form sets only *model_name*
            to a Databricks Model Serving endpoint. See the AI Explanations section of the Row
            Anomaly Detection reference docs for a full example.
        redact_columns: Column names to exclude from the LLM prompt. Filters SHAP contribution
            map keys, the top-2 pattern key, and — when the scored model is segmented — any
            matching segment key (emitted as ``key=<redacted>`` so sensitive segmentation values
            never reach the prompt).
        max_groups: Maximum number of distinct (segment, pattern) groups the LLM is called for
            per scoring run (default 500). Groups beyond this cap — ranked by
            group_size * group_avg_severity — get a null ai_explanation; a warning is logged.
            Note: for segmented models the cap is split across eligible segments with a floor of
            one call each, so when ``max_groups`` is smaller than the number of eligible segments
            the effective call count is the segment count (a warning is logged). Size
            ``max_groups`` at or above your expected eligible-segment count to keep cost bounded.
        driver_only: If True, score on the driver (no UDF). Use for tests or Spark Connect when
            worker UDF dependencies are not available. Default False for production.

    Returns:
        Tuple of condition expression, apply function and info column name.

    Example:
        Access anomaly metadata via _dq_info (array; first check = index 0):
        >>> df_scored.select(col("_dq_info").getItem(0).getField("anomaly").getField("score"), ...)
        >>> df_scored.filter(col("_dq_info").getItem(0).getField("anomaly").getField("is_anomaly"))
    """
    llm_model_config = _coerce_llm_model_config(ai_explanation_llm_model_config)
    # AI explanations need SHAP contributions; if contributions are off, disable explanations
    # (with a warning) rather than failing — both default on, so this only triggers when a caller
    # explicitly opts out of contributions.
    enable_ai_explanation = _resolve_ai_explanation_flag(enable_contributions, enable_ai_explanation)

    _validate_anomaly_check_args(
        model_name=model_name,
        registry_table=registry_table,
        threshold=threshold,
        drift_threshold=drift_threshold,
        enable_contributions=enable_contributions,
        redact_columns=redact_columns,
        max_groups=max_groups,
    )

    row_id_col = f"__dqx_row_id_{uuid.uuid4().hex}"
    output_columns = ScoringOutputColumns(
        score=f"__dq_anomaly_score_{uuid.uuid4().hex}",
        score_std=f"__dq_anomaly_score_std_{uuid.uuid4().hex}",
        contributions=f"__dq_anomaly_contributions_{uuid.uuid4().hex}",
        severity=f"__dq_severity_percentile_{uuid.uuid4().hex}",
        ai_explanation=f"__dq_ai_explanation_{uuid.uuid4().hex}",
        info=f"__dqx_info_{uuid.uuid4().hex}",
        pattern=f"__dq_anomaly_pattern_{uuid.uuid4().hex}",
    )

    def apply(df: DataFrame) -> DataFrame:
        check_reserved_row_id_columns(df)
        df_to_score = df.withColumn(row_id_col, F.monotonically_increasing_id())
        columns, segment_by = fetch_model_columns_and_segments(df_to_score, model_name, registry_table)

        config = ScoringConfig(
            columns=columns,
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
            merge_columns=[row_id_col],
            row_filter=row_filter,
            drift_threshold=drift_threshold,
            enable_contributions=enable_contributions,
            enable_confidence_std=enable_confidence_std,
            enable_ai_explanation=enable_ai_explanation,
            llm_model_config=llm_model_config,
            redact_columns=redact_columns or [],
            max_groups=max_groups,
            segment_by=segment_by,
            driver_only=driver_only,
            output_columns=output_columns,
        )

        result = run_anomaly_scoring(df_to_score, config, registry_table, model_name)
        return result.drop(row_id_col)

    message = F.concat_ws(
        "",
        F.lit("Anomaly severity "),
        F.round(F.col(output_columns.info).anomaly.severity_percentile, 1).cast("string"),
        F.lit(f" exceeded threshold {threshold}"),
    )
    condition_expr = F.col(output_columns.info).anomaly.is_anomaly
    return make_condition(condition_expr, message, "has_row_anomalies"), apply, output_columns.info
