"""
Check functions for row anomaly detection.

Facade: public rule entry point. Orchestration and scoring live in sibling modules.
"""

import dataclasses
import importlib.util
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

DSPY_AVAILABLE = importlib.util.find_spec("dspy") is not None

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
                f"llm_model_config has unknown keys: {sorted(unknown)}. "
                f"Allowed keys: {sorted(_LLM_MODEL_CONFIG_KEYS)}."
            )
        return LLMModelConfig(**value)
    raise InvalidParameterError(
        "llm_model_config must be an LLMModelConfig instance or a dict with keys {model_name, api_key, api_base}."
    )


def _validate_anomaly_check_args(
    model_name: str,
    registry_table: str,
    threshold: float,
    drift_threshold: float | None,
    enable_contributions: bool,
    enable_ai_explanation: bool,
    redact_columns: list[str] | None,
    max_groups: int,
) -> None:
    """Validate has_no_row_anomalies arguments. Raises InvalidParameterError on failure."""
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

    if not 0.0 <= float(threshold) <= 100.0:
        raise InvalidParameterError("threshold must be between 0.0 and 100.0.")

    if drift_threshold is not None and drift_threshold <= 0:
        raise InvalidParameterError("drift_threshold must be greater than 0 when provided.")

    if enable_contributions and not SHAP_AVAILABLE:
        raise InvalidParameterError(
            "enable_contributions=True requires the 'shap' dependency. "
            "Install anomaly extras: pip install databricks-labs-dqx[anomaly]"
        )

    if enable_ai_explanation and not enable_contributions:
        raise InvalidParameterError(
            "enable_ai_explanation=True requires enable_contributions=True. "
            "SHAP contributions are used as input to the LLM explanation."
        )

    if enable_ai_explanation and not DSPY_AVAILABLE:
        raise InvalidParameterError(
            "enable_ai_explanation=True requires the 'dspy' dependency. "
            "Install: pip install databricks-labs-dqx[anomaly,llm]"
        )

    if redact_columns is not None and not isinstance(redact_columns, list):
        raise InvalidParameterError("redact_columns must be a list of column name strings.")

    if not isinstance(max_groups, int) or isinstance(max_groups, bool) or max_groups <= 0:
        raise InvalidParameterError("max_groups must be a positive integer.")


@register_rule("dataset")
def has_no_row_anomalies(
    model_name: str,
    registry_table: str,
    threshold: float = 95.0,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
    enable_ai_explanation: bool = False,
    llm_model_config: LLMModelConfig | dict | None = None,
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
        enable_contributions: Include SHAP feature contributions for explainability (default False).
            Set True to get per-feature contributions in _dq_info; adds significant scoring cost.
            Requires SHAP library when True.
        enable_confidence_std: Include ensemble confidence scores in _dq_info and top-level (default False).
            Automatically available when training with ensemble_size > 1 (default is 3).
        enable_ai_explanation: If True, add a human-readable LLM explanation for each anomalous row
            (default False). Requires enable_contributions=True and the [llm] extra.
            Output is in _dq_info[0].anomaly.ai_explanation.
        llm_model_config: LLM model configuration. Defaults to LLMModelConfig()
            (databricks/databricks-claude-sonnet-4-5).

            When wrapping this check in DQDatasetRule / DQRowRule, applying it via
            apply_checks / apply_checks_by_metadata, or declaring it in YAML: **pass a dict**
            with keys ``{"model_name", "api_key", "api_base"}``. The rule pipeline
            normalizes check arguments and does not accept custom dataclass instances, so an
            LLMModelConfig object there raises ``TypeError: Unsupported type for normalization``.
            The dict is coerced back to LLMModelConfig inside the check.

            When calling this function directly (not via a rule), either a dict or an
            LLMModelConfig instance is accepted.

            Example (dict form, works for both paths)::

                "llm_model_config": {
                    "model_name": "databricks-qwen3-next-80b-a3b-instruct",
                    "api_key": "<pat_token>",
                    "api_base": "https://<gateway-id>.ai-gateway.cloud.databricks.com/mlflow/v1",
                }

            When ``api_base`` is set, the model is routed through litellm's OpenAI-compatible
            adapter (``openai/<model>``) — use this for AI Gateway and other OpenAI-compatible
            endpoints. ``api_key`` / ``api_base`` also fall back to ``OPENAI_API_KEY`` /
            ``OPENAI_API_BASE`` env vars when unset on the config.
        redact_columns: Feature names to exclude from the LLM prompt. Only feature names and SHAP
            percentages are ever sent — raw data values are never included.
        max_groups: Maximum number of distinct (segment, pattern) groups the LLM is called for
            per scoring run (default 500). Groups beyond this cap — ranked by
            group_size * group_avg_severity — get a null ai_explanation; a warning is logged.
        driver_only: If True, score on the driver (no UDF). Use for tests or Spark Connect when
            worker UDF dependencies are not available. Default False for production.

    Returns:
        Tuple of condition expression, apply function and info column name.

    Example:
        Access anomaly metadata via _dq_info (array; first check = index 0):
        >>> df_scored.select(col("_dq_info").getItem(0).getField("anomaly").getField("score"), ...)
        >>> df_scored.filter(col("_dq_info").getItem(0).getField("anomaly").getField("is_anomaly"))
    """
    _validate_anomaly_check_args(
        model_name=model_name,
        registry_table=registry_table,
        threshold=threshold,
        drift_threshold=drift_threshold,
        enable_contributions=enable_contributions,
        enable_ai_explanation=enable_ai_explanation,
        redact_columns=redact_columns,
        max_groups=max_groups,
    )

    llm_model_config = _coerce_llm_model_config(llm_model_config)

    row_id_col = f"__dqx_row_id_{uuid.uuid4().hex}"
    output_columns = ScoringOutputColumns(
        score=f"__dq_anomaly_score_{uuid.uuid4().hex}",
        score_std=f"__dq_anomaly_score_std_{uuid.uuid4().hex}",
        contributions=f"__dq_anomaly_contributions_{uuid.uuid4().hex}",
        severity=f"__dq_severity_percentile_{uuid.uuid4().hex}",
        ai_explanation=f"__dq_ai_explanation_{uuid.uuid4().hex}",
        info=f"__dqx_info_{uuid.uuid4().hex}",
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
