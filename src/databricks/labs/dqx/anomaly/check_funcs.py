"""
Check functions for row anomaly detection.

Facade: public rule entry point. Orchestration and scoring live in sibling modules.
"""

import uuid
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from databricks.labs.dqx.anomaly.model_discovery import fetch_model_columns_and_segments
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_utils import check_reserved_row_id_columns
from databricks.labs.dqx.anomaly.scoring_orchestrator import run_anomaly_scoring
from databricks.labs.dqx.anomaly.explainability import SHAP_AVAILABLE
from databricks.labs.dqx.anomaly.validation import validate_fully_qualified_name
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import register_rule


@register_rule("dataset")
def has_no_row_anomalies(
    model_name: str,
    registry_table: str,
    threshold: float = 95.0,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
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
        driver_only: If True, score on the driver (no UDF). Use for tests or Spark Connect when
            worker UDF dependencies are not available. Default False for production.

    Returns:
        Tuple of condition expression, apply function and info column name.

    Example:
        Access anomaly metadata via _dq_info (array; first check = index 0):
        >>> df_scored.select(col("_dq_info").getItem(0).getField("anomaly").getField("score"), ...)
        >>> df_scored.filter(col("_dq_info").getItem(0).getField("anomaly").getField("is_anomaly"))
    """
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

    row_id_col = f"__dqx_row_id_{uuid.uuid4().hex}"
    score_col = f"__dq_anomaly_score_{uuid.uuid4().hex}"
    score_std_col = f"__dq_anomaly_score_std_{uuid.uuid4().hex}"
    contributions_col = f"__dq_anomaly_contributions_{uuid.uuid4().hex}"
    severity_col = f"__dq_severity_percentile_{uuid.uuid4().hex}"
    info_col = f"__dqx_info_{uuid.uuid4().hex}"

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
            drift_threshold_value=drift_threshold if drift_threshold is not None else 3.0,
            enable_contributions=enable_contributions,
            enable_confidence_std=enable_confidence_std,
            segment_by=segment_by,
            driver_only=driver_only,
            score_col=score_col,
            score_std_col=score_std_col,
            contributions_col=contributions_col,
            severity_col=severity_col,
            info_col=info_col,
        )

        result = run_anomaly_scoring(df_to_score, config, registry_table, model_name)
        return result.drop(row_id_col)

    message = F.concat_ws(
        "",
        F.lit("Anomaly severity "),
        F.round(F.col(info_col).anomaly.severity_percentile, 1).cast("string"),
        F.lit(f" exceeded threshold {threshold}"),
    )
    condition_expr = F.col(info_col).anomaly.is_anomaly
    return make_condition(condition_expr, message, "has_row_anomalies"), apply, info_col
