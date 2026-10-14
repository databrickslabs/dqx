import inspect
import logging
from decimal import Decimal
from typing import cast
from unittest.mock import create_autospec, patch

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly import check_funcs
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.model_discovery import get_quantile_points_for_severity, get_record_for_discovery
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_strategies import GlobalModelScoringStrategy, resolve_scoring_strategy
from databricks.labs.dqx.anomaly.model_config import AnomalyModelRecord
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.errors import InvalidParameterError


def test_has_no_row_anomalies_requires_model_name():
    """model_name is required (non-empty)."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(model_name="", registry_table="catalog.schema.table")
    assert "model_name parameter is required" in str(exc_info.value)

    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(model_name=None, registry_table="catalog.schema.table")
    assert "model_name parameter is required" in str(exc_info.value)


def test_has_no_row_anomalies_requires_registry_table():
    """registry_table is required (non-empty)."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(model_name="catalog.schema.model", registry_table="")
    assert "registry_table parameter is required" in str(exc_info.value)

    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(model_name="catalog.schema.model", registry_table=None)
    assert "registry_table parameter is required" in str(exc_info.value)


def test_has_no_row_anomalies_requires_fully_qualified_model():
    """model_name must be fully qualified (catalog.schema.table)."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(model_name="schema.model", registry_table="catalog.schema.table")
    assert "model_name must be fully qualified" in str(exc_info.value)


def test_has_no_row_anomalies_requires_fully_qualified_registry_table():
    """registry_table must be fully qualified (catalog.schema.table)."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(model_name="catalog.schema.model", registry_table="schema.table")
    assert "registry_table must be fully qualified" in str(exc_info.value)


@pytest.mark.parametrize("threshold", [100.0, 120.0, -0.1, float("nan"), float("inf"), float("-inf")])
def test_has_no_row_anomalies_rejects_invalid_thresholds(threshold: float):
    """Thresholds must be finite percentiles in the half-open interval [0, 100)."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            threshold=threshold,
        )
    assert "finite number" in str(exc_info.value)


@pytest.mark.parametrize("threshold", ["99.5", Decimal("99.75")])
def test_has_no_row_anomalies_keeps_finite_threshold_coercion(threshold: object):
    """Existing finite values accepted by float coercion remain accepted."""
    check, apply_fn, _info = has_no_row_anomalies(
        model_name="catalog.schema.model",
        registry_table="catalog.schema.table",
        threshold=cast(float, threshold),
        enable_contributions=False,
    )
    assert check is not None and apply_fn is not None


def test_has_no_row_anomalies_rejects_non_positive_drift_threshold():
    """drift_threshold must be > 0 when provided."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=-0.1,
        )
    assert "drift_threshold must be greater than 0" in str(exc_info.value)

    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=0,
        )
    assert "drift_threshold must be greater than 0" in str(exc_info.value)


def test_has_no_row_anomalies_enable_contributions_requires_shap():
    """enable_contributions=True requires SHAP; raises when SHAP is not available."""
    with patch.object(check_funcs, "SHAP_AVAILABLE", False):
        with pytest.raises(InvalidParameterError) as exc_info:
            has_no_row_anomalies(
                model_name="catalog.schema.model",
                registry_table="catalog.schema.table",
                enable_contributions=True,
            )
    assert "enable_contributions=True requires the 'shap' dependency" in str(exc_info.value)


def test_default_flags_enable_contributions_and_ai_explanation():
    """Both SHAP contributions and AI explanations are on by default."""
    sig = inspect.signature(has_no_row_anomalies)
    assert sig.parameters["enable_contributions"].default is True
    assert sig.parameters["enable_ai_explanation"].default is True


def test_scoring_config_defaults_contributions_and_ai_explanation_on():
    """ScoringConfig mirrors the check's defaults: contributions + AI explanations on."""
    cfg = ScoringConfig(
        columns=["amount"],
        model_name="catalog.schema.m",
        registry_table="catalog.schema.reg",
        threshold=95.0,
        merge_columns=["__dqx_row_id_x"],
    )
    assert cfg.enable_contributions is True
    assert cfg.enable_ai_explanation is True


@pytest.mark.parametrize("enabled", [False, True])
def test_ai_explanation_dependency_is_reported_through_public_check(
    caplog: pytest.LogCaptureFixture, enabled: bool
) -> None:
    with caplog.at_level(logging.WARNING, logger=check_funcs.__name__):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.registry",
            enable_contributions=enabled,
            enable_ai_explanation=True,
        )
    warnings = [record.message for record in caplog.records if "disabling enable_ai_explanation" in record.message]
    assert bool(warnings) is not enabled


def test_has_no_row_anomalies_no_raise_when_contributions_off():
    """With contributions off (explanation left at its default-True), the check builds without
    raising — explanation is silently downgraded rather than erroring."""
    check, apply_fn, _info = has_no_row_anomalies(
        model_name="catalog.schema.model",
        registry_table="catalog.schema.table",
        enable_contributions=False,
    )
    assert check is not None and apply_fn is not None


def test_validate_explanation_flags_ai_query_needs_no_extra_dependency():
    """AI explanations run in Spark SQL via ``ai_query`` — no DSPy/driver dependency. The only
    hard requirement is SHAP (for the contributions that feed the prompt)."""
    with patch.object(check_funcs, "SHAP_AVAILABLE", True):
        has_no_row_anomalies(
            model_name="catalog.schema.model", registry_table="catalog.schema.registry", enable_contributions=True
        )


def test_has_no_row_anomalies_redact_columns_must_be_list():
    """redact_columns that is not a list raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            redact_columns="customer_id",  # type: ignore[arg-type]
        )
    assert "redact_columns must be a list" in str(exc_info.value)


@pytest.mark.parametrize("bad_value", [0, -1, -100])
def test_has_no_row_anomalies_max_groups_must_be_positive(bad_value):
    """max_groups <= 0 raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            max_groups=bad_value,
        )
    assert "max_groups must be a positive integer" in str(exc_info.value)


@pytest.mark.parametrize("bad_value", ["500", 500.0, None, [500]])
def test_has_no_row_anomalies_max_groups_must_be_int(bad_value):
    """max_groups non-int raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            max_groups=bad_value,  # type: ignore[arg-type]
        )
    assert "max_groups must be a positive integer" in str(exc_info.value)


def test_resolve_scoring_strategy_raises_for_unknown_algorithm():
    """resolve_scoring_strategy raises InvalidParameterError for an unrecognised algorithm name."""
    with pytest.raises(InvalidParameterError) as exc_info:
        resolve_scoring_strategy("UnknownAlgorithm")
    assert "Unsupported model algorithm 'UnknownAlgorithm'" in str(exc_info.value)


def test_has_no_row_anomalies_rejects_llm_model_config_unknown_keys():
    """llm_model_config dict with keys outside {model_name, api_key, api_base} is rejected."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            ai_explanation_llm_model_config={"model_name": "x", "api_base_url": "oops"},
        )
    assert "unknown keys" in str(exc_info.value)


def test_has_no_row_anomalies_rejects_llm_model_config_wrong_type():
    """llm_model_config that is neither LLMModelConfig nor dict is rejected."""
    with pytest.raises(InvalidParameterError) as exc_info:
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            ai_explanation_llm_model_config="databricks/foo",  # type: ignore[arg-type]
        )
    assert "must be an LLMModelConfig instance or a dict" in str(exc_info.value)


def test_resolve_scoring_strategy_returns_strategy_for_isolation_forest():
    """resolve_scoring_strategy succeeds for any IsolationForest algorithm variant."""
    strategy = resolve_scoring_strategy("IsolationForestV1")
    assert strategy is not None
    assert strategy.supports("IsolationForestV1")


def test_discovery_reports_missing_model_without_ordered_registry_reads() -> None:
    registry = create_autospec(AnomalyModelRegistry, instance=True)
    registry.get_active_model.return_value = None
    with pytest.raises(InvalidParameterError, match="Train first") as error:
        get_record_for_discovery(registry, "catalog.schema.registry", "catalog.schema.model")
    assert "catalog.schema.model" in str(error.value)
    assert "catalog.schema.registry" in str(error.value)
    registry.get_active_model.assert_called_once_with("catalog.schema.registry", "catalog.schema.model")


def test_discovery_returns_the_registered_record(anomaly_model_record: AnomalyModelRecord) -> None:
    registry = create_autospec(AnomalyModelRegistry, instance=True)
    registry.get_active_model.return_value = anomaly_model_record
    assert get_record_for_discovery(registry, "catalog.schema.registry", "catalog.schema.model") is anomaly_model_record


@pytest.mark.parametrize("quantiles", [None, {}])
def test_missing_quantile_points_require_retraining(
    anomaly_model_record: AnomalyModelRecord, quantiles: dict[str, float] | None
) -> None:
    anomaly_model_record.training.score_quantiles = quantiles
    with pytest.raises(InvalidParameterError, match="missing score quantiles") as error:
        get_quantile_points_for_severity(anomaly_model_record)
    assert anomaly_model_record.identity.model_name in str(error.value)
    assert "Retrain the model" in str(error.value)


def test_scoring_rejects_a_record_without_feature_metadata(anomaly_model_record: AnomalyModelRecord) -> None:
    df = create_autospec(DataFrame, instance=True)
    df.withColumn.return_value = df
    config = ScoringConfig(
        columns=["a", "b"],
        model_name=anomaly_model_record.identity.model_name,
        registry_table="catalog.schema.registry",
        threshold=95.0,
        merge_columns=["row_id"],
        enable_ai_explanation=False,
    )
    with pytest.raises(InvalidParameterError, match="missing feature_metadata") as error:
        GlobalModelScoringStrategy().score_global(df, anomaly_model_record, config)
    assert anomaly_model_record.identity.model_name in str(error.value)
