"""Unit tests for anomaly_llm_explainer (group-based algorithm).

Tests run without Spark or a live workspace. The LLM is mocked via a fake predictor
that returns deterministic values, allowing behavioural assertions without network calls.
Spark-dependent behaviour (groupBy aggregation, join-back) is covered in integration tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from databricks.labs.dqx.anomaly.anomaly_llm_explainer import (
    _call_llm_for_groups,
    _derive_confidence,
    _format_segment,
    _format_severity_range,
)


# ---------------------------------------------------------------------------
# Minimal ExplanationContext stub (avoids import-time Spark side effects).
# Only the fields _call_llm_for_groups reads need to exist on the stub.
# ---------------------------------------------------------------------------


@dataclass
class _StubConfig:
    threshold: float = 95.0
    model_name: str = "catalog.schema.model"


def _capturing_predictor(narrative: str = "ok", business_impact: str = "ok", action: str = "ok"):
    """Predictor that records every kwargs call in its .calls list."""

    class _P:
        def __init__(self):
            self.calls: list[dict] = []

        def __call__(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(narrative=narrative, business_impact=business_impact, action=action)

    return _P()


# ---------------------------------------------------------------------------
# _derive_confidence — three tiers per plan §7.3
# ---------------------------------------------------------------------------


def test_derive_confidence_single_model_is_na():
    assert _derive_confidence(0.0, is_ensemble=False) == "n/a"
    assert _derive_confidence(0.5, is_ensemble=False) == "n/a"
    assert _derive_confidence(None, is_ensemble=False) == "n/a"


def test_derive_confidence_ensemble_none_std_is_na():
    assert _derive_confidence(None, is_ensemble=True) == "n/a"


def test_derive_confidence_ensemble_high_below_0_05():
    assert _derive_confidence(0.0, is_ensemble=True) == "high"
    assert _derive_confidence(0.02, is_ensemble=True) == "high"
    assert _derive_confidence(0.049, is_ensemble=True) == "high"


def test_derive_confidence_ensemble_mixed_between_0_05_and_0_15():
    assert _derive_confidence(0.05, is_ensemble=True) == "mixed"
    assert _derive_confidence(0.10, is_ensemble=True) == "mixed"
    assert _derive_confidence(0.149, is_ensemble=True) == "mixed"


def test_derive_confidence_ensemble_low_at_or_above_0_15():
    assert _derive_confidence(0.15, is_ensemble=True) == "low"
    assert _derive_confidence(0.30, is_ensemble=True) == "low"


# ---------------------------------------------------------------------------
# _format_segment — "k=v, k=v" format per plan §15.2
# ---------------------------------------------------------------------------


def test_format_segment_empty_when_none():
    assert _format_segment(None) == ""


def test_format_segment_empty_when_empty_dict():
    assert _format_segment({}) == ""


def test_format_segment_uses_comma_space():
    assert _format_segment({"region": "US", "product": "electronics"}) == "region=US, product=electronics"


# ---------------------------------------------------------------------------
# _format_severity_range
# ---------------------------------------------------------------------------


def test_format_severity_range_one_decimal():
    assert _format_severity_range(97.4, 95.1, 99.8) == "mean 97.4, min 95.1, max 99.8"


# ---------------------------------------------------------------------------
# _call_llm_for_groups — one call per group, field forwarding, aggregation
#
# Group-cap enforcement and ranking now live in _aggregate_groups, which runs
# inside Spark (orderBy + limit). That path is covered by integration tests.
# ---------------------------------------------------------------------------


def _group(pattern: str, size: int, avg_sev: float, **extra) -> dict:
    base = {
        "__dqx_pattern": pattern,
        "group_size": size,
        "group_avg_severity": avg_sev,
        "severity_min": avg_sev,
        "severity_max": avg_sev,
        "mean_std": 0.0,
        "mean_contributions": {"amount": 100.0},
    }
    base.update(extra)
    return base


def test_call_llm_for_groups_one_call_per_group():
    groups = [
        _group("amount+quantity", size=300, avg_sev=97.0),
        _group("discount+region", size=50, avg_sev=96.0),
        _group("unknown", size=5, avg_sev=95.5),
    ]
    predictor = _capturing_predictor()
    _call_llm_for_groups(
        groups, _StubConfig(), "region=US", is_ensemble=False, drift_summary="none", predictor=predictor
    )
    assert len(predictor.calls) == 3


def test_call_llm_for_groups_forwards_all_signature_fields():
    groups = [_group("amount+quantity", size=312, avg_sev=97.4, severity_min=95.1, severity_max=99.8, mean_std=0.03)]
    predictor = _capturing_predictor()
    _call_llm_for_groups(
        groups,
        _StubConfig(threshold=95.0, model_name="catalog.schema.m"),
        segment_str="region=US",
        is_ensemble=True,
        drift_summary="amount KS=0.42",
        predictor=predictor,
    )
    call = predictor.calls[0]
    for field in (
        "feature_contributions",
        "group_size",
        "severity_range",
        "confidence",
        "segment",
        "threshold",
        "model_name",
        "drift_summary",
    ):
        assert field in call, f"missing {field}"
    assert call["group_size"] == "312 rows"
    assert call["severity_range"] == "mean 97.4, min 95.1, max 99.8"
    assert call["confidence"] == "high"
    assert call["segment"] == "region=US"
    assert call["threshold"] == "95.0"
    assert call["model_name"] == "catalog.schema.m"
    assert call["drift_summary"] == "amount KS=0.42"


def test_call_llm_for_groups_drift_none_when_empty():
    groups = [_group("a+b", 10, 97.0)]
    predictor = _capturing_predictor()
    _call_llm_for_groups(groups, _StubConfig(), "", is_ensemble=False, drift_summary="", predictor=predictor)
    assert predictor.calls[0]["drift_summary"] == "none"


def test_call_llm_for_groups_returns_pattern_group_size_and_avg_severity():
    groups = [_group("amount+quantity", size=312, avg_sev=97.4)]
    predictor = _capturing_predictor(narrative="n", business_impact="i", action="a")
    rows = _call_llm_for_groups(groups, _StubConfig(), "", is_ensemble=False, drift_summary="none", predictor=predictor)
    assert len(rows) == 1
    pattern, narrative, impact, action, size, avg_sev = rows[0]
    assert pattern == "amount+quantity"
    assert narrative == "n"
    assert impact == "i"
    assert action == "a"
    assert size == 312
    assert avg_sev == pytest.approx(97.4)


def test_call_llm_for_groups_applies_redaction_via_mean_contributions():
    """Redaction happens upstream (in _aggregate_groups); verify the predictor never sees redacted keys
    when they are already absent from mean_contributions."""
    groups = [_group("amount+region", size=100, avg_sev=98.0, mean_contributions={"amount": 80.0, "region": 20.0})]
    predictor = _capturing_predictor()
    _call_llm_for_groups(groups, _StubConfig(), "", is_ensemble=False, drift_summary="none", predictor=predictor)
    contrib_str = predictor.calls[0]["feature_contributions"]
    assert "customer_id" not in contrib_str
    assert "amount" in contrib_str


# ---------------------------------------------------------------------------
# AnomalyGroupExplanationSignature — import smoke test
# ---------------------------------------------------------------------------


def test_signature_class_exists_and_is_importable():
    from databricks.labs.dqx.anomaly.anomaly_llm_explainer import AnomalyGroupExplanationSignature

    assert AnomalyGroupExplanationSignature is not None


# ---------------------------------------------------------------------------
# _build_lm_config — LLMModelConfig → dict shape expected by dspy.LM
# ---------------------------------------------------------------------------


def test_build_lm_config_includes_required_keys():
    from databricks.labs.dqx.anomaly.anomaly_llm_explainer import _build_lm_config

    cfg = MagicMock(model_name="databricks/x", api_key="k", api_base="b")
    out = _build_lm_config(cfg)
    assert out["model"] == "databricks/x"
    assert out["model_type"] == "chat"
    assert out["api_key"] == "k"
    assert out["api_base"] == "b"
    assert "max_retries" in out


def test_build_lm_config_forces_openai_prefix_when_api_base_set_and_no_provider():
    """Bare model name + api_base → force openai/ prefix so litellm uses OpenAI-compatible routing.

    Without this, litellm defaults to its Databricks provider for 'databricks-*' names and
    ignores api_base → ENDPOINT_NOT_FOUND against AI-Gateway endpoints.
    """
    from databricks.labs.dqx.anomaly.anomaly_llm_explainer import _build_lm_config

    cfg = MagicMock(model_name="databricks-qwen3-80b", api_key="tok", api_base="https://gw.example/v1")
    out = _build_lm_config(cfg)
    assert out["model"] == "openai/databricks-qwen3-80b"
    assert out["api_base"] == "https://gw.example/v1"


def test_build_lm_config_preserves_explicit_provider_prefix():
    from databricks.labs.dqx.anomaly.anomaly_llm_explainer import _build_lm_config

    cfg = MagicMock(model_name="databricks/claude-sonnet", api_key="", api_base="https://gw.example/v1")
    out = _build_lm_config(cfg)
    assert out["model"] == "databricks/claude-sonnet"


def test_build_lm_config_no_prefix_when_no_api_base():
    """Workspace-auth path: no api_base → no openai/ prefix, litellm auto-detects."""
    from databricks.labs.dqx.anomaly.anomaly_llm_explainer import _build_lm_config

    cfg = MagicMock(model_name="databricks/claude-sonnet", api_key="", api_base="")
    out = _build_lm_config(cfg)
    assert out["model"] == "databricks/claude-sonnet"
    assert "api_base" not in out
    assert "api_key" not in out


def test_build_lm_config_falls_back_to_env_vars(monkeypatch):
    from databricks.labs.dqx.anomaly.anomaly_llm_explainer import _build_lm_config

    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    monkeypatch.setenv("OPENAI_API_BASE", "https://env.example/v1")
    cfg = MagicMock(model_name="some-model", api_key="", api_base="")
    out = _build_lm_config(cfg)
    assert out["api_key"] == "env-key"
    assert out["api_base"] == "https://env.example/v1"
    assert out["model"] == "openai/some-model"


# ---------------------------------------------------------------------------
# _coerce_llm_model_config — dict coercion for YAML / metadata path
# ---------------------------------------------------------------------------


def test_coerce_llm_model_config_accepts_instance():
    from databricks.labs.dqx.anomaly.check_funcs import _coerce_llm_model_config
    from databricks.labs.dqx.config import LLMModelConfig

    cfg = LLMModelConfig(model_name="x")
    assert _coerce_llm_model_config(cfg) is cfg


def test_coerce_llm_model_config_accepts_none():
    from databricks.labs.dqx.anomaly.check_funcs import _coerce_llm_model_config

    assert _coerce_llm_model_config(None) is None


def test_coerce_llm_model_config_converts_dict():
    from databricks.labs.dqx.anomaly.check_funcs import _coerce_llm_model_config
    from databricks.labs.dqx.config import LLMModelConfig

    out = _coerce_llm_model_config({"model_name": "x", "api_key": "k", "api_base": "b"})
    assert isinstance(out, LLMModelConfig)
    assert out.model_name == "x"
    assert out.api_key == "k"
    assert out.api_base == "b"


def test_coerce_llm_model_config_rejects_unknown_keys():
    from databricks.labs.dqx.anomaly.check_funcs import _coerce_llm_model_config
    from databricks.labs.dqx.errors import InvalidParameterError

    with pytest.raises(InvalidParameterError, match="unknown keys"):
        _coerce_llm_model_config({"model_name": "x", "api_base_url": "oops"})


def test_coerce_llm_model_config_rejects_other_types():
    from databricks.labs.dqx.anomaly.check_funcs import _coerce_llm_model_config
    from databricks.labs.dqx.errors import InvalidParameterError

    with pytest.raises(InvalidParameterError, match="must be an LLMModelConfig instance or a dict"):
        _coerce_llm_model_config("databricks/foo")  # type: ignore[arg-type]
