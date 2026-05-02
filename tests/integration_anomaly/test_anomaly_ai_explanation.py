"""Integration tests for LLM-based AI explanation of row anomalies.

Uses driver_only=True so the LLM call happens in the driver process, letting us
monkeypatch dspy without crossing a UDF worker boundary. The LLM itself is
stubbed out — we exercise the real Spark/SHAP/_dq_info plumbing end-to-end.
"""

from __future__ import annotations

import dataclasses
import os
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, MapType, StringType, StructField, StructType

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import DSPY_AVAILABLE, ExplanationContext
from databricks.labs.dqx.config import AnomalyParams, LLMModelConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.llm.llm_core import LLMModelConfigurator
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.conftest import create_anomaly_apply_fn, qualify_model_name
from tests.integration_anomaly.constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)

dspy = pytest.importorskip("dspy")


_CANNED = SimpleNamespace(
    narrative="Row flagged because amount is far above baseline.",
    business_impact="May inflate revenue reporting.",
    action="Verify the transaction source.",
)


class _FakePredictor:
    """Stands in for dspy.Predict(signature) — returns canned output."""

    def __init__(self, *_args, **_kwargs):
        pass

    def __call__(self, **_kwargs):
        return _CANNED


class _FakeLM:
    def __init__(self, *_args, **_kwargs):
        pass


@pytest.fixture
def mock_llm(monkeypatch):
    """Patch dspy.LM and dspy.Predict at the explainer module level."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _FakePredictor)
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _FakePredictor)
    return _CANNED


def _llm_cfg() -> LLMModelConfig:
    # executor='driver' is mandatory here: *mock_llm* monkeypatches dspy.LM / dspy.Predict,
    # which are only consulted on the driver path. With the default 'ai_query' executor the
    # tests would silently route through Spark SQL *ai_query* against a non-existent
    # endpoint named 'stub' and 404. Tests that intentionally exercise the ai_query path
    # construct their own config (see *test_ai_query_explanation_*).
    return LLMModelConfig(
        model_name="databricks/stub",
        api_key="stub",
        api_base="https://stub.example.test",
        api_base_allowed_hosts=("stub.example.test",),
        executor="driver",
    )


def _score_with_explanation(scorer, df, model_meta, **overrides):
    kwargs = {
        "model_name": model_meta["model_name"],
        "registry_table": model_meta["registry_table"],
        "threshold": DEFAULT_SCORE_THRESHOLD,
        "enable_contributions": True,
        "enable_ai_explanation": True,
        "llm_model_config": _llm_cfg(),
        "extract_score": False,
        **overrides,
    }
    return scorer(df, **kwargs)


def _make_outlier_df(spark, factory, *, repeat: int = 1):
    return factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)] * repeat,
        columns_schema="amount double, quantity double, discount double",
    )


def test_ai_explanation_populated_for_anomalous_row(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, mock_llm
):
    """Anomalous rows get the ai_explanation struct populated from the (mocked) LLM."""
    test_df = _make_outlier_df(spark, test_df_factory)
    result_df = _score_with_explanation(anomaly_scorer, test_df, shared_3d_model)
    row = result_df.collect()[0]
    anomaly_info = row["_dq_info"][0]["anomaly"]

    assert anomaly_info["is_anomaly"] is True
    explanation = anomaly_info["ai_explanation"]
    assert explanation is not None
    assert explanation["narrative"] == mock_llm.narrative
    assert explanation["business_impact"] == mock_llm.business_impact
    assert explanation["action"] == mock_llm.action
    # pattern is computed deterministically from real SHAP contributions (top-2, alpha-sorted).
    assert explanation["pattern"]
    assert "+" in explanation["pattern"] or explanation["pattern"] in {"amount", "quantity", "discount"}
    for feat in explanation["pattern"].split("+"):
        assert feat in {"amount", "quantity", "discount"}
    # Group metadata — a single anomalous row → group_size 1, group_avg_severity == row severity.
    assert explanation["group_size"] == 1
    assert explanation["group_avg_severity"] == pytest.approx(anomaly_info["severity_percentile"], rel=1e-6)


def test_ai_explanation_null_for_non_anomalous_row(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer
):
    """Rows below the severity threshold keep ai_explanation null."""
    # Row drawn from the training distribution (see get_standard_3d_training_data: i=100).
    test_df = test_df_factory(
        spark,
        normal_rows=[(150.0, 20.0, 0.2)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double, discount double",
    )
    result_df = _score_with_explanation(anomaly_scorer, test_df, shared_3d_model)
    row = result_df.collect()[0]
    anomaly_info = row["_dq_info"][0]["anomaly"]

    assert anomaly_info["is_anomaly"] is False
    assert anomaly_info["ai_explanation"] is None


def test_ai_explanation_redact_columns_filters_pattern(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, monkeypatch
):
    """redact_columns removes features from the LLM prompt and the pattern key."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")

    captured: dict = {}

    class _CapturingPredictor:
        def __init__(self, *_a, **_kw):
            pass

        def __call__(self, **kwargs):
            captured.update(kwargs)
            return _CANNED

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _CapturingPredictor)
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _CapturingPredictor)

    test_df = _make_outlier_df(spark, test_df_factory)
    result_df = _score_with_explanation(anomaly_scorer, test_df, shared_3d_model, redact_columns=["amount"])
    row = result_df.collect()[0]
    explanation = row["_dq_info"][0]["anomaly"]["ai_explanation"]

    assert explanation is not None
    assert "amount" not in explanation["pattern"]
    assert "amount" not in captured.get("feature_contributions", "")


def test_ai_explanation_one_llm_call_per_group(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, monkeypatch
):
    """Multiple anomalous rows collapsing into a single (segment, pattern) group trigger exactly one LLM call."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")

    call_count = 0

    class _CountingPredictor:
        def __init__(self, *_a, **_kw):
            pass

        def __call__(self, **_kwargs):
            nonlocal call_count
            call_count += 1
            return _CANNED

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _CountingPredictor)
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _CountingPredictor)

    # Several identical outliers → same contributions → same pattern → one group.
    test_df = _make_outlier_df(spark, test_df_factory, repeat=5)
    result_df = _score_with_explanation(anomaly_scorer, test_df, shared_3d_model)
    rows = result_df.collect()
    explanations = [
        r["_dq_info"][0]["anomaly"]["ai_explanation"] for r in rows if r["_dq_info"][0]["anomaly"]["is_anomaly"]
    ]

    # One group → one LLM call, and all flagged rows share the same narrative + group_size.
    assert call_count == 1
    assert len({e["narrative"] for e in explanations}) == 1
    assert len({e["pattern"] for e in explanations}) == 1
    assert all(e["group_size"] == len(explanations) for e in explanations)


# ---------------------------------------------------------------------------
# Direct add_explanation_column tests against a synthetic scored DataFrame.
# These exercise the Spark-side ranking + cap path without needing a model.
# ---------------------------------------------------------------------------


def _build_synthetic_scored_df(spark: SparkSession, rows: list[tuple[float, dict[str, float]]]):
    """Build a minimal scored DataFrame the explainer accepts: severity, contributions, score_std=0.0."""
    schema = StructType(
        [
            StructField("severity_percentile", DoubleType(), False),
            StructField("anomaly_score_std", DoubleType(), True),
            StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True),
        ]
    )
    data = [(sev, 0.0, contrib) for sev, contrib in rows]
    return spark.createDataFrame(data, schema=schema)


def _build_scored_df_with_std(
    spark: SparkSession,
    rows: list[tuple[float, float | None, dict[str, float]]],
):
    """Synthetic scored DF where the per-row score_std can be set explicitly (or None)."""
    schema = StructType(
        [
            StructField("severity_percentile", DoubleType(), False),
            StructField("anomaly_score_std", DoubleType(), True),
            StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def _capturing_predictor() -> tuple[type, list[dict]]:
    """Build a (PredictorClass, captured_calls_list) pair for asserting predictor kwargs."""
    captured: list[dict] = []

    class _Predictor:
        def __init__(self, *_a, **_kw) -> None:
            pass

        def __call__(self, **kwargs) -> SimpleNamespace:
            captured.append(kwargs)
            return _CANNED

    return _Predictor, captured


def _capturing_lm() -> tuple[type, list[dict]]:
    """Build a (LMClass, captured_init_kwargs_list) pair for asserting dspy.LM construction."""
    captured: list[dict] = []

    class _LM:
        def __init__(self, *_a, **kwargs) -> None:
            captured.append(kwargs)

    return _LM, captured


def _patch_dspy(monkeypatch, lm_cls, predictor_cls):
    monkeypatch.setattr(dspy, "LM", lm_cls)
    monkeypatch.setattr(dspy, "Predict", predictor_cls)
    monkeypatch.setattr(llm_explainer.dspy, "LM", lm_cls)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", predictor_cls)


def _ctx(max_groups: int = 500, redact_columns: tuple[str, ...] = ()) -> ExplanationContext:
    # executor='driver' for the same reason as in *_llm_cfg*: every caller of this helper
    # uses *_patch_dspy* (driver-only seam) to capture LM/predictor kwargs. Without the
    # explicit override the default 'ai_query' would send each test through Spark SQL
    # against the non-existent 'stub' endpoint.
    return ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.synthetic",
        llm_model_config=LLMModelConfig(
            model_name="databricks/stub",
            api_key="stub",
            api_base="https://stub.example.test",
            api_base_allowed_hosts=("stub.example.test",),
            executor="driver",
        ),
        max_groups=max_groups,
        redact_columns=redact_columns,
    )


def test_ai_explanation_warning_logged_when_max_groups_exceeded(spark: SparkSession, mock_llm, caplog):
    """Two distinct patterns + max_groups=1 → highest-ranked group keeps its narrative;
    the dropped group's rows get a null struct, and a warning is emitted with concrete counts."""
    rows = [
        # Pattern "amount+quantity", size 3, avg sev 99.0 → rank score 297
        (99.0, {"amount": 80.0, "quantity": 15.0, "discount": 5.0}),
        (99.0, {"amount": 80.0, "quantity": 15.0, "discount": 5.0}),
        (99.0, {"amount": 80.0, "quantity": 15.0, "discount": 5.0}),
        # Pattern "amount+discount", size 2, avg sev 96.0 → rank score 192 (dropped)
        (96.0, {"amount": 70.0, "discount": 25.0, "quantity": 5.0}),
        (96.0, {"amount": 70.0, "discount": 25.0, "quantity": 5.0}),
    ]
    df = _build_synthetic_scored_df(spark, rows)

    with caplog.at_level("WARNING", logger=llm_explainer.__name__):
        result = llm_explainer.add_explanation_column(
            df, _ctx(max_groups=1), segment_values=None, is_ensemble=False, drift_summary="none"
        )

    explanations = [r["ai_explanation"] for r in result.collect()]
    populated = [e for e in explanations if e is not None]
    null_count = sum(1 for e in explanations if e is None)
    assert len(populated) == 3, "kept group of size 3 should have populated struct"
    assert null_count == 2, "dropped group of size 2 should have null struct"
    assert {e["pattern"] for e in populated} == {"amount+quantity"}

    warnings = [r for r in caplog.records if r.levelname == "WARNING" and "exceeded max_groups" in r.getMessage()]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "{}" not in msg, "lazy formatting must interpolate values"
    assert "1 groups covering 2 rows" in msg
    assert "max_groups=1" in msg


def test_ai_explanation_handles_empty_input_dataframe(spark: SparkSession):
    """Empty input → empty output with the explanation struct column attached, no LLM call, no warning."""
    df = _build_synthetic_scored_df(spark, rows=[])

    result = llm_explainer.add_explanation_column(
        df, _ctx(max_groups=2), segment_values=None, is_ensemble=False, drift_summary="none"
    )

    assert "ai_explanation" in result.columns
    assert result.count() == 0


# ---------------------------------------------------------------------------
# Predictor-side behaviours: confidence tiers, segment formatting, severity
# range formatting, drift defaulting, and full signature-field forwarding.
# Exercised via add_explanation_column with a CapturingPredictor.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "score_std,is_ensemble,expected_confidence",
    [
        (0.0, False, "n/a"),  # single-model → always n/a
        (0.5, False, "n/a"),  # single-model ignores std magnitude
        (0.02, True, "high"),  # ensemble + std < 0.05
        (0.10, True, "mixed"),  # ensemble + 0.05 <= std < 0.15
        (0.20, True, "low"),  # ensemble + std >= 0.15
    ],
)
def test_confidence_label_reflects_score_std_and_ensemble_flag(
    spark: SparkSession, monkeypatch, score_std, is_ensemble, expected_confidence
):
    """confidence kwarg sent to the LLM follows the (mean_std, is_ensemble) → tier mapping."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    predictor_cls, captured = _capturing_predictor()
    _patch_dspy(monkeypatch, _FakeLM, predictor_cls)

    df = _build_scored_df_with_std(
        spark,
        [(99.0, score_std, {"amount": 80.0, "quantity": 20.0})],
    )
    llm_explainer.add_explanation_column(
        df, _ctx(), segment_values=None, is_ensemble=is_ensemble, drift_summary="none"
    ).collect()

    assert len(captured) == 1
    assert captured[0]["confidence"] == expected_confidence


def test_confidence_label_is_n_a_when_score_std_is_null(spark: SparkSession, monkeypatch):
    """Even with is_ensemble=True, a null score_std yields confidence='n/a'."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    predictor_cls, captured = _capturing_predictor()
    _patch_dspy(monkeypatch, _FakeLM, predictor_cls)

    df = _build_scored_df_with_std(
        spark,
        [(99.0, None, {"amount": 80.0, "quantity": 20.0})],
    )
    llm_explainer.add_explanation_column(
        df, _ctx(), segment_values=None, is_ensemble=True, drift_summary="none"
    ).collect()

    assert captured[0]["confidence"] == "n/a"


@pytest.mark.parametrize(
    "segment_values,expected_segment",
    [
        (None, ""),
        ({}, ""),
        ({"region": "US"}, "region=US"),
        ({"region": "US", "product": "electronics"}, "region=US, product=electronics"),
    ],
)
def test_segment_kwarg_formatting(spark: SparkSession, monkeypatch, segment_values, expected_segment):
    """segment kwarg is 'k=v, k=v' (empty when no segmentation)."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    predictor_cls, captured = _capturing_predictor()
    _patch_dspy(monkeypatch, _FakeLM, predictor_cls)

    df = _build_synthetic_scored_df(spark, [(99.0, {"amount": 80.0, "quantity": 20.0})])
    llm_explainer.add_explanation_column(
        df, _ctx(), segment_values=segment_values, is_ensemble=False, drift_summary="none"
    ).collect()

    assert captured[0]["segment"] == expected_segment


def test_drift_summary_defaults_to_none_when_empty(spark: SparkSession, monkeypatch):
    """Empty drift_summary is normalised to literal 'none' for the LLM."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    predictor_cls, captured = _capturing_predictor()
    _patch_dspy(monkeypatch, _FakeLM, predictor_cls)

    df = _build_synthetic_scored_df(spark, [(99.0, {"amount": 80.0, "quantity": 20.0})])
    llm_explainer.add_explanation_column(df, _ctx(), segment_values=None, is_ensemble=False, drift_summary="").collect()

    assert captured[0]["drift_summary"] == "none"


def test_severity_range_kwarg_uses_one_decimal_mean_min_max(spark: SparkSession, monkeypatch):
    """severity_range is formatted as 'mean X.X, min Y.Y, max Z.Z' with one decimal."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    predictor_cls, captured = _capturing_predictor()
    _patch_dspy(monkeypatch, _FakeLM, predictor_cls)

    rows = [
        (95.1, {"amount": 80.0, "quantity": 20.0}),
        (97.4, {"amount": 80.0, "quantity": 20.0}),
        (99.8, {"amount": 80.0, "quantity": 20.0}),
    ]
    df = _build_synthetic_scored_df(spark, rows)
    llm_explainer.add_explanation_column(
        df, _ctx(), segment_values=None, is_ensemble=False, drift_summary="none"
    ).collect()

    assert captured[0]["severity_range"] == "mean 97.4, min 95.1, max 99.8"


def test_predictor_receives_all_signature_fields(spark: SparkSession, monkeypatch):
    """Every input field declared on AnomalyGroupExplanationSignature is forwarded."""
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    predictor_cls, captured = _capturing_predictor()
    _patch_dspy(monkeypatch, _FakeLM, predictor_cls)

    df = _build_synthetic_scored_df(spark, [(99.0, {"amount": 80.0, "quantity": 20.0})])
    llm_explainer.add_explanation_column(
        df, _ctx(), segment_values={"region": "US"}, is_ensemble=False, drift_summary="amount KS=0.42"
    ).collect()

    expected_fields = {
        "feature_contributions",
        "group_size",
        "severity_range",
        "confidence",
        "segment",
        "threshold",
        "model_name",
        "drift_summary",
    }
    assert expected_fields.issubset(captured[0].keys())
    assert captured[0]["group_size"] == "1 rows"
    assert captured[0]["threshold"] == "95.0"
    assert captured[0]["model_name"] == "catalog.schema.synthetic"
    assert captured[0]["drift_summary"] == "amount KS=0.42"


# ---------------------------------------------------------------------------
# dspy.LM construction — routing rules from LLMModelConfig.
# Exercised by patching dspy.LM with a CapturingLM and reading its kwargs.
# ---------------------------------------------------------------------------


def _run_with_lm_capture(spark: SparkSession, monkeypatch, llm_model_config: LLMModelConfig):
    """Drive *add_explanation_column* through the driver executor with a CapturingLM.

    These tests assert on dspy.LM kwargs, so the executor must be 'driver' regardless of the
    *LLMModelConfig.executor* default. Force-coerce here so callers don't have to repeat the
    boilerplate (and so the suite doesn't silently switch paths when defaults change).
    """
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    lm_cls, lm_kwargs = _capturing_lm()
    _patch_dspy(monkeypatch, lm_cls, _FakePredictor)

    if llm_model_config.executor != "driver":
        llm_model_config = dataclasses.replace(llm_model_config, executor="driver")

    df = _build_synthetic_scored_df(spark, [(99.0, {"amount": 80.0, "quantity": 20.0})])
    ctx = ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.m",
        llm_model_config=llm_model_config,
    )
    llm_explainer.add_explanation_column(
        df, ctx, segment_values=None, is_ensemble=False, drift_summary="none"
    ).collect()
    return lm_kwargs


def test_lm_config_passes_provider_prefixed_model_through(spark: SparkSession, monkeypatch):
    """A provider-prefixed model + allowlisted api_base is forwarded to dspy.LM unchanged."""
    cfg = LLMModelConfig(
        model_name="databricks/claude-sonnet",
        api_key="tok",
        api_base="https://gw.example.test/v1",
        api_base_allowed_hosts=("gw.example.test",),
    )
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["model"] == "databricks/claude-sonnet"
    assert captured[0]["api_base"] == "https://gw.example.test/v1"
    assert captured[0]["api_key"] == "tok"
    assert captured[0]["model_type"] == "chat"
    assert captured[0]["max_retries"] == 3


def test_lm_config_forwards_max_retries_override(spark: SparkSession, monkeypatch):
    """*max_retries* on LLMModelConfig overrides the default and is forwarded to dspy.LM.

    Pinned here (not just in unit config tests) because the contract that matters for users is
    "the value I set on LLMModelConfig actually reaches the LM constructor" — not whether the
    field exists. *max_retries=0* is the most useful override (fail fast for tests / fail-soft
    workflows), so we lock that case.
    """
    cfg = LLMModelConfig(model_name="databricks/claude-sonnet", max_retries=0)
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)
    assert captured[0]["max_retries"] == 0


def test_lm_config_workspace_auth_with_empty_credentials(spark: SparkSession, monkeypatch):
    """Workspace-auth path: empty api_key/api_base are forwarded as empty strings."""
    cfg = LLMModelConfig(model_name="databricks/claude-sonnet", api_key="", api_base="")
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["model"] == "databricks/claude-sonnet"
    assert captured[0]["api_base"] == ""
    assert captured[0]["api_key"] == ""


def test_lm_config_forwards_budget_caps(spark: SparkSession, monkeypatch):
    """max_tokens / temperature / timeout from LLMModelConfig are forwarded to dspy.LM."""
    cfg = LLMModelConfig(
        model_name="databricks/claude-sonnet",
        max_tokens=250,
        temperature=0.4,
        timeout=12.5,
    )
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["max_tokens"] == 250
    assert captured[0]["temperature"] == 0.4
    assert captured[0]["timeout"] == 12.5


def test_lm_config_default_budget_caps_applied(spark: SparkSession, monkeypatch):
    """Default LLMModelConfig forwards the documented default caps to dspy.LM."""
    cfg = LLMModelConfig()
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["max_tokens"] == 1000
    assert captured[0]["temperature"] == 0.0
    assert captured[0]["timeout"] == 30.0


def test_max_tokens_cap_enforced_by_live_llm(ws):
    """Real LLM call must not return more completion tokens than max_tokens.

    The *ws* fixture ensures the test is skipped when no Databricks workspace is
    configured, and triggers the workspace-auth setup that the foundation-model
    endpoint needs. Uses a deliberately verbose prompt to push the model past
    the cap, then inspects litellm's usage.completion_tokens on the raw
    response. A small margin (+8) accounts for tokenizer rounding across
    providers.
    """
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    assert ws.current_user.me() is not None  # fail-fast if workspace auth is broken

    cfg = LLMModelConfig(model_name="databricks/databricks-llama-4-maverick", max_tokens=20)
    language_model = LLMModelConfigurator(cfg).create_lm()
    long_prompt = (
        "Write an extremely detailed 1000-word essay about the history of databases, "
        "covering relational, NoSQL, and modern cloud lakehouse architectures."
    )
    completion = language_model(long_prompt)
    assert completion, "LLM returned no text — endpoint may be misconfigured"

    last = language_model.history[-1]
    usage = last.get("usage") or last.get("response", {}).get("usage")
    assert usage is not None, f"No usage block on response: {last!r}"
    completion_tokens = (
        usage.get("completion_tokens") if isinstance(usage, dict) else getattr(usage, "completion_tokens", None)
    )
    assert completion_tokens is not None, f"completion_tokens missing from usage: {usage!r}"
    assert (
        completion_tokens <= cfg.max_tokens + 8
    ), f"LLM exceeded max_tokens={cfg.max_tokens}: completion_tokens={completion_tokens}"


# ---------------------------------------------------------------------------
# ai_query executor path — these run a real Spark SQL ai_query call against a
# Databricks Model Serving endpoint and so are skipped on workspaces without
# Foundation Model APIs (or when the endpoint is unreachable). Override the
# endpoint with the DQX_AI_QUERY_TEST_ENDPOINT env var if the default is not
# available in your workspace.
# ---------------------------------------------------------------------------


_AI_QUERY_TEST_ENDPOINT = os.environ.get("DQX_AI_QUERY_TEST_ENDPOINT", "databricks-llama-4-maverick")


def _ai_query_endpoint_available(spark: SparkSession) -> tuple[bool, str | None]:
    """Cheap probe — does ai_query against the configured endpoint succeed?

    Returns ``(available, error_message)``. The error message is surfaced in the skip reason so
    a failing probe doesn't masquerade as 'endpoint not provisioned' — knowing why the probe
    failed (auth, missing entitlement, wrong name) is what lets the user decide whether to set
    DQX_AI_QUERY_TEST_ENDPOINT.
    """
    try:
        spark.sql(
            f"SELECT ai_query('{_AI_QUERY_TEST_ENDPOINT}', 'reply with the single word: ok', "
            f"modelParameters => named_struct('max_tokens', 8, 'temperature', 0.0)) AS r"
        ).collect()
        return True, None
    except Exception as exc:  # pylint: disable=broad-except
        return False, repr(exc)


@pytest.fixture
def ai_query_endpoint(ws, spark):
    """Skip the test when the workspace cannot reach the configured ai_query endpoint."""
    assert ws.current_user.me() is not None  # fail-fast if workspace auth is broken
    available, error = _ai_query_endpoint_available(spark)
    if not available:
        pytest.skip(
            f"ai_query endpoint {_AI_QUERY_TEST_ENDPOINT!r} not reachable; "
            f"set DQX_AI_QUERY_TEST_ENDPOINT to override. Probe error: {error}"
        )
    return _AI_QUERY_TEST_ENDPOINT


def _ai_query_llm_cfg(endpoint: str) -> LLMModelConfig:
    return LLMModelConfig(model_name=endpoint, executor="ai_query")


def test_ai_query_explanation_populated_for_anomalous_row(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """ai_query path produces a non-null ai_explanation struct on anomalous rows.

    Asserts only on structural properties — non-empty strings, length cap, struct fields —
    so the test does not depend on the exact wording the model returns.
    """
    test_df = _make_outlier_df(spark, test_df_factory)
    result_df = _score_with_explanation(
        anomaly_scorer, test_df, shared_3d_model, llm_model_config=_ai_query_llm_cfg(ai_query_endpoint)
    )
    row = result_df.collect()[0]
    anomaly_info = row["_dq_info"][0]["anomaly"]

    assert anomaly_info["is_anomaly"] is True
    explanation = anomaly_info["ai_explanation"]
    assert explanation is not None, "ai_query returned a null struct on an anomalous row"
    for field_name in ("narrative", "business_impact", "action"):
        value = explanation[field_name]
        assert isinstance(value, str) and value.strip(), f"{field_name!r} is empty"
        assert len(value) <= llm_explainer._LLM_FIELD_MAX_LEN  # pylint: disable=protected-access
    assert explanation["pattern"]
    assert explanation["group_size"] == 1


def test_ai_query_explanation_redact_columns_filters_prompt(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """redact_columns prevents the redacted feature name from appearing in any returned field.

    Combined with the unit test on the prompt builder, this verifies redaction holds end-to-end:
    not only is the name kept out of the prompt, but the model isn't somehow echoing it via
    another channel (e.g. the pattern key or schema metadata).
    """
    test_df = _make_outlier_df(spark, test_df_factory)
    result_df = _score_with_explanation(
        anomaly_scorer,
        test_df,
        shared_3d_model,
        llm_model_config=_ai_query_llm_cfg(ai_query_endpoint),
        redact_columns=["amount"],
    )
    row = result_df.collect()[0]
    explanation = row["_dq_info"][0]["anomaly"]["ai_explanation"]

    assert explanation is not None
    assert "amount" not in explanation["pattern"]
    for field_name in ("narrative", "business_impact", "action"):
        assert (
            "amount" not in explanation[field_name].lower()
        ), f"redacted column 'amount' leaked into {field_name}: {explanation[field_name]!r}"


def test_ai_query_explanation_one_call_per_group(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """Multiple identical anomalous rows collapse into a single (segment, pattern) group.

    The driver-path version of this test uses a counting predictor; the ai_query path runs on
    executors so we can't intercept the call directly. Instead we assert the *observable*
    contract: every flagged row in the group shares the same narrative and group_size, which
    can only happen if the LLM was invoked once and the result fanned out via the join.
    """
    test_df = _make_outlier_df(spark, test_df_factory, repeat=5)
    result_df = _score_with_explanation(
        anomaly_scorer, test_df, shared_3d_model, llm_model_config=_ai_query_llm_cfg(ai_query_endpoint)
    )
    rows = result_df.collect()
    explanations = [
        r["_dq_info"][0]["anomaly"]["ai_explanation"] for r in rows if r["_dq_info"][0]["anomaly"]["is_anomaly"]
    ]

    assert len(explanations) == 5
    # Single LLM call → single narrative + pattern shared across the group.
    assert len({e["narrative"] for e in explanations}) == 1
    assert len({e["pattern"] for e in explanations}) == 1
    assert all(e["group_size"] == len(explanations) for e in explanations)


def test_ai_query_executor_rejects_non_databricks_provider():
    """``executor='ai_query'`` with a non-Databricks provider prefix surfaces InvalidParameterError.

    Pure validation — no live call, no skip needed. Catches the case where a user copies a
    DSPy/litellm-style ``provider/model`` config and forgets to switch executor.
    """
    cfg = LLMModelConfig(model_name="openai/gpt-4", executor="ai_query")
    ctx = ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.m",
        llm_model_config=cfg,
    )
    with pytest.raises(InvalidParameterError, match="executor='ai_query' requires a Databricks"):
        llm_explainer._resolve_ai_query_endpoint(ctx.llm_model_config.model_name)  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# Global max_groups cap across segments — wiring test for *score_segmented*.
# Trains a real 2-segment model (cheap settings) and counts predictor calls
# end-to-end to verify the per-segment budget split actually bounds the total.
# ---------------------------------------------------------------------------


def test_max_groups_is_global_cap_across_segments(
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
    monkeypatch,
):
    """*max_groups* bounds the total LLM call count across ALL segments, not per-segment.

    Regression test for the per-segment-cap bug: previously *max_groups* applied
    independently to each segment, so with N segments the worst case was N * max_groups
    LLM calls. The fix splits the budget equally across eligible segments before
    threading it into *add_explanation_column*. This test trains a real 2-segment model,
    feeds enough distinct anomaly *patterns* per segment to exceed any per-segment cap,
    and asserts total predictor invocations <= max_groups.

    Driver path only — *_patch_dspy* installs a counting predictor that's only consulted
    when *executor='driver'*. The ai_query path's bound is enforced by the same
    *_split_max_groups_budget* helper at the SQL level (each segment runs an independent
    *ai_query* call per group, capped by the per-segment budget).
    """
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")

    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"{TEST_CATALOG}.{schema.name}.test_seg_cap_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_seg_cap_{suffix}"

    # Two segments, ~30 normal rows each — enough for the trainer's minimum-rows
    # check (>=10) without paying for hundreds of rows we don't need. Two columns
    # so we get non-trivial SHAP contributions and distinct (segment, pattern) groups.
    train_rows = []
    for region, base in [("A", 100.0), ("B", 200.0)]:
        for i in range(30):
            train_rows.append((region, base + i * 0.5, base * 0.8 + i * 0.3))
    train_df = spark.createDataFrame(train_rows, "region string, amount double, discount double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "discount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
        params=AnomalyParams(sample_fraction=1.0),
    )

    # Build a scoring DataFrame with multiple distinct outlier shapes per segment.
    # The (segment, pattern) key is (region, sorted top-2 SHAP contributors), so
    # varying which feature dominates per row produces distinct groups within a
    # segment. With max_groups=3 and 2 segments, the per-segment budget is 1; if
    # the cap were only per-segment we'd see >=4 LLM calls (2 patterns x 2 segments).
    score_rows = []
    for region in ("A", "B"):
        # Pattern 1: amount-dominant outliers
        score_rows.extend([(region, 9999.0, 1.0)] * 3)
        # Pattern 2: discount-dominant outliers
        score_rows.extend([(region, 1.0, 9999.0)] * 3)
    score_df = spark.createDataFrame(score_rows, "region string, amount double, discount double")

    call_count = 0

    class _CountingPredictor:
        def __init__(self, *_a, **_kw) -> None:
            pass

        def __call__(self, **_kwargs) -> SimpleNamespace:
            nonlocal call_count
            call_count += 1
            return _CANNED

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _CountingPredictor)
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _CountingPredictor)

    max_groups = 3
    apply_fn = create_anomaly_apply_fn(
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_ai_explanation=True,
        llm_model_config=_llm_cfg(),
        max_groups=max_groups,
    )
    apply_fn(score_df).collect()

    # The whole point: total LLM calls across both segments stay <= max_groups.
    # With per-segment cap (the bug) we'd see up to 2 * max_groups = 6 calls.
    assert call_count <= max_groups, (
        f"Global max_groups cap violated: {call_count} LLM calls observed, max_groups={max_groups}. "
        f"Per-segment-cap regression — see *_split_max_groups_budget* in scoring_run.py."
    )
    # And we should have actually made at least one call (otherwise the test isn't
    # exercising the cap at all — e.g. all anomalies fell below threshold).
    assert call_count >= 1, "no LLM calls made — test setup didn't produce anomalies above threshold"
