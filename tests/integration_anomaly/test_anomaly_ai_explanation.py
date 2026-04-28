"""Integration tests for LLM-based AI explanation of row anomalies.

Uses driver_only=True so the LLM call happens in the driver process, letting us
monkeypatch dspy without crossing a UDF worker boundary. The LLM itself is
stubbed out — we exercise the real Spark/SHAP/_dq_info plumbing end-to-end.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, MapType, StringType, StructField, StructType

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import DSPY_AVAILABLE, ExplanationContext
from databricks.labs.dqx.config import LLMModelConfig
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
    return LLMModelConfig(model_name="databricks/stub", api_key="stub", api_base="https://stub")


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
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, mock_llm
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
    return ExplanationContext(
        severity_col="severity_percentile",
        contributions_col="anomaly_contributions",
        score_std_col="anomaly_score_std",
        ai_explanation_col="ai_explanation",
        threshold=95.0,
        model_name="catalog.schema.synthetic",
        llm_model_config=LLMModelConfig(model_name="databricks/stub", api_key="stub", api_base="https://stub"),
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


def test_ai_explanation_handles_empty_input_dataframe(spark: SparkSession, mock_llm):
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
    if not DSPY_AVAILABLE:
        pytest.skip("dspy not installed")
    lm_cls, lm_kwargs = _capturing_lm()
    _patch_dspy(monkeypatch, lm_cls, _FakePredictor)

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


def test_lm_config_forces_openai_prefix_when_api_base_set_and_no_provider(spark: SparkSession, monkeypatch):
    """Bare model name + api_base → litellm openai/ adapter (avoids ENDPOINT_NOT_FOUND on AI Gateway)."""
    cfg = LLMModelConfig(model_name="databricks-qwen3-80b", api_key="tok", api_base="https://gw.example/v1")
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["model"] == "openai/databricks-qwen3-80b"
    assert captured[0]["api_base"] == "https://gw.example/v1"
    assert captured[0]["api_key"] == "tok"
    assert captured[0]["model_type"] == "chat"
    assert "max_retries" in captured[0]


def test_lm_config_preserves_explicit_provider_prefix(spark: SparkSession, monkeypatch):
    """A model name that already contains '/' is passed through unchanged."""
    cfg = LLMModelConfig(model_name="databricks/claude-sonnet", api_key="", api_base="https://gw.example/v1")
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["model"] == "databricks/claude-sonnet"


def test_lm_config_no_prefix_or_creds_when_workspace_auth(spark: SparkSession, monkeypatch):
    """Workspace-auth path: no api_base + empty creds → bare model, no api_base/api_key keys."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    cfg = LLMModelConfig(model_name="databricks/claude-sonnet", api_key="", api_base="")
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["model"] == "databricks/claude-sonnet"
    assert "api_base" not in captured[0]
    assert "api_key" not in captured[0]


def test_lm_config_falls_back_to_openai_env_vars(spark: SparkSession, monkeypatch):
    """Empty api_key/api_base on the config fall back to OPENAI_API_KEY / OPENAI_API_BASE."""
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    monkeypatch.setenv("OPENAI_API_BASE", "https://env.example/v1")
    cfg = LLMModelConfig(model_name="some-model", api_key="", api_base="")
    captured = _run_with_lm_capture(spark, monkeypatch, cfg)

    assert captured[0]["api_key"] == "env-key"
    assert captured[0]["api_base"] == "https://env.example/v1"
    assert captured[0]["model"] == "openai/some-model"
