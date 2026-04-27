"""Integration tests for LLM-based AI explanation of row anomalies.

Uses driver_only=True so the LLM call happens in the driver process, letting us
monkeypatch dspy without crossing a UDF worker boundary. The LLM itself is
stubbed out — we exercise the real Spark/SHAP/_dq_info plumbing end-to-end.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import DSPY_AVAILABLE, ExplanationContext
from databricks.labs.dqx.config import LLMModelConfig
from tests.integration_anomaly.constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)


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

    import dspy  # type: ignore

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _FakePredictor)
    # Also patch the reference imported into the module in case dspy symbols were rebound.
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _FakePredictor)
    return _CANNED


def _llm_cfg() -> LLMModelConfig:
    return LLMModelConfig(model_name="databricks/stub", api_key="stub", api_base="https://stub")


def test_ai_explanation_populated_for_anomalous_row(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, mock_llm
):
    """Anomalous rows get the ai_explanation struct populated from the (mocked) LLM."""
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )

    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_ai_explanation=True,
        llm_model_config=_llm_cfg(),
        extract_score=False,
    )
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
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Row drawn from the training distribution (see get_standard_3d_training_data: i=100).
    test_df = test_df_factory(
        spark,
        normal_rows=[(150.0, 20.0, 0.2)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double, discount double",
    )

    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_ai_explanation=True,
        llm_model_config=_llm_cfg(),
        extract_score=False,
    )
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

    import dspy  # type: ignore

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _CapturingPredictor)
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _CapturingPredictor)

    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )

    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_ai_explanation=True,
        llm_model_config=_llm_cfg(),
        redact_columns=["amount"],
        extract_score=False,
    )
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

    import dspy  # type: ignore

    monkeypatch.setattr(dspy, "LM", _FakeLM)
    monkeypatch.setattr(dspy, "Predict", _CountingPredictor)
    monkeypatch.setattr(llm_explainer.dspy, "LM", _FakeLM)
    monkeypatch.setattr(llm_explainer.dspy, "Predict", _CountingPredictor)

    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Several identical outliers → same contributions → same pattern → one group.
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)] * 5,
        columns_schema="amount double, quantity double, discount double",
    )

    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_ai_explanation=True,
        llm_model_config=_llm_cfg(),
        extract_score=False,
    )
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
    """Build a minimal scored DataFrame the explainer accepts: severity, contributions, score_std."""
    from pyspark.sql.types import DoubleType, MapType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("severity_percentile", DoubleType(), False),
            StructField("anomaly_score_std", DoubleType(), True),
            StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True),
        ]
    )
    data = [(sev, 0.0, contrib) for sev, contrib in rows]
    return spark.createDataFrame(data, schema=schema)


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
