"""Integration tests for LLM-based AI explanation of row anomalies.

The LLM call runs inside Spark via the SQL ``ai_query`` function against a Databricks Model
Serving endpoint, so these tests exercise the real Spark / SHAP / _dq_info / ai_query plumbing
end-to-end. Tests that need a live endpoint skip themselves when the workspace cannot reach the
configured endpoint (override with the DQX_AI_QUERY_TEST_ENDPOINT env var).
"""

from __future__ import annotations

import os
import re

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import anomaly_llm_explainer as llm_explainer
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.errors import InvalidParameterError
from tests.integration_anomaly.conftest import qualify_model_name
from tests.integration_anomaly.constants import (
    DEFAULT_AI_QUERY_ENDPOINT,
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)

_LLM_EXPLAINER_LOGGER = "databricks.labs.dqx.anomaly.anomaly_llm_explainer"
_AI_QUERY_TEST_ENDPOINT = os.environ.get("DQX_AI_QUERY_TEST_ENDPOINT", DEFAULT_AI_QUERY_ENDPOINT)


def _ai_query_llm_cfg(endpoint: str) -> LLMModelConfig:
    return LLMModelConfig(model_name=endpoint)


def _score_with_explanation(scorer, df, model_meta, *, llm_model_config, **overrides):
    kwargs = {
        "model_name": model_meta["model_name"],
        "registry_table": model_meta["registry_table"],
        "threshold": DEFAULT_SCORE_THRESHOLD,
        "enable_contributions": True,
        "enable_ai_explanation": True,
        "ai_explanation_llm_model_config": llm_model_config,
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
    except Exception as exc:
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
        assert len(value) <= llm_explainer._LLM_FIELD_MAX_LEN
    # top_features is computed deterministically from real SHAP contributions (top-2, alpha-sorted).
    assert explanation["top_features"]
    for feat in explanation["top_features"].split("+"):
        assert feat in {"amount", "quantity", "discount"}
    assert explanation["group_size"] == 1
    # Single-row group: group_avg_severity is the row's severity. The struct's
    # severity_percentile is rounded to 1 decimal while group_avg_severity is full precision,
    # so compare with a tolerance that absorbs that rounding rather than exact equality.
    assert explanation["group_avg_severity"] == pytest.approx(anomaly_info["severity_percentile"], abs=0.1)


def test_ai_query_explanation_null_for_non_anomalous_row(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer
):
    """Rows below the severity threshold keep ai_explanation null and make no ai_query call.

    Needs no reachable endpoint: with no anomalies above threshold the explainer short-circuits
    before any ai_query call, so it runs even on workspaces without Foundation Model APIs.
    """
    # Row drawn from the training distribution (see get_standard_3d_training_data: i=100).
    test_df = test_df_factory(
        spark,
        normal_rows=[(150.0, 20.0, 0.2)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double, discount double",
    )
    result_df = _score_with_explanation(
        anomaly_scorer, test_df, shared_3d_model, llm_model_config=_ai_query_llm_cfg(_AI_QUERY_TEST_ENDPOINT)
    )
    row = result_df.collect()[0]
    anomaly_info = row["_dq_info"][0]["anomaly"]

    assert anomaly_info["is_anomaly"] is False
    assert anomaly_info["ai_explanation"] is None


def test_ai_query_explanation_redact_columns_filters_output(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """redact_columns prevents the redacted feature name from appearing in the pattern key or any
    returned field — verifying redaction holds end-to-end (prompt + pattern + model output)."""
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
    assert "amount" not in explanation["top_features"]
    for field_name in ("narrative", "business_impact", "action"):
        assert (
            "amount" not in explanation[field_name].lower()
        ), f"redacted column 'amount' leaked into {field_name}: {explanation[field_name]!r}"


def test_ai_query_explanation_one_call_per_group(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """Multiple identical anomalous rows collapse into a single (segment, pattern) group.

    The ai_query call runs on executors so we can't intercept it directly; instead we assert the
    *observable* contract: every flagged row in the group shares the same narrative and
    group_size, which can only happen if the LLM was invoked once and the result fanned out via
    the join.
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
    # Single LLM call → single narrative + top_features shared across the group.
    assert len({e["narrative"] for e in explanations}) == 1
    assert len({e["top_features"] for e in explanations}) == 1
    assert all(e["group_size"] == len(explanations) for e in explanations)


def test_ai_query_explanation_references_dominant_feature_within_word_caps(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """Low-bar prompt-quality guard: the narrative references the dominant contributing feature
    and the fields respect the documented length caps.

    This won't catch subtle quality regressions, but it does catch structurally-bogus output —
    a model that ignores the inputs entirely, or one that ignores the length instructions.
    """
    test_df = _make_outlier_df(spark, test_df_factory)
    result_df = _score_with_explanation(
        anomaly_scorer, test_df, shared_3d_model, llm_model_config=_ai_query_llm_cfg(ai_query_endpoint)
    )
    explanation = result_df.collect()[0]["_dq_info"][0]["anomaly"]["ai_explanation"]
    assert explanation is not None

    # Factual-correctness floor: at least one of the deterministic top features must appear
    # somewhere in the narrative/impact/action — the model must actually use its inputs.
    dominant_features = explanation["top_features"].split("+")
    combined = " ".join(explanation[f] for f in ("narrative", "business_impact", "action")).lower()
    assert any(
        feat.lower() in combined for feat in dominant_features
    ), f"none of the top features {dominant_features} appear in the explanation: {combined!r}"

    # Length-discipline floor (looser than the prompt's hard limits to absorb model variance).
    def _word_count(text: str) -> int:
        return len(re.findall(r"\S+", text))

    assert _word_count(explanation["narrative"]) <= 50
    assert _word_count(explanation["business_impact"]) <= 30
    assert _word_count(explanation["action"]) <= 25


# Regression test for ai_query response-shape portability. Different Databricks serving
# endpoints return ``ai_query`` results in different SQL types — STRING for some (e.g.
# ``databricks-llama-4-maverick``), STRUCT<result, errorMessage> envelope for others (e.g.
# ``databricks-gemma-3-12b``). The DQX parser must adapt to both. This test sweeps a
# representative set and skips endpoints that aren't reachable in the current workspace, so
# it doubles as a portability matrix for whichever endpoints are provisioned.
_AI_QUERY_REGRESSION_ENDPOINTS = [
    "databricks-llama-4-maverick",
    "databricks-gemma-3-12b",
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-claude-3-7-sonnet",
    "databricks-gpt-oss-20b",
]


@pytest.mark.slow
@pytest.mark.parametrize("endpoint", _AI_QUERY_REGRESSION_ENDPOINTS)
def test_ai_query_response_shape_portability(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, endpoint
):
    """ai_query path produces a usable explanation regardless of the endpoint's response shape.

    Regression for `from_json(__raw_response)` failing with DATATYPE_MISMATCH on endpoints
    that return STRUCT<result, errorMessage> (e.g. databricks-gemma-3-12b) instead of the
    plain STRING shape (e.g. databricks-llama-4-maverick). The shape detection in
    *_call_llm_for_groups_ai_query* must adapt to both without per-endpoint configuration.
    """
    # Per-endpoint reachability check: skip individually so one missing endpoint doesn't mask
    # the others' results — that's the whole point of the regression matrix.
    try:
        spark.sql(
            f"SELECT ai_query('{endpoint}', 'reply ok', "
            f"modelParameters => named_struct('max_tokens', 8, 'temperature', 0.0)) AS r"
        ).collect()
    except Exception as exc:
        pytest.skip(f"ai_query endpoint {endpoint!r} not reachable: {exc!r}"[:200])

    test_df = _make_outlier_df(spark, test_df_factory)
    result_df = _score_with_explanation(
        anomaly_scorer, test_df, shared_3d_model, llm_model_config=_ai_query_llm_cfg(endpoint)
    )
    row = result_df.collect()[0]
    anomaly_info = row["_dq_info"][0]["anomaly"]

    assert anomaly_info["is_anomaly"] is True
    explanation = anomaly_info["ai_explanation"]
    assert explanation is not None, f"ai_query against {endpoint!r} returned a null struct"
    for field_name in ("narrative", "business_impact", "action"):
        value = explanation[field_name]
        assert isinstance(value, str) and value.strip(), f"{endpoint!r}: {field_name!r} is empty"


def test_ai_query_rejects_non_databricks_provider():
    """A non-Databricks provider prefix surfaces InvalidParameterError.

    Pure validation — no live call, no skip needed. Catches the case where a user copies a
    litellm-style ``provider/model`` config that the ai_query path can't route.
    """
    cfg = LLMModelConfig(model_name="openai/gpt-4")
    with pytest.raises(InvalidParameterError, match="require a Databricks serving endpoint"):
        llm_explainer._resolve_ai_query_endpoint(cfg.model_name)


def test_ai_query_explanation_on_by_default(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, ai_query_endpoint
):
    """With no enable flags, both contributions and ai_explanation are produced (on by default).

    Builds the check directly (not via the test helper, which defaults the flags off) so it
    exercises the production defaults end-to-end.
    """
    test_df = _make_outlier_df(spark, test_df_factory)
    _, apply_fn, info_col = has_no_row_anomalies(
        model_name=qualify_model_name(shared_3d_model["model_name"], shared_3d_model["registry_table"]),
        registry_table=shared_3d_model["registry_table"],
        driver_only=True,
        ai_explanation_llm_model_config={"model_name": ai_query_endpoint},
    )
    # Direct apply (not via the normalizing helper), so info_col is a single struct<anomaly: ...>,
    # not the _dq_info array — access the anomaly field directly.
    anomaly = apply_fn(test_df).collect()[0][info_col]["anomaly"]
    assert anomaly["is_anomaly"] is True
    assert anomaly["contributions"] is not None, "contributions should be on by default"
    assert anomaly["ai_explanation"] is not None, "ai_explanation should be on by default"


def test_ai_query_explanation_degrades_when_endpoint_unavailable(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, caplog
):
    """A valid-format but non-existent endpoint degrades to a null explanation + WARNING — scoring
    still completes (the default-on safety net for workspaces without a reachable endpoint)."""
    test_df = _make_outlier_df(spark, test_df_factory)
    bogus = LLMModelConfig(model_name="databricks-nonexistent-endpoint-zzz")
    with caplog.at_level("WARNING", logger=_LLM_EXPLAINER_LOGGER):
        result_df = _score_with_explanation(anomaly_scorer, test_df, shared_3d_model, llm_model_config=bogus)
        anomaly = result_df.collect()[0]["_dq_info"][0]["anomaly"]
    assert anomaly["is_anomaly"] is True
    assert anomaly["ai_explanation"] is None, "explanation should degrade to null on an unreachable endpoint"
    assert any("not reachable" in r.message for r in caplog.records), "expected a 'not reachable' WARNING"


def test_ai_query_explanation_disabled_without_contributions(
    spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer, caplog
):
    """Turning contributions off disables AI explanations (with a WARNING) instead of erroring."""
    test_df = _make_outlier_df(spark, test_df_factory)
    with caplog.at_level("WARNING", logger="databricks.labs.dqx.anomaly.check_funcs"):
        result_df = _score_with_explanation(
            anomaly_scorer,
            test_df,
            shared_3d_model,
            llm_model_config=_ai_query_llm_cfg(_AI_QUERY_TEST_ENDPOINT),
            enable_contributions=False,  # overrides the default-True; explanation is downgraded off
        )
        anomaly = result_df.collect()[0]["_dq_info"][0]["anomaly"]
    assert anomaly["contributions"] is None
    assert anomaly["ai_explanation"] is None
    assert any("disabling enable_ai_explanation" in r.message for r in caplog.records)
