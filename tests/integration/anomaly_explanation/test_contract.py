"""Public explanation-contract regressions with real Spark and a substituted serving boundary.

Only the third-party ai_query expression is replaced. Prompt construction, response parsing,
materialization and the join back to caller rows execute normally, without paid endpoint calls.
"""

from collections import Counter
import json

import pytest
from pyspark.sql import SparkSession, functions as F

from databricks.labs.dqx.anomaly.anomaly_llm_explainer import (
    ExplanationContext,
    add_explanation_column,
    redaction_set,
)
from databricks.labs.dqx.anomaly.drift import DriftResult, format_drift_summary
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata


@pytest.mark.parametrize("redact_all", [False, True])
def test_explanation_redaction_covers_complete_prompt(
    spark: SparkSession,
    explanation_serving_boundary: list[str],
    private_explanation_metadata: SparkFeatureMetadata,
    redact_all: bool,
) -> None:
    metadata = private_explanation_metadata
    redacted: tuple[str, ...] = ("secret_value", "private_group", "private_time")
    if redact_all:
        redacted += ("visible_metric", "visible_group")
    ctx = ExplanationContext(
        severity_col="severity",
        contributions_col="contributions",
        basis_contributions_col="basis",
        score_std_col="score_std",
        ai_explanation_col="explanation",
        pattern_col="pattern_owned",
        threshold=95.0,
        model_name="catalog.schema.model",
        redact_columns=redacted,
        feature_metadata=metadata,
    )
    source = spark.createDataFrame(
        [
            (
                99.0,
                {"secret_value": 80.0, "visible_metric": 20.0},
                {"secret_value = PRIVATE_CATEGORY": 100.0, "visible_metric vs its group baseline": 100.0},
                0.0,
            )
        ],
        "severity double, contributions map<string,double>, basis map<string,double>, score_std double",
    )
    drift = DriftResult(
        drift_detected=True,
        drift_score=4.0,
        drifted_columns=["secret_alias", "secret_value_freq", "secret_value_is_null", "visible_metric"],
        column_scores={name: 4.0 for name in metadata.engineered_feature_names},
        recommendation="",
    )

    result = add_explanation_column(
        source,
        ctx,
        is_ensemble=False,
        drift_summary=format_drift_summary(drift, redaction_set(redacted, metadata)),
        endpoint_reachable=True,
    )
    explanation = result.collect()[0]["explanation"]

    assert explanation["business_impact"] == "safe", "a redacted name or category reached the complete prompt"
    assert "temporal_baseline: withheld" in explanation["action"]
    if redact_all:
        assert explanation["action"].startswith("withheld, withheld")
        assert "drift detected (4 features)" in explanation["action"]
        assert "visible_metric" not in explanation["top_drivers"]
    else:
        assert explanation["action"].startswith("withheld, visible_group")
        assert "drift detected: visible_metric=4.00" in explanation["action"]
        assert "visible_metric" in explanation["top_drivers"]
    assert result.select(*source.columns).collect() == source.collect()
    assert len(explanation_serving_boundary) == 1


def test_explanation_preserves_response_named_columns_and_duplicate_rows(
    spark: SparkSession, explanation_serving_boundary: list[str]
) -> None:
    response_names = [
        "narrative",
        "business_impact",
        "top_features",
        "top_drivers",
        "action",
        "group_size",
        "group_avg_severity",
        "__disclosure",
        "__prompt",
        "__raw_response",
        "__parsed",
    ]
    source = spark.createDataFrame(
        [(1, 99.0, {"metric": 100.0}, 0.0), (1, 99.0, {"metric": 100.0}, 0.0), (2, 50.0, {"metric": 100.0}, 0.0)],
        "row_id int, severity double, contributions map<string,double>, score_std double",
    )
    for name in response_names:
        source = source.withColumn(name, F.lit(f"original {name}"))
    ctx = ExplanationContext(
        severity_col="severity",
        contributions_col="contributions",
        score_std_col="score_std",
        ai_explanation_col="explanation_owned",
        pattern_col="pattern_owned",
        threshold=95.0,
        model_name="catalog.schema.model",
    )

    result = add_explanation_column(source, ctx, is_ensemble=False, endpoint_reachable=True)
    first = result.collect()
    second = result.collect()

    assert Counter(json.dumps(row.asDict(recursive=True), sort_keys=True) for row in first) == Counter(
        json.dumps(row.asDict(recursive=True), sort_keys=True) for row in second
    )
    assert result.columns == [*source.columns, "explanation_owned"]
    assert sorted(result.select(*source.columns).collect()) == sorted(source.collect())
    assert len(first) == 3
    for row in first:
        explanation = row["explanation_owned"]
        if row["row_id"] == 2:
            assert explanation is None
        else:
            assert explanation["group_size"] == 2
            assert explanation["business_impact"] == "safe"
        assert all(row[name] == f"original {name}" for name in response_names)
    assert len(explanation_serving_boundary) == 1


@pytest.mark.parametrize(
    "explanation_serving_boundary", ["string", "envelope", "malformed", "null", "error", "controls"], indirect=True
)
def test_serving_response_shapes_and_failures_preserve_rows(spark, explanation_serving_boundary, request):
    """Real Spark parses each supported response shape and safely handles failed completions."""
    kind = request.node.callspec.params["explanation_serving_boundary"]
    source = spark.createDataFrame(
        [(1, 99.0, {"metric": 100.0}, 0.0), (1, 99.0, {"metric": 100.0}, 0.0), (2, 50.0, {"metric": 100.0}, 0.0)],
        "row_id int, severity double, contributions map<string,double>, score_std double",
    )
    context = ExplanationContext(
        severity_col="severity",
        contributions_col="contributions",
        score_std_col="score_std",
        ai_explanation_col="explanation",
        pattern_col="pattern_owned",
        threshold=95.0,
        model_name="catalog.schema.model",
    )
    result = add_explanation_column(source, context, is_ensemble=False, endpoint_reachable=True)
    rows = result.collect()
    assert len(rows) == 3
    assert sorted(result.select(*source.columns).collect()) == sorted(source.collect())
    for row in rows:
        explanation = row["explanation"]
        if row.row_id == 2 or kind in {"malformed", "null", "error"}:
            assert explanation is None
        else:
            assert explanation is not None
            assert explanation["group_size"] == 2
            assert explanation["top_features"] == "metric"
            for field in ("narrative", "business_impact", "action"):
                value = explanation[field]
                assert value and len(value) <= 500
                assert all(ord(character) >= 32 and ord(character) != 127 for character in value)
    assert len(explanation_serving_boundary) == 1  # SQL expression construction, not endpoint invocation count.


def test_explanation_budget_keeps_largest_group_and_preserves_dropped_rows(spark, explanation_serving_boundary):
    source = spark.createDataFrame(
        [(1, 99.0, {"first": 100.0}, 0.0), (2, 99.0, {"first": 100.0}, 0.0), (3, 99.0, {"second": 100.0}, 0.0)],
        "row_id int, severity double, contributions map<string,double>, score_std double",
    )
    context = ExplanationContext(
        severity_col="severity",
        contributions_col="contributions",
        score_std_col="score_std",
        ai_explanation_col="explanation",
        pattern_col="pattern_owned",
        threshold=95.0,
        model_name="catalog.schema.model",
        max_groups=1,
    )
    result = add_explanation_column(source, context, is_ensemble=False, endpoint_reachable=True)
    rows = {row.row_id: row for row in result.collect()}
    assert set(rows) == {1, 2, 3}
    assert rows[1]["explanation"]["group_size"] == 2
    assert rows[1]["explanation"] == rows[2]["explanation"]
    assert rows[3]["explanation"] is None
    assert sorted(result.select(*source.columns).collect()) == sorted(source.collect())
    assert len(explanation_serving_boundary) == 1


@pytest.mark.parametrize(
    "withheld,visible,expected_scope",
    [
        (0.0, 100.0, "complete"),
        (50.0, 50.0, "partial"),
        (50.01, 49.99, "limited"),
        (100.0, 0.0, "limited"),
        (0.0, 0.0, "complete"),
    ],
    ids=["none", "exactly_half", "over_half", "all", "no_evidence"],
)
@pytest.mark.parametrize("explanation_serving_boundary", ["string"], indirect=True)
def test_explanation_publishes_actual_evidence_disclosure(
    spark, explanation_serving_boundary, withheld: float, visible: float, expected_scope: str
) -> None:
    """The public result carries Spark's computed disclosure band and its matching narrative limit."""
    source = spark.createDataFrame(
        [(1, 99.0, {"secret_metric": withheld, "visible_metric": visible}, 0.0)],
        "row_id int, severity double, contributions map<string,double>, score_std double",
    )
    context = ExplanationContext(
        severity_col="severity",
        contributions_col="contributions",
        score_std_col="score_std",
        ai_explanation_col="explanation",
        pattern_col="pattern_owned",
        threshold=95.0,
        model_name="catalog.schema.model",
        redact_columns=("secret_metric",),
    )
    result = add_explanation_column(source, context, is_ensemble=False, endpoint_reachable=True)
    rows = result.collect()
    assert len(rows) == 1
    explanation = rows[0]["explanation"]
    assert explanation is not None and explanation["evidence_scope"] == expected_scope
    assert "secret_metric" not in explanation["top_features"]
    assert "secret_metric" not in explanation["top_drivers"]
    narrative = explanation["narrative"]
    if expected_scope == "complete":
        assert "could not be disclosed" not in narrative
    else:
        assert narrative.count("could not be disclosed") == 1
        assert (
            "Most of the contributing evidence" if expected_scope == "limited" else "Some contributing evidence"
        ) in narrative
    assert result.select(*source.columns).collect() == source.collect()
    assert len(explanation_serving_boundary) == 1
