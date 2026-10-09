"""Public explanation-contract regressions with real Spark and a substituted serving boundary.

Only the third-party ai_query expression is replaced. Prompt construction, response parsing,
materialization and the join back to caller rows execute normally, without paid endpoint calls.
"""

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

    assert first == second
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
