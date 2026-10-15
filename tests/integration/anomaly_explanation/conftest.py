"""Fixtures for explanation-contract tests that require Spark but not a serving endpoint."""

from collections.abc import Callable
import json

import pytest
from pyspark.sql import Column, functions as F

from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata


def _adversarial_response(response_kind: str) -> Column | None:
    if response_kind == "false_redaction":
        return F.lit(
            json.dumps(
                {
                    "narrative": "No specific metrics can be disclosed.",
                    "business_impact": "unsupported",
                    "action": "unsupported",
                }
            )
        )
    if response_kind == "controls":
        return F.lit(
            json.dumps({name: "Review\n\t" + "evidence " * 300 for name in ("narrative", "business_impact", "action")})
        )
    return None


def _prompt_echo_response(response_kind: str) -> Column:
    assert response_kind in {"prompt_echo", "basis_redaction", "basis_echo"}
    prompt = F.col("__prompt")
    if response_kind == "basis_echo":
        basis = F.substring_index(F.substring_index(prompt, "\nbasis_contributions: ", -1), "\ngroup_size: ", 1)
        metric_qualification = prompt.contains(
            "Its shares are within each metric; this displayed shortlist may omit lower-ranked comparisons, "
            "so the listed shares need not total 100 per metric."
        )
        column_qualification = prompt.contains(
            "Shares are within each column; this displayed shortlist may omit lower-ranked comparisons, "
            "so the listed shares need not total 100 per column."
        )
        return F.to_json(
            F.struct(
                basis.alias("narrative"),
                F.when(metric_qualification & column_qualification, F.lit("prompt contract ok"))
                .otherwise(F.lit("prompt contract missing"))
                .alias("business_impact"),
                F.when(
                    metric_qualification & column_qualification,
                    F.lit("metric and column qualifications ok"),
                )
                .otherwise(F.lit("metric or column qualification missing"))
                .alias("action"),
            )
        )
    leaked = prompt.rlike("secret_value|PRIVATE_CATEGORY|secret_alias|private_group|private_time")
    if response_kind == "basis_redaction":
        leaked = (
            leaked
            | prompt.contains("x vs its group baseline [column: 'x vs its group baseline']")
            | prompt.contains("x vs its group baseline is null")
        )
        safe_response = F.when(prompt.contains("x vs its group baseline [column: 'x']"), F.lit("safe")).otherwise(
            F.lit("missing visible basis")
        )
    else:
        safe_response = F.lit("safe")
    return F.to_json(
        F.struct(
            F.lit("Review the disclosed evidence.").alias("narrative"),
            F.when(leaked, F.lit("leaked")).otherwise(safe_response).alias("business_impact"),
            # The final occurrence is the current input, after the fixed few-shot examples.
            F.substring_index(prompt, "\nbaseline_grouping: ", -1).alias("action"),
        )
    )


@pytest.fixture
def explanation_serving_boundary(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> list[str]:
    """Return safe diagnostic responses computed from the actual prompt delivered to ai_query."""
    spark_expr: Callable[[str], Column] = F.expr
    calls: list[str] = []
    response_kind = getattr(request, "param", "prompt_echo")

    def expression(query: str) -> Column:
        if not query.startswith("ai_query("):
            return spark_expr(query)
        calls.append(query)
        adversarial = _adversarial_response(response_kind)
        if adversarial is not None:
            return adversarial
        payload = json.dumps(
            {"narrative": "Review the evidence.", "business_impact": "safe", "action": "Check the source."}
        )
        if response_kind == "feature_echo":
            return F.to_json(
                F.struct(
                    F.col("feature_contributions").alias("narrative"),
                    F.lit("safe").alias("business_impact"),
                    F.lit("Check the source.").alias("action"),
                )
            )
        if response_kind == "string":
            return F.lit(payload)
        if response_kind == "envelope":
            return F.struct(F.lit(payload).alias("result"), F.lit(None).cast("string").alias("errorMessage"))
        if response_kind == "malformed":
            return F.lit("not valid JSON")
        if response_kind == "null":
            return F.lit(None).cast("string")
        if response_kind == "error":
            return F.struct(F.lit(None).cast("string").alias("result"), F.lit("unavailable").alias("errorMessage"))
        return _prompt_echo_response(response_kind)

    monkeypatch.setattr(F, "expr", expression)
    return calls


@pytest.fixture
def private_explanation_metadata() -> SparkFeatureMetadata:
    return SparkFeatureMetadata(
        column_infos=[
            {"name": "secret_value", "category": "categorical"},
            {"name": "visible_metric", "category": "numeric"},
        ],
        categorical_frequency_maps={"secret_value": {"PRIVATE_CATEGORY": 1.0}},
        onehot_categories={"secret_value": {"PRIVATE_CATEGORY": "secret_alias"}},
        engineered_feature_names=[
            "secret_alias",
            "secret_value_freq",
            "secret_value_is_null",
            "visible_metric",
            "visible_metric_rel_baseline",
            "visible_metric_rel_time",
        ],
        baseline_by=["private_group", "visible_group"],
        baseline_over_time="private_time",
    )


@pytest.fixture
def forbid_endpoint_probe(spark, monkeypatch):
    """A deterministic evidence explanation must not make even a one-token serving probe."""
    original_sql = spark.sql

    def sql(query: str):
        assert not query.startswith("SELECT ai_query("), "attribution-unavailable groups must not call serving"
        return original_sql(query)

    monkeypatch.setattr(spark, "sql", sql)
