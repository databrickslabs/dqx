"""Fixtures for explanation-contract tests that require Spark but not a serving endpoint."""

from collections.abc import Callable
import json

import pytest
from pyspark.sql import Column, functions as F

from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata


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
        payload = json.dumps(
            {"narrative": "Review the evidence.", "business_impact": "safe", "action": "Check the source."}
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
        if response_kind == "controls":
            return F.lit(
                json.dumps(
                    {name: "Review\n\t" + "evidence " * 300 for name in ("narrative", "business_impact", "action")}
                )
            )
        assert response_kind == "prompt_echo"
        prompt = F.col("__prompt")
        leaked = prompt.rlike("secret_value|PRIVATE_CATEGORY|secret_alias|private_group|private_time")
        return F.to_json(
            F.struct(
                F.lit("Review the disclosed evidence.").alias("narrative"),
                F.when(leaked, F.lit("leaked")).otherwise(F.lit("safe")).alias("business_impact"),
                # The final occurrence is the current input, after the fixed few-shot examples.
                F.substring_index(prompt, "\nbaseline_grouping: ", -1).alias("action"),
            )
        )

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
