"""Fixtures for explanation-contract tests that require Spark but not a serving endpoint."""

from collections.abc import Callable

import pytest
from pyspark.sql import Column, functions as F

from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata


@pytest.fixture
def explanation_serving_boundary(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Return safe diagnostic responses computed from the actual prompt delivered to ai_query."""
    spark_expr: Callable[[str], Column] = F.expr
    calls: list[str] = []

    def expression(query: str) -> Column:
        if not query.startswith("ai_query("):
            return spark_expr(query)
        calls.append(query)
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
