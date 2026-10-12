"""Fixtures for explanation-contract tests that require Spark but not a serving endpoint."""

from collections.abc import Callable
import json
import hashlib
import logging
import os
import platform
from importlib.metadata import distributions
from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.sql import Column, functions as F

from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from tests.anomaly_diagnostics import AnomalyDiagnostics, DEPENDENCIES, dependency_provenance


def pytest_configure(config: pytest.Config) -> None:
    """Opt in to safe reports for this anomaly-specific integration subtree only."""
    directory = os.environ.get("DQX_TEST_DIAGNOSTICS_DIR")
    if not directory:
        return
    versions = {
        package.metadata["Name"]: package.version
        for package in distributions()
        if package.metadata["Name"] in DEPENDENCIES
    }
    wheel = os.environ.get("DQX_PREBUILT_WHEEL")
    wheel_digest = None
    if wheel:
        try:
            wheel_digest = hashlib.sha256(Path(wheel).read_bytes()).hexdigest()
        except OSError:
            logging.getLogger(__name__).warning("Prebuilt wheel is unavailable for diagnostic provenance")
    revision = os.environ.get("GITHUB_SHA")
    source = os.environ.get("DQX_TEST_CODE_SOURCE", "checkout")
    provenance = dependency_provenance(
        versions,
        platform.python_version(),
        wheel_sha256=wheel_digest,
        revision=revision,
        source=source,
    )
    provenance["acceptance_retry"] = bool(os.environ.get("TEST_FILTER"))
    reporter = AnomalyDiagnostics(
        Path(directory), Path(__file__).parent, f"explanation-{os.getpid()}-{uuid4().hex}", provenance
    )
    config.pluginmanager.register(reporter, "dqx-explanation-diagnostics")


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
